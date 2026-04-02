"""
driveo_charge_ingest.py
─────────────────────────────────────────────────────────────────────
Fetches Driveo Charge Report → inserts into charge_reports_daily
→ upserts total_charge_added into day_end_metrics
─────────────────────────────────────────────────────────────────────
"""

import io
import csv
import base64
import logging
import requests
import psycopg2
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
import msal

# ═══════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════

EMAIL_ADDRESS = "data.analyst@driveo.in"
EMAIL_PASSWORD = "passworD11.aynk"
TENANT_ID     = "23a00293-83fd-475f-a0c7-91acafd60b26"
CLIENT_ID     = "d3590ed6-52b3-4102-aeff-aad2292ab01c"

DB_SCHEMA = "Analytics"
DB_TABLE  = "charge_reports_daily"

SUPABASE_DB_URL = "postgresql+psycopg2://postgres.qkpmgzfwxoujiodlufue:FGTN4hxvi8xaLbjV@aws-0-ap-south-1.pooler.supabase.com:5432/postgres?sslmode=require"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPES     = ["https://graph.microsoft.com/.default"]


# ───────────────────────────────────────────────────────────────────
# HELPERS
# ───────────────────────────────────────────────────────────────────

def yesterday():
    d = datetime.now() - timedelta(days=1)
    return d.strftime("%d-%b-%Y"), d.date()


def clean_numeric(val):
    if val is None:
        return None
    s = str(val).strip()
    if s in ("", "0", "00", "0.0"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def clean_timestamp(val):
    if not val or not str(val).strip():
        return None

    s = str(val).strip()

    for fmt in (
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue

    log.warning(f"Could not parse timestamp: '{s}'")
    return None


# ───────────────────────────────────────────────────────────────────
# AUTH
# ───────────────────────────────────────────────────────────────────

def get_access_token():
    app = msal.PublicClientApplication(
        client_id=CLIENT_ID,
        authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    )

    result = app.acquire_token_by_username_password(
        username=EMAIL_ADDRESS,
        password=EMAIL_PASSWORD,
        scopes=SCOPES,
    )

    if "access_token" not in result:
        raise RuntimeError("Authentication failed")

    log.info("Microsoft Graph token acquired ✓")
    return result["access_token"]


# ───────────────────────────────────────────────────────────────────
# FETCH EMAIL
# ───────────────────────────────────────────────────────────────────

def fetch_csv_attachment(token, date_str):
    headers = {
        "Authorization": f"Bearer {token}",
        "ConsistencyLevel": "eventual"
    }

    params = {
        "$search": f'"driveo charge report {date_str}"',
        "$top": "5",
    }

    log.info(f"Searching Graph API for charge report: {date_str}")

    resp = requests.get(
        f"{GRAPH_BASE}/me/messages",
        headers=headers,
        params=params,
        timeout=30,
    )
    resp.raise_for_status()

    messages = resp.json().get("value", [])

    if not messages:
        log.warning("No matching email found.")
        return None

    msg = next((m for m in messages if m.get("hasAttachments")), None)

    if not msg:
        log.warning("No attachments found.")
        return None

    log.info(f"Email found → {msg['subject']}")

    att_resp = requests.get(
        f"{GRAPH_BASE}/me/messages/{msg['id']}/attachments",
        headers={"Authorization": f"Bearer {token}"},
    )
    att_resp.raise_for_status()

    for att in att_resp.json().get("value", []):
        name = att.get("name", "")
        if name.lower().endswith(".csv") or name.lower().endswith(".xlsx"):
            content = base64.b64decode(att["contentBytes"])
            log.info(f"Downloaded: {name}")
            return content, name

    return None


# ───────────────────────────────────────────────────────────────────
# PARSE CSV
# ───────────────────────────────────────────────────────────────────

def parse_csv(payload, filename):
    text = payload.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    log.info(f"Parsed {len(rows)} rows")
    return rows


# ───────────────────────────────────────────────────────────────────
# INGEST CHARGE DATA
# ───────────────────────────────────────────────────────────────────

def ingest(rows, report_date):
    conn = psycopg2.connect(
        SUPABASE_DB_URL.replace("postgresql+psycopg2://", "postgresql://"),
        connect_timeout=15
    )

    cur = conn.cursor()

    records = []
    for row in rows:
        records.append((
            (row.get("Vehicleno") or "").strip(),
            clean_timestamp(row.get("Start Time")),
            clean_timestamp(row.get("End Time")),
            (row.get("Duration") or "").strip(),
            (row.get("Charge Type") or "").strip(),
            clean_numeric(row.get("Start SOC")),
            clean_numeric(row.get("End SOC")),
            clean_numeric(row.get("Total SOC")),
            (row.get("Charged Location") or "").strip(),
            report_date,
        ))

    insert_sql = f"""
        INSERT INTO "{DB_SCHEMA}".{DB_TABLE}
        (vehicleno, start_time, end_time, duration, charge_type,
         start_soc, end_soc, total_soc, charged_location, report_date)
        VALUES %s
    """

    execute_values(cur, insert_sql, records, page_size=500)

    conn.commit()
    cur.close()

    log.info(f"✓ Inserted {len(records)} rows")

    return conn


# ───────────────────────────────────────────────────────────────────
# UPSERT TOTAL CHARGE
# ───────────────────────────────────────────────────────────────────

def update_total_charge_added(conn, report_date):
    cur = conn.cursor()

    upsert_sql = """
    INSERT INTO "Analytics".day_end_metrics (cid, date, total_charge_added)
    SELECT 
        vehicleno,
        report_date,
        SUM(total_soc) AS total_charge_added
    FROM "Analytics".charge_reports_daily
    WHERE report_date = %s
    GROUP BY vehicleno, report_date

    ON CONFLICT (cid, date)
    DO UPDATE SET
        total_charge_added = EXCLUDED.total_charge_added;
    """

    cur.execute(upsert_sql, (report_date,))
    conn.commit()
    cur.close()

    log.info("✅ total_charge_added upserted")


# ───────────────────────────────────────────────────────────────────
# MAIN
# ───────────────────────────────────────────────────────────────────

def main():
    date_str, report_date = yesterday()
    log.info(f"Processing charge report for {date_str}")

    token = get_access_token()

    result = fetch_csv_attachment(token, date_str)
    if result is None:
        log.error("No attachment found")
        return

    payload, filename = result

    rows = parse_csv(payload, filename)
    if not rows:
        log.warning("Empty file")
        return

    conn = ingest(rows, report_date)

    # 🔥 FIXED STEP
    update_total_charge_added(conn, report_date)

    conn.close()

    log.info("Pipeline complete ✓")


if __name__ == "__main__":
    main()