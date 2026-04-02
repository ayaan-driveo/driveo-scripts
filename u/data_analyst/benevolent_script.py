"""
driveo_trip_ingest.py
─────────────────────────────────────────────────────────────────────
Fetches the Driveo Trip Report email for T-1 day via Microsoft Graph
API (no Azure app registration needed — uses MS Office public client),
extracts the CSV attachment, and upserts rows into Supabase.
─────────────────────────────────────────────────────────────────────
Install deps:
    pip install msal requests psycopg2-binary
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
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

# ── Microsoft 365 ──────────────────────────────────────────────────
EMAIL_ADDRESS = "data.analyst@driveo.in"
EMAIL_PASSWORD = "passworD11.aynk"
TENANT_ID     = "23a00293-83fd-475f-a0c7-91acafd60b26"
CLIENT_ID     = "d3590ed6-52b3-4102-aeff-aad2292ab01c"

# ── Supabase / PostgreSQL ──────────────────────────────────────────
DB_HOST     = "db.qkpmgzfwxoujiodlufue.supabase.co"
DB_PORT     = 5432
DB_NAME     = "postgres"
DB_USER     = "postgres"
DB_PASSWORD = "FGTN4hxvi8xaLbjV"
DB_SCHEMA   = "Analytics"
DB_TABLE    = "trip_reports_daily"

# ═══════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SCOPES     = ["https://graph.microsoft.com/.default"]


# ───────────────────────────────────────────────────────────────────
#  HELPERS
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
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%d-%b-%Y %H:%M",
    ):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    log.warning(f"Could not parse timestamp: '{s}'")
    return None


# ───────────────────────────────────────────────────────────────────
#  STEP 1 — Authenticate via MSAL
# ───────────────────────────────────────────────────────────────────

def get_access_token() -> str:
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
        err = result.get("error_description") or result.get("error") or str(result)
        raise RuntimeError(f"Authentication failed: {err}")
    log.info("Microsoft Graph token acquired ✓")
    return result["access_token"]


# ───────────────────────────────────────────────────────────────────
#  STEP 2 — Fetch email + attachment (FIXED HERE)
# ───────────────────────────────────────────────────────────────────

def fetch_csv_attachment(token: str, date_str: str):
    headers = {
        "Authorization": f"Bearer {token}",
        "ConsistencyLevel": "eventual"   # REQUIRED for $search
    }

    subject_filter = f"Driveo Trip Report - {date_str}"

    params = {
        "$search": f'"subject:{subject_filter}"',
        "$top": "5",
    }

    log.info(f"Searching Graph API for subject: '{subject_filter}'")
    resp = requests.get(
        f"{GRAPH_BASE}/me/messages",
        headers=headers,
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    messages = resp.json().get("value", [])

    if not messages:
        log.warning("No matching email found in inbox.")
        return None

    msg = next((m for m in messages if m.get("hasAttachments")), None)
    if not msg:
        log.warning("Email found but no attachments present.")
        return None

    log.info(f"Email found → Subject: {msg['subject']}  |  Received: {msg['receivedDateTime']}")

    att_resp = requests.get(
        f"{GRAPH_BASE}/me/messages/{msg['id']}/attachments",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    att_resp.raise_for_status()
    attachments = att_resp.json().get("value", [])

    for att in attachments:
        name = att.get("name", "")
        if name.lower().endswith(".csv") or name.lower().endswith(".xlsx"):
            content = base64.b64decode(att["contentBytes"])
            log.info(f"Attachment downloaded: {name}  ({len(content):,} bytes)")
            return content, name

    log.warning("No CSV/XLSX attachment found in the email.")
    return None


# ───────────────────────────────────────────────────────────────────
#  STEP 3 — Parse CSV
# ───────────────────────────────────────────────────────────────────

def parse_csv(payload: bytes, filename: str) -> list:
    text = payload.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    log.info(f"Parsed {len(rows)} rows from '{filename}'")
    return rows


# ───────────────────────────────────────────────────────────────────
#  STEP 4 — Ingest into Supabase
# ───────────────────────────────────────────────────────────────────

def ingest(rows: list, report_date):
    SUPABASE_DB_URL = "postgresql+psycopg2://postgres.qkpmgzfwxoujiodlufue:FGTN4hxvi8xaLbjV@aws-0-ap-south-1.pooler.supabase.com:5432/postgres?sslmode=require"

# psycopg2 does NOT accept '+psycopg2' → remove it
    conn = psycopg2.connect(
    SUPABASE_DB_URL.replace("postgresql+psycopg2://", "postgresql://"),
    connect_timeout=15
)
    
    cur = conn.cursor()

    records = []
    for row in rows:
        records.append((
            (row.get("Vehicleno") or "").strip() or None,
            (row.get("Model") or "").strip() or None,
            clean_timestamp(row.get("Starttime")),
            clean_timestamp(row.get("Endtime")),
            (row.get("Duration") or "").strip() or None,
            clean_numeric(row.get("Distance")),
            clean_numeric(row.get("Start SOC")),
            clean_numeric(row.get("End SOC")),
            clean_numeric(row.get("Mileage")),
            clean_numeric(row.get("Avg Speed")),
            (row.get("Startloc") or "").strip() or None,
            (row.get("Endloc") or "").strip() or None,
            report_date,
        ))

    if not records:
        log.warning("No records to insert.")
        cur.close()
        conn.close()
        return

    insert_sql = f"""
        INSERT INTO "{DB_SCHEMA}".{DB_TABLE}
            (vehicleno, model, starttime, endtime, duration, distance,
             start_soc, end_soc, mileage, avg_speed, startloc, endloc, report_date)
        VALUES %s
        
    """
    execute_values(cur, insert_sql, records, page_size=500)
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    log.info(f"✓ Inserted {len(records)} rows")


# ───────────────────────────────────────────────────────────────────
#  MAIN
# ───────────────────────────────────────────────────────────────────

def main():
    date_str, report_date = yesterday()
    log.info(f"▶  Processing Driveo Trip Report for {date_str} ({report_date})")

    token = get_access_token()

    result = fetch_csv_attachment(token, date_str)
    if result is None:
        log.error("No attachment retrieved — pipeline aborted.")
        return

    payload, filename = result

    rows = parse_csv(payload, filename)
    if not rows:
        log.warning("CSV was empty — nothing to ingest.")
        return

    ingest(rows, report_date)
    log.info("Pipeline complete ✓")


if __name__ == "__main__":
    main()