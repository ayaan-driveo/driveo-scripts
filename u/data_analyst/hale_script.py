import requests
import pandas as pd
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values
import argparse
import sys

# ====================================
# CONFIG
# ====================================
AUTH_BASE_URL = "https://apiplatform.intellicar.in/api/standard"
DATA_URL      = "https://driveoapi.intellicar.io/api/v1/driveo/bulklastgpscandata"

USERNAME = "data.analyst@driveo.in"
PASSWORD = "QYoNUFAVip"

SUPABASE_DB_URL = (
    "postgresql://postgres.qkpmgzfwxoujiodlufue:FGTN4hxvi8xaLbjV"
    "@aws-0-ap-south-1.pooler.supabase.com:5432/postgres?sslmode=require"
)

# Before this hour → 'start', at/after → 'end'
DAY_END_HOUR = 18


# ====================================
# HELPERS
# ====================================
def clean_value(val):
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    return val


def clean_row(row_tuple):
    return tuple(clean_value(v) for v in row_tuple)


def ms_to_ts(ms_value):
    """Convert millisecond epoch → timezone-aware datetime, or None."""
    ts = pd.to_datetime(ms_value, unit="ms", errors="coerce")
    return None if pd.isna(ts) else ts.to_pydatetime().replace(tzinfo=timezone.utc)


def resolve_mode(now: datetime, forced: str | None) -> str:
    if forced:
        return forced
    return "start" if now.hour < DAY_END_HOUR else "end"


# ====================================
# AUTH
# ====================================
def get_token() -> str:
    res = requests.post(
        f"{AUTH_BASE_URL}/gettoken",
        json={"username": USERNAME, "password": PASSWORD},
        timeout=15,
    )
    res.raise_for_status()
    return res.json()["data"]["token"]


# ====================================
# FETCH
# ====================================
def fetch_bulk_data(token: str) -> list:
    res = requests.get(
        DATA_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    res.raise_for_status()
    return res.json().get("data", [])


# ====================================
# BUILD RECORDS
# ====================================
def build_records(data: list, mode: str, today, fetched_at: datetime) -> list:
    """
    Returns one flat dict per vehicle ready for vehicle_snapshots.
    _OEM suffix is stripped before dedup so HR55AU3636 and HR55AU3636_OEM
    are treated as the same vehicle and merged into one row.
    """
    raw: dict = {}   # keyed by normalized cid for dedup

    for item in data:
        gps     = item.get("gpsdata")     or {}
        vehicle = item.get("vehicledata") or {}
        odo_obj = vehicle.get("odometer") or {}
        soc_obj = vehicle.get("soc")      or {}

        cid = item.get("vehicleno")
        if not cid:
            continue
        cid = str(cid).strip().replace("_OEM", "")  # ← normalize before dedup

        row = {
            "cid":           cid,
            "soc":           soc_obj.get("value"),
            "soc_time":      ms_to_ts(soc_obj.get("lastupdatedat")),
            "odo":           odo_obj.get("value"),
            "odo_time":      ms_to_ts(odo_obj.get("lastupdatedat")),
            "fetched_at":    fetched_at,
            "lat":           gps.get("lat"),
            "lon":           gps.get("lng"),
            "location_time": ms_to_ts(gps.get("commtime")),
            "metric_type":   mode,
            "snapshot_date": today,
        }

        if cid not in raw:
            raw[cid] = row
        else:
            # merge: keep first non-null value for each field
            for k, v in row.items():
                if raw[cid][k] is None and v is not None:
                    raw[cid][k] = v

    return list(raw.values())


# ====================================
# UPSERT
# ====================================
def upsert_records(records: list):
    conn = psycopg2.connect(SUPABASE_DB_URL)
    cur  = conn.cursor()

    cur.execute("""
        CREATE TEMP TABLE tmp_snapshots (
            cid           TEXT,
            soc           NUMERIC,
            soc_time      TIMESTAMPTZ,
            odo           NUMERIC,
            odo_time      TIMESTAMPTZ,
            fetched_at    TIMESTAMPTZ,
            lat           NUMERIC,
            lon           NUMERIC,
            location_time TIMESTAMPTZ,
            metric_type   TEXT,
            snapshot_date DATE
        ) ON COMMIT DROP;
    """)

    execute_values(
        cur,
        "INSERT INTO tmp_snapshots VALUES %s",
        [
            clean_row((
                r["cid"],
                r["soc"],
                r["soc_time"],
                r["odo"],
                r["odo_time"],
                r["fetched_at"],
                r["lat"],
                r["lon"],
                r["location_time"],
                r["metric_type"],
                r["snapshot_date"],
            ))
            for r in records
        ],
    )

    cur.execute("""
        WITH deduped AS (
            SELECT
                cid,
                MAX(soc)           AS soc,
                MAX(soc_time)      AS soc_time,
                MAX(odo)           AS odo,
                MAX(odo_time)      AS odo_time,
                MAX(fetched_at)    AS fetched_at,
                MAX(lat)           AS lat,
                MAX(lon)           AS lon,
                MAX(location_time) AS location_time,
                metric_type,
                snapshot_date
            FROM tmp_snapshots
            GROUP BY cid, metric_type, snapshot_date
        )
        INSERT INTO "Analytics".vehicle_snapshots (
            cid, soc, soc_time, odo, odo_time,
            fetched_at, lat, lon, location_time,
            metric_type, snapshot_date
        )
        SELECT
            cid, soc, soc_time, odo, odo_time,
            fetched_at, lat, lon, location_time,
            metric_type, snapshot_date
        FROM deduped

        ON CONFLICT (cid, snapshot_date, metric_type) DO UPDATE SET
            soc           = COALESCE(EXCLUDED.soc,           "Analytics".vehicle_snapshots.soc),
            soc_time      = COALESCE(EXCLUDED.soc_time,      "Analytics".vehicle_snapshots.soc_time),
            odo           = COALESCE(EXCLUDED.odo,           "Analytics".vehicle_snapshots.odo),
            odo_time      = COALESCE(EXCLUDED.odo_time,      "Analytics".vehicle_snapshots.odo_time),
            fetched_at    = EXCLUDED.fetched_at,
            lat           = COALESCE(EXCLUDED.lat,           "Analytics".vehicle_snapshots.lat),
            lon           = COALESCE(EXCLUDED.lon,           "Analytics".vehicle_snapshots.lon),
            location_time = COALESCE(EXCLUDED.location_time, "Analytics".vehicle_snapshots.location_time);
    """)

    conn.commit()
    cur.close()
    conn.close()


# ====================================
# MAIN
# ====================================
def main():
    parser = argparse.ArgumentParser(
        description="Snapshot vehicle GPS/CAN data into Analytics.vehicle_snapshots."
    )
    parser.add_argument(
        "--mode",
        choices=["start", "end"],
        default=None,
        help=(
            f"Force run mode. If omitted: before {DAY_END_HOUR}:00 → start, "
            f"{DAY_END_HOUR}:00+ → end."
        ),
    )
    args = parser.parse_args()

    now        = datetime.now(tz=timezone.utc)
    today      = now.date()
    fetched_at = now
    mode       = resolve_mode(now, args.mode)

    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S UTC')}]  mode = {mode.upper()}")

    print("🔑 Fetching auth token...")
    token = get_token()

    print("📡 Fetching bulk GPS/CAN data...")
    data = fetch_bulk_data(token)
    print(f"   → {len(data)} vehicles returned by API")

    records = build_records(data, mode, today, fetched_at)
    print(f"📦 Records after dedup : {len(records)}")

    if not records:
        print("⚠️  No records — exiting.")
        sys.exit(0)

    print("💾 Upserting into Analytics.vehicle_snapshots ...")
    upsert_records(records)
    print(f"✅ Done — {len(records)} rows written  [metric_type = {mode}]")


# ====================================
if __name__ == "__main__":
    main()