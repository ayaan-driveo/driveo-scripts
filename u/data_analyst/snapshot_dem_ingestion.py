import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone, date, timedelta
import argparse
import sys

# ====================================
# CONFIG
# ====================================
SUPABASE_DB_URL = (
    "postgresql://postgres.qkpmgzfwxoujiodlufue:FGTN4hxvi8xaLbjV"
    "@aws-0-ap-south-1.pooler.supabase.com:5432/postgres?sslmode=require"
)


# ====================================
# FETCH + PIVOT t-1 FROM vehicle_snapshots
# ====================================
def fetch_pivoted(conn, target_date: date) -> list[dict]:
    query = """
        SELECT
            REPLACE(cid, '_OEM', '') AS cid,
            snapshot_date,

            MAX(soc) FILTER (WHERE metric_type = 'start')      AS first_soc,
            MAX(soc_time) FILTER (WHERE metric_type = 'start') AS event_time_of_first_soc,
            MAX(soc) FILTER (WHERE metric_type = 'end')        AS last_soc,
            MAX(soc_time) FILTER (WHERE metric_type = 'end')   AS event_time_of_last_soc,
            MAX(odo) FILTER (WHERE metric_type = 'start')      AS min_odo,
            MAX(odo) FILTER (WHERE metric_type = 'end')        AS max_odo,
            MAX(lat) FILTER (WHERE metric_type = 'start')      AS start_lat,
            MAX(lon) FILTER (WHERE metric_type = 'start')      AS start_lon,
            MAX(lat) FILTER (WHERE metric_type = 'end')        AS end_lat,
            MAX(lon) FILTER (WHERE metric_type = 'end')        AS end_lon

        FROM "Analytics".vehicle_snapshots
        WHERE snapshot_date = %s
          AND metric_type IN ('start', 'end')
        GROUP BY REPLACE(cid, '_OEM', ''), snapshot_date
    """
    cur = conn.cursor()
    cur.execute(query, (target_date,))
    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()

    # Quick coverage report
    start_count = sum(1 for r in rows if r["first_soc"] is not None or r["min_odo"] is not None)
    end_count   = sum(1 for r in rows if r["last_soc"]  is not None or r["max_odo"]  is not None)
    both_count  = sum(1 for r in rows if (r["first_soc"] is not None or r["min_odo"] is not None)
                                      and (r["last_soc"] is not None or r["max_odo"] is not None))
    print(f"   → {len(rows)} total cid(s) for {target_date}")
    print(f"   → {start_count} have start data | {end_count} have end data | {both_count} have both")

    return rows


# ====================================
# COMPUTE DERIVED FIELDS
# ====================================
def enrich(rows: list[dict]) -> list[dict]:
    for r in rows:
        min_odo = r.get("min_odo")
        max_odo = r.get("max_odo")

        if min_odo is not None and max_odo is not None:
            diff = round(max_odo - min_odo, 4)
            # Guard against bad odo data (e.g. odo reset or stale start reading)
            r["total_util"] = diff if diff >= 0 else None
        else:
            r["total_util"] = None

        # Extend these later when source is confirmed
        r["total_charge_consumed"] = None
        r["total_charge_added"]    = None
        r["range_km"]              = None

    return rows


# ====================================
# UPSERT INTO day_end_metrics
# ====================================
def upsert_day_end_metrics(conn, rows: list[dict]):
    cur = conn.cursor()

    cur.execute("""
        CREATE TEMP TABLE tmp_day_end (
            cid                     TEXT,
            date                    DATE,
            first_soc               DOUBLE PRECISION,
            event_time_of_first_soc TIMESTAMPTZ,
            last_soc                DOUBLE PRECISION,
            event_time_of_last_soc  TIMESTAMPTZ,
            min_odo                 DOUBLE PRECISION,
            max_odo                 DOUBLE PRECISION,
            total_util              DOUBLE PRECISION,
            start_lat               DOUBLE PRECISION,
            start_lon               DOUBLE PRECISION,
            end_lat                 DOUBLE PRECISION,
            end_lon                 DOUBLE PRECISION,
            total_charge_consumed   DOUBLE PRECISION,
            total_charge_added      DOUBLE PRECISION,
            range_km                DOUBLE PRECISION
        ) ON COMMIT DROP;
    """)

    execute_values(
        cur,
        "INSERT INTO tmp_day_end VALUES %s",
        [
            (
                r["cid"],
                r["snapshot_date"],
                r["first_soc"],
                r["event_time_of_first_soc"],
                r["last_soc"],
                r["event_time_of_last_soc"],
                r["min_odo"],
                r["max_odo"],
                r["total_util"],
                r["start_lat"],
                r["start_lon"],
                r["end_lat"],
                r["end_lon"],
                r["total_charge_consumed"],
                r["total_charge_added"],
                r["range_km"],
            )
            for r in rows
        ],
    )

    cur.execute("""
        WITH deduped AS (
            SELECT
                cid,
                date,
                MAX(first_soc)               AS first_soc,
                MAX(event_time_of_first_soc) AS event_time_of_first_soc,
                MAX(last_soc)                AS last_soc,
                MAX(event_time_of_last_soc)  AS event_time_of_last_soc,
                MAX(min_odo)                 AS min_odo,
                MAX(max_odo)                 AS max_odo,
                MAX(total_util)              AS total_util,
                MAX(start_lat)               AS start_lat,
                MAX(start_lon)               AS start_lon,
                MAX(end_lat)                 AS end_lat,
                MAX(end_lon)                 AS end_lon,
                MAX(total_charge_consumed)   AS total_charge_consumed,
                MAX(total_charge_added)      AS total_charge_added,
                MAX(range_km)                AS range_km
            FROM tmp_day_end
            GROUP BY cid, date
        )
        INSERT INTO "Analytics".day_end_metrics (
            cid, date,
            first_soc, event_time_of_first_soc,
            last_soc, event_time_of_last_soc,
            min_odo, max_odo, total_util,
            start_lat, start_lon, end_lat, end_lon,
            total_charge_consumed, total_charge_added, range_km
        )
        SELECT
            cid, date,
            first_soc, event_time_of_first_soc,
            last_soc, event_time_of_last_soc,
            min_odo, max_odo, total_util,
            start_lat, start_lon, end_lat, end_lon,
            total_charge_consumed, total_charge_added, range_km
        FROM deduped
        ON CONFLICT (cid, date) DO UPDATE SET
            first_soc               = COALESCE(EXCLUDED.first_soc,               day_end_metrics.first_soc),
            event_time_of_first_soc = COALESCE(EXCLUDED.event_time_of_first_soc, day_end_metrics.event_time_of_first_soc),
            last_soc                = COALESCE(EXCLUDED.last_soc,                day_end_metrics.last_soc),
            event_time_of_last_soc  = COALESCE(EXCLUDED.event_time_of_last_soc,  day_end_metrics.event_time_of_last_soc),
            min_odo                 = COALESCE(EXCLUDED.min_odo,                 day_end_metrics.min_odo),
            max_odo                 = COALESCE(EXCLUDED.max_odo,                 day_end_metrics.max_odo),
            total_util              = COALESCE(EXCLUDED.total_util,              day_end_metrics.total_util),
            start_lat               = COALESCE(EXCLUDED.start_lat,               day_end_metrics.start_lat),
            start_lon               = COALESCE(EXCLUDED.start_lon,               day_end_metrics.start_lon),
            end_lat                 = COALESCE(EXCLUDED.end_lat,                 day_end_metrics.end_lat),
            end_lon                 = COALESCE(EXCLUDED.end_lon,                 day_end_metrics.end_lon);
    """)

    conn.commit()
    cur.close()


# ====================================
# MAIN
# ====================================
def main():
    parser = argparse.ArgumentParser(
        description="Backfill day_end_metrics from vehicle_snapshots for t-1."
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Override target date YYYY-MM-DD. Defaults to yesterday (UTC).",
    )
    args = parser.parse_args()

    # Default is always yesterday — this runs at 3am so t-1 is complete
    if args.date:
        target_date = date.fromisoformat(args.date)
    else:
        target_date = datetime.now(tz=timezone.utc).date() - timedelta(days=1)

    now = datetime.now(tz=timezone.utc)
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S UTC')}]")
    print(f"📅 Target date (t-1): {target_date}")

    print("🔌 Connecting to Supabase...")
    conn = psycopg2.connect(SUPABASE_DB_URL)

    print("📊 Fetching + pivoting vehicle_snapshots...")
    rows = fetch_pivoted(conn, target_date)

    if not rows:
        print("⚠️  No snapshot data found for this date — exiting.")
        conn.close()
        sys.exit(0)

    rows = enrich(rows)

    print("💾 Upserting into Analytics.day_end_metrics...")
    upsert_day_end_metrics(conn, rows)

    conn.close()
    print(f"✅ Done — {len(rows)} rows upserted into day_end_metrics for {target_date}")


if __name__ == "__main__":
    main()