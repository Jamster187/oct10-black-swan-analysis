import psycopg2
import psycopg2.extras
import pandas as pd

# ==========================
# CONFIG
# ==========================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "Testing_Data_Collection_Binance",  # <- change if needed
    "user": "postgres",                           # <- change if needed
    "password": "PleaseDontFuckMe123123123!!!",   # <- change if needed
}

# Date of interest (UTC in your candles)
TARGET_DATE = "2025-10-10"

# Table filter:
#   - assumes 1d OHLCV tables named like: binance_btc_usdt_1d
#   - you can relax this if you want e.g. tablename LIKE 'binance_%'
TABLE_NAME_LIKE = "binance\\_%\\_1d"   # PostgreSQL-style pattern; backslashes to escape underscores

# Column names in your OHLCV tables
TIME_COL = "timestamp"   # Unix timestamp in ms
HIGH_COL = "high"
LOW_COL  = "low"

# Output Excel file
OUTPUT_XLSX = "binance_oct10_drops.xlsx"


# ==========================
# HELPERS
# ==========================

def build_time_bounds(start_str, end_str, unit):
    """
    Convert human-readable datetime strings to numeric Unix timestamps
    in the specified unit ("s" or "ms").
    Returns (start_ts, end_ts).
    """
    def to_epoch(s):
        if s is None:
            return None
        dt = pd.to_datetime(s)
        ts = dt.timestamp()  # seconds
        if unit == "ms":
            ts *= 1000.0
        return int(ts)

    return to_epoch(start_str), to_epoch(end_str)


# Use ms because your timestamps are in milliseconds.
START_TS, END_TS = build_time_bounds(
    f"{TARGET_DATE} 00:00:00",
    f"{TARGET_DATE} 23:59:59",
    unit="ms",
)


def get_binance_tables(conn):
    """
    Get all binance 1d tables from public schema matching TABLE_NAME_LIKE.
    """
    query = """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
          AND tablename LIKE %s
        ORDER BY tablename;
    """
    with conn.cursor() as cur:
        cur.execute(query, (TABLE_NAME_LIKE,))
        rows = cur.fetchall()
    return [r[0] for r in rows]


def get_daily_stats_for_table(conn, table_name, start_ts, end_ts):
    """
    For tables with unix integer timestamps (ms).
    """
    query = f"""
        SELECT
            COUNT(*) AS n_rows,
            MAX({HIGH_COL}) AS day_high,
            MIN({LOW_COL})  AS day_low
        FROM {table_name}
        WHERE {TIME_COL} >= %s
          AND {TIME_COL} <= %s;
    """

    with conn.cursor() as cur:
        cur.execute(query, (start_ts, end_ts))
        n_rows, day_high, day_low = cur.fetchone()

    if n_rows == 0 or day_high is None or day_low is None:
        return None

    return {
        "n_rows": n_rows,
        "day_high": float(day_high),
        "day_low": float(day_low),
    }


def parse_market_from_table_name(table_name):
    """
    Given something like: binance_btc_usdt_1d
    Return:
        exchange = 'binance'
        market   = 'btc_usdt'
        timeframe = '1d'

    This also works if quote has a colon, e.g.:
        binance_sui_usdt:usdt_1d -> market = 'sui_usdt:usdt'
    """
    parts = table_name.split("_")
    if len(parts) < 3:
        return table_name, table_name, ""

    exchange = parts[0]
    timeframe = parts[-1]
    market = "_".join(parts[1:-1])

    return exchange, market, timeframe


# ==========================
# MAIN LOGIC
# ==========================

def main():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        print("Fetching binance tables...")
        tables = get_binance_tables(conn)
        print(f"Found {len(tables)} tables.")

        results = []

        for tbl in tables:

    # Skip futures tables (colon in table name)
            if ":" in tbl:
                continue
        
            stats = get_daily_stats_for_table(conn, tbl, START_TS, END_TS)
            if stats is None:
                continue
        
            day_high = stats["day_high"]
            day_low = stats["day_low"]
        
            # Avoid division by zero
            if day_high == 0:
                continue
        
            # Intraday drop magnitude (positive %)
            drop_pct = (day_high - day_low) / day_high * 100.0
        
            exchange, market, timeframe = parse_market_from_table_name(tbl)
        
            results.append({
                "table_name": tbl,
                "exchange": exchange,
                "market": market,
                "timeframe": timeframe,
                "n_candles_on_day": stats["n_rows"],
                "day_high": day_high,
                "day_low": day_low,
                "intraday_drop_pct": drop_pct,
            })


        if not results:
            print("No data found for the given date. Check timestamps / date.")
            return

        # Build DataFrame
        df = pd.DataFrame(results)

        # Sort by biggest drop first
        df.sort_values(by="intraday_drop_pct", ascending=False, inplace=True)

        # Save to Excel
        df.to_excel(OUTPUT_XLSX, index=False)
        print(f"Done. Wrote results to {OUTPUT_XLSX}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
