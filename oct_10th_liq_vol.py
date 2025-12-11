import psycopg2
import pandas as pd
import re

# ==========================================================
# CONFIG
# ==========================================================

EXCHANGES = [
    "Binance", "Bitfinex", "Bitget", "Bitstamp",
    "Bitvavo", "Bybit", "Coinbase", "Cryptocom", "Gemini",
    "Kraken", "Kucoin", "OKX", "Probit"
]

DB_TEMPLATE = "Testing_Data_Collection_{}"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "YOUR_PASSWORD_HERE",
}

# Liquidation window
START_TIME_STR = "2025-10-10 21:09:00"
END_TIME_STR   = "2025-10-10 22:00:00"

# Timestamp unit in your DB
TS_UNIT = "ms"   # change to "s" if timestamps are in seconds

# Column names
TIME_COL   = "timestamp"
HIGH_COL   = "high"
LOW_COL    = "low"
VOLUME_COL = "volume"

# Only count these quote assets, treat all as USD
USD_QUOTES = {"usd", "usdt", "usdc", "eur"}

OUTPUT_CSV = "all_exchanges_liq_window_volume_spot_futures.csv"


# ==========================================================
# HELPERS
# ==========================================================

def build_time_bounds(start_str, end_str, unit="ms"):
    def to_epoch(s):
        dt = pd.to_datetime(s)
        ts = dt.timestamp()  # seconds
        if unit == "ms":
            ts *= 1000
        return int(ts)

    return to_epoch(start_str), to_epoch(end_str)


def get_spot_and_futures_tables(conn, exchange):
    """
    Returns:
        spot_tables    = [ ... ]
        futures_tables = [ ... ]

    Spot tables:    exchange_base_quote_1m
    Futures tables: exchange_base_quote:quote_1m  (contain colon)

    For Gate specifically, we enforce strict patterns so we don't ingest
    weird stuff like 'gate_game.com_usdt_1m' or 'gate_bitcoin file_usdt_1m'.
    """
    ex_lower = exchange.lower()
    pattern = f"{ex_lower}\\_%\\_1m"

    query = """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname='public'
          AND tablename LIKE %s;
    """

    with conn.cursor() as cur:
        cur.execute(query, (pattern,))
        rows = cur.fetchall()

    spot, fut = [], []

    # Strict patterns for Gate
    gate_spot_re = re.compile(r"^gate_[a-z0-9]+_(usdt|usdc|usd|eur)_1m$")
    gate_fut_re  = re.compile(r"^gate_[a-z0-9]+_(usdt|usdc|usd|eur):\1_1m$")

    for (name,) in rows:
        # Skip names with spaces or dots everywhere
        if " " in name or "." in name:
            continue

        if ex_lower == "gate":
            # Gate: enforce strict patterns
            if ":" in name:
                # futures
                if not gate_fut_re.match(name):
                    continue
                fut.append(name)
            else:
                # spot
                if not gate_spot_re.match(name):
                    continue
                spot.append(name)
        else:
            # Non-Gate: simpler rules
            if ":" in name:
                fut.append(name)
            else:
                spot.append(name)

    return spot, fut


def parse_market(table):
    """
    Assumes structure:
        exchange_base_quote_1m
    or  exchange_base_quote:quote_1m

    We normalize by replacing ':' with '_' before splitting.
    """
    name = table.replace(":", "_")
    parts = name.split("_")
    if len(parts) < 4:
        return None, None

    base = parts[1].lower()
    quote = parts[2].lower()
    return base, quote


def quote_ident(name: str) -> str:
    """
    Safely quote a Postgres identifier, even if it contains :, ., or caps.
    """
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def fetch_window(conn, table, start_ts, end_ts):
    """
    Fetch all rows in the liquidation window for a given table.
    Returns DataFrame with columns: timestamp, high, low, volume
    """
    tbl = quote_ident(table)

    query = f"""
        SELECT {TIME_COL}, {HIGH_COL}, {LOW_COL}, {VOLUME_COL}
        FROM {tbl}
        WHERE {TIME_COL} >= %s
          AND {TIME_COL} <= %s;
    """

    with conn.cursor() as cur:
        try:
            cur.execute(query, (start_ts, end_ts))
        except psycopg2.errors.UndefinedTable:
            print(f"  Skipping missing table: {table}")
            return pd.DataFrame()
        except Exception as e:
            print(f"  ERROR querying {table}: {e}")
            return pd.DataFrame()

        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[TIME_COL, HIGH_COL, LOW_COL, VOLUME_COL])
    df = df.astype({HIGH_COL: float, LOW_COL: float, VOLUME_COL: float})
    return df


# ==========================================================
# MAIN
# ==========================================================

def main():
    start_ts, end_ts = build_time_bounds(START_TIME_STR, END_TIME_STR, unit=TS_UNIT)
    print(f"Window ts: {start_ts} -> {end_ts}")

    global_spot_total = 0.0
    global_fut_total = 0.0

    master_rows = []

    for ex in EXCHANGES:
        dbname = DB_TEMPLATE.format(ex)
        print(f"\n===== Processing {ex} ({dbname}) =====")

        try:
            conn = psycopg2.connect(dbname=dbname, **DB_CONFIG)
        except Exception as e:
            print(f"Could not connect to {dbname}: {e}")
            continue

        try:
            spot_tables, fut_tables = get_spot_and_futures_tables(conn, ex)
            print(f"{len(spot_tables)} spot tables, {len(fut_tables)} futures tables")

            ex_spot_total = 0.0
            ex_fut_total = 0.0

            # -------------------------------
            # SPOT
            # -------------------------------
            for tbl in spot_tables:
                base, quote = parse_market(tbl)
                if base is None or quote is None:
                    continue

                if quote not in USD_QUOTES:
                    continue

                df = fetch_window(conn, tbl, start_ts, end_ts)
                if df.empty:
                    continue

                df["mid"] = (df[HIGH_COL] + df[LOW_COL]) / 2.0
                df["usd_volume"] = df["mid"] * df[VOLUME_COL]

                usd_sum = df["usd_volume"].sum()
                ex_spot_total += usd_sum

                master_rows.append({
                    "exchange": ex,
                    "type": "spot",
                    "table": tbl,
                    "base": base,
                    "quote": quote,
                    "usd_volume": usd_sum,
                })

            # -------------------------------
            # FUTURES
            # -------------------------------
            for tbl in fut_tables:
                base, quote = parse_market(tbl)
                if base is None or quote is None:
                    continue

                if quote not in USD_QUOTES:
                    continue

                df = fetch_window(conn, tbl, start_ts, end_ts)
                if df.empty:
                    continue

                df["mid"] = (df[HIGH_COL] + df[LOW_COL]) / 2.0
                df["usd_volume"] = df["mid"] * df[VOLUME_COL]

                usd_sum = df["usd_volume"].sum()
                ex_fut_total += usd_sum

                master_rows.append({
                    "exchange": ex,
                    "type": "futures",
                    "table": tbl,
                    "base": base,
                    "quote": quote,
                    "usd_volume": usd_sum,
                })

            ex_combined = ex_spot_total + ex_fut_total

            print(f"Spot total    : {ex_spot_total:,.2f} USD")
            print(f"Futures total : {ex_fut_total:,.2f} USD")
            print(f"Combined total: {ex_combined:,.2f} USD")

            global_spot_total += ex_spot_total
            global_fut_total += ex_fut_total

        finally:
            conn.close()

    # Save detailed per-market rows
    if master_rows:
        df = pd.DataFrame(master_rows)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"\nWrote per-market CSV: {OUTPUT_CSV}")

    # Global summary
    grand_total = global_spot_total + global_fut_total

    print("\n===== FINAL LIQUIDATION WINDOW SUMMARY =====")
    print(f"ALL SPOT TOTAL    : {global_spot_total:,.2f} USD")
    print(f"ALL FUTURES TOTAL : {global_fut_total:,.2f} USD")
    print(f"GRAND TOTAL       : {grand_total:,.2f} USD")


if __name__ == "__main__":
    main()
