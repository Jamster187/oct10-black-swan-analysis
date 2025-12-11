import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ==========================================================
# CONFIG
# ==========================================================

# Binance is your spot index
BINANCE_DB = "Testing_Data_Collection_Binance"
BINANCE_EXCHANGE_NAME = "Binance"

# Exchanges where we might have futures (Binance included)
# Gate intentionally excluded due to weird contract denomination.
EXCHANGES_FUTURES = [
    "Binance", "Bitfinex", "Bitget", "Bitmart", "Bitso", "Bitstamp",
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

# Crash / liquidation window (UTC)
START_TIME_STR = "2025-10-10 21:09:00"
END_TIME_STR   = "2025-10-10 22:00:00"

# Timestamp unit in your DB
TS_UNIT = "ms"   # change to "s" if timestamps are in seconds

# Column names in your OHLCV tables
TIME_COL   = "timestamp"
HIGH_COL   = "high"
LOW_COL    = "low"

# Only consider these quote assets (treat all as USD-ish)
USD_QUOTES = {"usd", "usdt", "usdc", "eur"}

OUT_CSV       = "futures_vs_binance_spot_basis_2025-10-10_2109_2200_with_high_low.csv"
OUT_PNG_MID   = "median_mid_basis_vs_binance_spot_2025-10-10_2109_2200.png"
OUT_PNG_HIGH  = "median_high_basis_vs_binance_spot_2025-10-10_2109_2200.png"
OUT_PNG_LOW   = "median_low_basis_vs_binance_spot_2025-10-10_2109_2200.png"


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


def quote_ident(name: str) -> str:
    """
    Safely quote a Postgres identifier, even if it contains :, ., or caps.
    """
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def ts_to_dt(ts, unit="ms"):
    if unit == "ms":
        return pd.to_datetime(ts, unit="ms", utc=True)
    else:
        return pd.to_datetime(ts, unit="s", utc=True)


def get_spot_and_futures_tables(conn, exchange_lower: str):
    """
    Returns:
        spot_tables    = [ ... ]
        futures_tables = [ ... ]

    Spot tables:    exchange_base_quote_1m
    Futures tables: exchange_base_quote:quote_1m (contain colon)

    We also skip tables with spaces/dots to avoid weird names.
    """
    pattern = f"{exchange_lower}\\_%\\_1m"

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
    for (name,) in rows:
        if " " in name or "." in name:
            continue
        if ":" in name:
            fut.append(name)
        else:
            spot.append(name)

    return spot, fut


def parse_market_from_table(table: str):
    """
    Normalize futures names by replacing ':' with '_', then parse:
      exchange_base_quote_1m
      exchange_base_quote:quote_1m -> same after ':'' -> '_'
    Returns base, quote (lowercase) or (None, None) if not parseable.
    """
    norm = table.replace(":", "_")
    parts = norm.split("_")
    if len(parts) < 4:
        return None, None
    base = parts[1].lower()
    quote = parts[2].lower()
    return base, quote


def fetch_ohlc_window(conn, table: str, start_ts: int, end_ts: int):
    """
    Fetch timestamp, high, low, mid price for given table in [start_ts, end_ts].
    Return DataFrame with columns: dt, high, low, mid
    """
    tbl = quote_ident(table)

    query = f"""
        SELECT {TIME_COL}, {HIGH_COL}, {LOW_COL}
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

    df = pd.DataFrame(rows, columns=["ts", "high", "low"])
    df["high"] = df["high"].astype(float)
    df["low"]  = df["low"].astype(float)
    df["mid"]  = (df["high"] + df["low"]) / 2.0
    df["dt"]   = df["ts"].apply(lambda x: ts_to_dt(x, TS_UNIT))
    return df[["dt", "high", "low", "mid"]]


# ==========================================================
# MAIN
# ==========================================================

def main():
    start_ts, end_ts = build_time_bounds(START_TIME_STR, END_TIME_STR, unit=TS_UNIT)
    print(f"Window ts: {start_ts} -> {end_ts}")

    # ------------------------------------------------------
    # 1) Connect to Binance and build the spot "index" map
    # ------------------------------------------------------
    try:
        conn_binance = psycopg2.connect(dbname=BINANCE_DB, **DB_CONFIG)
    except Exception as e:
        print(f"Could not connect to {BINANCE_DB}: {e}")
        return

    try:
        binance_spot_tables, binance_fut_tables = get_spot_and_futures_tables(
            conn_binance, BINANCE_EXCHANGE_NAME.lower()
        )
        print(f"Binance: {len(binance_spot_tables)} spot tables, {len(binance_fut_tables)} futures tables")

        binance_spot_map = {}  # (base, quote) -> table

        for tbl in binance_spot_tables:
            base, quote = parse_market_from_table(tbl)
            if base is None or quote is None:
                continue
            if quote not in USD_QUOTES:
                continue
            binance_spot_map[(base, quote)] = tbl

        print(f"Binance spot index markets (USD-like): {len(binance_spot_map)}")

        # ------------------------------------------------------
        # 2) For each exchange futures, compare vs Binance spot
        # ------------------------------------------------------
        all_basis_rows = []

        for ex in EXCHANGES_FUTURES:
            dbname = DB_TEMPLATE.format(ex)
            ex_lower = ex.lower()

            # Use existing Binance connection for Binance itself
            if ex == BINANCE_EXCHANGE_NAME:
                conn_ex = conn_binance
            else:
                try:
                    conn_ex = psycopg2.connect(dbname=dbname, **DB_CONFIG)
                except Exception as e:
                    print(f"\n===== {ex}: could not connect to {dbname}: {e}")
                    continue

            print(f"\n===== Processing {ex} ({dbname}) =====")

            try:
                spot_tables_ex, fut_tables_ex = get_spot_and_futures_tables(conn_ex, ex_lower)
                print(f"{len(spot_tables_ex)} spot tables, {len(fut_tables_ex)} futures tables")

                if not fut_tables_ex:
                    print("  No futures tables, skipping.")
                    if ex != BINANCE_EXCHANGE_NAME:
                        conn_ex.close()
                    continue

                matched_markets = 0

                for fut_tbl in fut_tables_ex:
                    base, quote = parse_market_from_table(fut_tbl)
                    if base is None or quote is None:
                        continue
                    if quote not in USD_QUOTES:
                        continue

                    key = (base, quote)
                    if key not in binance_spot_map:
                        # No Binance spot for this contract; skip
                        continue

                    binance_tbl = binance_spot_map[key]

                    # Fetch OHLC(mid) for spot and futures
                    df_spot = fetch_ohlc_window(conn_binance, binance_tbl, start_ts, end_ts)
                    df_fut  = fetch_ohlc_window(conn_ex,      fut_tbl,     start_ts, end_ts)

                    if df_spot.empty or df_fut.empty:
                        continue

                    df_spot = df_spot.rename(columns={
                        "high": "spot_high",
                        "low":  "spot_low",
                        "mid":  "spot_mid",
                    })
                    df_fut = df_fut.rename(columns={
                        "high": "fut_high",
                        "low":  "fut_low",
                        "mid":  "fut_mid",
                    })

                    merged = pd.merge(df_spot, df_fut, on="dt", how="inner")
                    if merged.empty:
                        continue

                    # Mid, high, and low basis vs Binance spot
                    merged["basis_mid_pct"]  = (merged["fut_mid"]  - merged["spot_mid"])   / merged["spot_mid"]   * 100.0
                    merged["basis_high_pct"] = (merged["fut_high"] - merged["spot_high"]) / merged["spot_high"] * 100.0
                    merged["basis_low_pct"]  = (merged["fut_low"]  - merged["spot_low"])  / merged["spot_low"]  * 100.0

                    merged["exchange"]  = ex
                    merged["base"]      = base
                    merged["quote"]     = quote

                    all_basis_rows.append(merged)
                    matched_markets += 1

                print(f"  Matched {matched_markets} futures markets vs Binance spot.")

            finally:
                # Don't close the Binance connection here; it's shared
                if ex != BINANCE_EXCHANGE_NAME:
                    conn_ex.close()

        if not all_basis_rows:
            print("No futures vs Binance spot pairs found in the window.")
            return

        basis_df = pd.concat(all_basis_rows, ignore_index=True)

        # Save detailed per-market basis data (mid/high/low)
        basis_df.to_csv(OUT_CSV, index=False)
        print(f"\nWrote per-market basis CSV (mid/high/low): {OUT_CSV}")

        # ------------------------------------------------------
        # 3) Aggregate per exchange: median basis per minute
        # ------------------------------------------------------
        grouped_mid = basis_df.groupby(["exchange", "dt"])["basis_mid_pct"].median().reset_index()
        grouped_high = basis_df.groupby(["exchange", "dt"])["basis_high_pct"].median().reset_index()
        grouped_low = basis_df.groupby(["exchange", "dt"])["basis_low_pct"].median().reset_index()

        pivot_mid = grouped_mid.pivot(index="dt", columns="exchange", values="basis_mid_pct")
        pivot_high = grouped_high.pivot(index="dt", columns="exchange", values="basis_high_pct")
        pivot_low = grouped_low.pivot(index="dt", columns="exchange", values="basis_low_pct")

        print("\nSample of aggregated median MID basis:")
        print(pivot_mid.head())

        # ------------------------------------------------------
        # 4) Plot MID basis
        # ------------------------------------------------------
        plt.figure(figsize=(12, 6))
        for ex in pivot_mid.columns:
            plt.plot(pivot_mid.index, pivot_mid[ex], label=ex)

        plt.axhline(0.0, color="black", linewidth=1, linestyle="--", alpha=0.7)
        plt.title(
            "Futures vs Binance spot: % difference during crash window (MID)\n"
            f"{START_TIME_STR} → {END_TIME_STR} (UTC), median across markets"
        )
        plt.ylabel("% difference from Binance spot (futures – spot) / spot × 100 (mid)")
        plt.xlabel("Time (UTC)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(OUT_PNG_MID, dpi=150)
        plt.close()
        print(f"Wrote median mid-basis chart: {OUT_PNG_MID}")

        # ------------------------------------------------------
        # 5) Plot HIGH basis
        # ------------------------------------------------------
        plt.figure(figsize=(12, 6))
        for ex in pivot_high.columns:
            plt.plot(pivot_high.index, pivot_high[ex], label=ex)

        plt.axhline(0.0, color="black", linewidth=1, linestyle="--", alpha=0.7)
        plt.title(
            "Futures vs Binance spot: % difference during crash window (HIGH)\n"
            f"{START_TIME_STR} → {END_TIME_STR} (UTC), median across markets"
        )
        plt.ylabel("% difference from Binance spot (futures – spot) / spot × 100 (high)")
        plt.xlabel("Time (UTC)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(OUT_PNG_HIGH, dpi=150)
        plt.close()
        print(f"Wrote median high-basis chart: {OUT_PNG_HIGH}")

        # ------------------------------------------------------
        # 6) Plot LOW basis
        # ------------------------------------------------------
        plt.figure(figsize=(12, 6))
        for ex in pivot_low.columns:
            plt.plot(pivot_low.index, pivot_low[ex], label=ex)

        plt.axhline(0.0, color="black", linewidth=1, linestyle="--", alpha=0.7)
        plt.title(
            "Futures vs Binance spot: % difference during crash window (LOW)\n"
            f"{START_TIME_STR} → {END_TIME_STR} (UTC), median across markets"
        )
        plt.ylabel("% difference from Binance spot (futures – spot) / spot × 100 (low)")
        plt.xlabel("Time (UTC)")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(OUT_PNG_LOW, dpi=150)
        plt.close()
        print(f"Wrote median low-basis chart: {OUT_PNG_LOW}")

    finally:
        conn_binance.close()


if __name__ == "__main__":
    main()
