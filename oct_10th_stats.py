import psycopg2
import pandas as pd
import numpy as np

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "YOUR_PASSWORD_HERE",
}

DB_NAME = "Testing_Data_Collection_Binance"
TABLE_PATTERN = "binance\\_%\\_1d"

TARGET_DAY = pd.to_datetime("2025-10-10").date()

TIME_COL  = "timestamp"
OPEN_COL  = "open"
HIGH_COL  = "high"
LOW_COL   = "low"
CLOSE_COL = "close"

OUT_CSV = "oct10_zscores_clean.csv"


def quote_ident(name: str) -> str:
    """Safely quote ANY Postgres identifier."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def get_tables(conn):
    q = """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname='public'
          AND tablename LIKE %s;
    """
    with conn.cursor() as cur:
        cur.execute(q, (TABLE_PATTERN,))
        rows = cur.fetchall()

    out = []
    for (tbl,) in rows:
        if ":" in tbl:
            continue  # skip futures
        if " " in tbl or "." in tbl:
            continue  # skip malformed names
        out.append(tbl)

    return out


def ts_to_day(ts_ms):
    return pd.to_datetime(ts_ms, unit="ms", utc=True).date()


def main():
    conn = psycopg2.connect(dbname=DB_NAME, **DB_CONFIG)
    tables = get_tables(conn)

    print(f"Processing {len(tables)} tables...")

    rows = []

    for tbl in tables:
        tbl_q = quote_ident(tbl)

        q = f"""
            SELECT {TIME_COL}, {OPEN_COL}, {HIGH_COL}, {LOW_COL}
            FROM {tbl_q};
        """

        with conn.cursor() as cur:
            try:
                cur.execute(q)
            except Exception as e:
                print(f"Skipping {tbl} due to error: {e}")
                continue

            data = cur.fetchall()

        for ts_ms, o, h, l in data:
            if o is None or h is None or l is None:
                continue
            if o <= 0 or h <= 0 or l <= 0:
                continue

            day = ts_to_day(ts_ms)

            drop_pct  = (o - l) / o * 100.0
            pump_pct  = (h - l) / l * 100.0
            range_pct = (h - l) / o * 100.0

            rows.append((day, drop_pct, pump_pct, range_pct))

    df = pd.DataFrame(rows, columns=["day", "drop%", "pump%", "range%"])
    print(f"Total rows: {len(df)}")

    hist = df[df["day"] < TARGET_DAY]
    oct10 = df[df["day"] == TARGET_DAY]

    print(f"Historical rows: {len(hist)}")
    print(f"Oct 10 rows:     {len(oct10)}")

    results = []

    # ================================
    # drop% — untrimmed
    # ================================
    col = "drop%"
    mu = hist[col].mean()
    sigma = hist[col].std()
    oct_med = oct10[col].median()
    z = (oct_med - mu) / sigma

    print(f"\n=== {col} ===")
    print(f"mean : {mu:.4f}")
    print(f"std  : {sigma:.4f}")
    print(f"oct10 median: {oct_med:.4f}")
    print(f"z-score: {z:.2f}σ")

    results.append([col, "none", mu, sigma, oct_med, z])

    # ================================
    # pump% and range% — trimmed tails
    # ================================
    for col in ["pump%", "range%"]:
        lower = hist[col].quantile(0.001)
        upper = hist[col].quantile(0.999)

        hist_t = hist[(hist[col] >= lower) & (hist[col] <= upper)]

        mu = hist_t[col].mean()
        sigma = hist_t[col].std()
        oct_med = oct10[col].median()
        z = (oct_med - mu) / sigma

        print(f"\n=== {col} (trimmed) ===")
        print(f"Bounds: [{lower:.4f}, {upper:.4f}]")
        print(f"Rows kept: {len(hist_t)} / {len(hist)}")
        print(f"mean : {mu:.4f}")
        print(f"std  : {sigma:.4f}")
        print(f"oct10 median: {oct_med:.4f}")
        print(f"z-score: {z:.2f}σ")

        results.append([col, "trim0.1%-99.9%", mu, sigma, oct_med, z])

    out_df = pd.DataFrame(results, columns=["metric", "trim", "mean", "std", "oct10_median", "zscore"])
    out_df.to_csv(OUT_CSV, index=False)
    print(f"\nWrote {OUT_CSV}")


if __name__ == "__main__":
    main()
