import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "Testing_Data_Collection_Binance",
    "user": "postgres",
    "password": "YOUR_PASSWORD_HERE",
}

TABLE_NAME_LIKE = "binance\\_%\\_1d"
TARGET_DAY = "2025-10-10"

TIME_COL  = "timestamp"
OPEN_COL  = "open"
HIGH_COL  = "high"
LOW_COL   = "low"

OUT_CSV = "daily_volatility_median_only.csv"
OUT_PNG = "median_volatility_2017_2025_with_oct10.png"


def get_binance_1d_tables(conn):
    q = """
        SELECT tablename
        FROM pg_tables
        WHERE schemaname='public'
          AND tablename LIKE %s;
    """
    with conn.cursor() as cur:
        cur.execute(q, (TABLE_NAME_LIKE,))
        rows = cur.fetchall()
    return [r[0] for r in rows if ":" not in r[0]]


def ts_to_day(ts_ms):
    return pd.to_datetime(ts_ms, unit="ms", utc=True).date()


def compute_daily_volatility(conn):
    tables = get_binance_1d_tables(conn)
    print(f"Found {len(tables)} spot 1d tables.")

    rows = []

    for tbl in tables:
        q = f"SELECT {TIME_COL}, {OPEN_COL}, {HIGH_COL}, {LOW_COL} FROM {tbl};"
        with conn.cursor() as cur:
            cur.execute(q)
            data = cur.fetchall()

        for ts_ms, o, h, l in data:
            if any(v is None for v in (o, h, l)): 
                continue
            if o == 0:
                continue

            day = ts_to_day(ts_ms)
            range_pct = (float(h) - float(l)) / float(o) * 100.0
            rows.append((day, range_pct))

    return pd.DataFrame(rows, columns=["day", "range_pct"])


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        df = compute_daily_volatility(conn)
        print(f"Loaded {len(df)} rows.")

        grouped = df.groupby("day")["range_pct"]
        daily_stats = grouped.median().to_frame(name="median")

        daily_stats.to_csv(OUT_CSV)
        print(f"Wrote {OUT_CSV}")

        target_date = pd.to_datetime(TARGET_DAY).date()
        oct10_median = daily_stats.loc[target_date]["median"]
        print(f"\nOct 10 Median Volatility = {oct10_median:.2f}%")

        # Plot
        plt.figure(figsize=(14, 6))
        ax = plt.gca()

        # Median volatility time series
        ax.plot(
            daily_stats.index,
            daily_stats["median"],
            label="Median volatility (2017–2025)",
            color="blue",
            linewidth=2,
        )

        # Oct 10 vertical line
        crash_ts = pd.to_datetime(TARGET_DAY)
        ax.vlines(
            x=crash_ts,
            ymin=0,
            ymax=oct10_median,
            colors="purple",
            linewidth=3,
            label=f"Oct 10 median ({oct10_median:.1f}%)"
        )

        ax.set_ylabel("Median volatility %")
        ax.set_title("Median Daily Volatility (2017–2025) with Oct 10 Highlighted")
        ax.grid(True)

        handles, labels = ax.get_legend_handles_labels()
        uniq = dict(zip(labels, handles))
        ax.legend(uniq.values(), uniq.keys(), loc="upper right")

        plt.tight_layout()
        plt.savefig(OUT_PNG, dpi=150)
        plt.close()

        print(f"Wrote {OUT_PNG}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
