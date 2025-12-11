#!/usr/bin/env python3
"""
PostgreSQL Candle Visualizer (hard-coded config, human-readable time window)

- Connection info is hard-coded in DB_CONFIG.
- Market/table, columns, timeframe, and query period are hard-coded in CONFIG.
- Start/end times are human-readable strings and converted to Unix timestamps.
- Query uses BETWEEN start_ts AND end_ts (inclusive).
- Candles plotted on true time axis; gaps in timestamps show up as visual gaps.
"""

import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle


# ================== HARD-CODED DATABASE CONFIG ==================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "Testing_Data_Collection_Binance",  # <- change if needed
    "user": "postgres",                           # <- change if needed
    "password": "PleaseDontFuckMe123123123!!!",             # <- change if needed
}

# ================== HARD-CODED QUERY / MARKET CONFIG ============

CONFIG = {
    # Table to visualize (one specific market)
    "table_name": "binance_render_usdt_1m",  # <- change per market

    # Column names in the table
    "timestamp_col": "timestamp",
    "open_col": "open",
    "high_col": "high",
    "low_col": "low",
    "close_col": "close",
    "volume_col": "volume",

    # Timestamp unit in DB: "s" for seconds, "ms" for milliseconds
    "timestamp_unit": "ms",

    # Candle timeframe in seconds (used for gap detection & candle width)
    "timeframe_sec": 60,  # 60 = 1m, 300 = 5m, etc.

    # --------- QUERY PERIOD (HUMAN-READABLE) ---------
    # Set these to the period you want to plot.
    # Any format parseable by pandas.to_datetime is fine.
    # Example: "2024-10-10 00:00:00"
    #          "2024-10-10 23:59:59"
    #
    # If you want "full history", set one or both to None.

    "start_time_str": "2025-10-10 21:09:00",  # <- edit
    "end_time_str":   "2025-10-10 22:00:00",  # <- edit
}

# ================================================================


def connect_to_db():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )
    print("Connected.")
    return conn


def build_time_bounds(start_str, end_str, unit):
    """
    Convert human-readable datetime strings to numeric Unix timestamps
    in the specified unit ("s" or "ms").
    Returns (start_ts, end_ts) where each can be None.
    """
    def to_epoch(s):
        if s is None:
            return None
        dt = pd.to_datetime(s)
        ts = dt.timestamp()  # seconds
        if unit == "ms":
            ts *= 1000.0
        return int(ts)

    start_ts = to_epoch(start_str) if start_str else None
    end_ts = to_epoch(end_str) if end_str else None
    return start_ts, end_ts


def build_query(cfg, start_ts, end_ts):
    t = cfg["table_name"]
    ts_col = cfg["timestamp_col"]
    o_col = cfg["open_col"]
    h_col = cfg["high_col"]
    l_col = cfg["low_col"]
    c_col = cfg["close_col"]
    v_col = cfg["volume_col"]

    query = f"""
        SELECT
            {ts_col} AS ts,
            {o_col}  AS o,
            {h_col}  AS h,
            {l_col}  AS l,
            {c_col}  AS c,
            {v_col}  AS v
        FROM {t}
    """

    params = []
    where_clauses = []

    # Use BETWEEN (inclusive) if both bounds exist,
    # otherwise use >= or <= when only one side is specified.
    if start_ts is not None and end_ts is not None:
        where_clauses.append(f"{ts_col} BETWEEN %s AND %s")
        params.extend([start_ts, end_ts])
    elif start_ts is not None:
        where_clauses.append(f"{ts_col} >= %s")
        params.append(start_ts)
    elif end_ts is not None:
        where_clauses.append(f"{ts_col} <= %s")
        params.append(end_ts)

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += f" ORDER BY {ts_col} ASC"

    return query, params


def fetch_candles(conn, cfg):
    start_ts, end_ts = build_time_bounds(
        cfg["start_time_str"],
        cfg["end_time_str"],
        cfg["timestamp_unit"],
    )

    query, params = build_query(cfg, start_ts, end_ts)

    print("\nRunning query:")
    print(query)
    print("Params:", params)

    df = pd.read_sql_query(query, conn, params=params)
    print(f"Fetched {len(df)} rows.")
    return df


def prepare_dataframe(df, cfg):
    if df.empty:
        return df

    unit = cfg["timestamp_unit"]
    # Convert numeric timestamps to datetime index
    df["datetime"] = pd.to_datetime(df["ts"], unit=unit)
    df.set_index("datetime", inplace=True)
    df.sort_index(inplace=True)
    return df


def detect_gaps(df, cfg):
    """
    Return list of (gap_start, gap_end, multiple) for big gaps
    in terms of the configured timeframe.
    """
    timeframe_sec = cfg["timeframe_sec"]

    if df.empty or len(df) < 2:
        return []

    times = df.index.to_pydatetime()
    secs = np.array([t.timestamp() for t in times])
    diffs = np.diff(secs)

    gaps = []
    for i, delta in enumerate(diffs):
        if delta > 1.5 * timeframe_sec:
            gap_start = times[i]
            gap_end = times[i + 1]
            multiple = delta / timeframe_sec
            gaps.append((gap_start, gap_end, multiple))

    return gaps


def plot_candles(df, cfg):
    if df.empty:
        print("No data to plot.")
        return

    timeframe_sec = cfg["timeframe_sec"]
    table_name = cfg["table_name"]
    start_str = cfg["start_time_str"]
    end_str = cfg["end_time_str"]

    datetimes = df.index.to_pydatetime()
    date_nums = mdates.date2num(datetimes)

    opens = df["o"].values
    highs = df["h"].values
    lows = df["l"].values
    closes = df["c"].values

    # Candle width based on average spacing in time
    if len(date_nums) > 1:
        deltas = np.diff(date_nums)
        avg_delta = np.mean(deltas)
        candle_width = avg_delta * 0.8
    else:
        # Single candle: use timeframe converted to days
        candle_width = (timeframe_sec / 86400.0) * 0.8

    fig, ax = plt.subplots(figsize=(14, 7))

    for x, o, h, l, c in zip(date_nums, opens, highs, lows, closes):
        # Wick
        ax.plot([x, x], [l, h], linewidth=1)

        # Body
        color = "green" if c >= o else "red"
        lower = min(o, c)
        height = abs(c - o)
        if height == 0:
            # Make perfectly flat candles visible
            height = (max(highs) - min(lows)) * 0.001

        rect = Rectangle(
            (x - candle_width / 2.0, lower),
            candle_width,
            height,
            edgecolor="black",
            facecolor=color,
            linewidth=0.5,
        )
        ax.add_patch(rect)

    ax.set_xlabel("Time")
    ax.set_ylabel("Price")

    period_str = ""
    if start_str or end_str:
        period_str = f"\n[{start_str or '-∞'}  →  {end_str or '+∞'}]"

    ax.set_title(f"Candle Reconstruction for {table_name}{period_str}")

    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M:%S"))
    fig.autofmt_xdate()

    plt.tight_layout()
    plt.show()


def main():
    print("======================================")
    print("  PostgreSQL Candle Visualizer v2.0  ")
    print("======================================")
    print(f"DB  : {DB_CONFIG['dbname']}")
    print(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"Table: {CONFIG['table_name']}")
    print(f"Period: {CONFIG['start_time_str']}  ->  {CONFIG['end_time_str']}")
    print("======================================\n")

    conn = connect_to_db()
    try:
        df_raw = fetch_candles(conn, CONFIG)
        df = prepare_dataframe(df_raw, CONFIG)

        # Gap report
        gaps = detect_gaps(df, CONFIG)
        if gaps:
            print("\nDetected gaps ( > 1.5x expected interval ):")
            for start, end, multiple in gaps:
                print(f"  Gap from {start} to {end} (~{multiple:.2f}x timeframe)")
        else:
            print("\nNo significant gaps detected.")

        plot_candles(df, CONFIG)
    finally:
        conn.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()
