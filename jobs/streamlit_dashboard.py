"""
Streamlit dashboard for the real-time lane — reads the windowed KPIs the
Spark Structured Streaming job writes to a shared JSON-lines file and
displays them as live-updating metrics and charts.

Run with:
    pip install streamlit pandas matplotlib
    streamlit run jobs/streamlit_dashboard.py

Then open the URL Streamlit prints (usually http://localhost:8501).
"""

import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

# Resolve the data file relative to this script's location, not the current
# working directory — so it works whether you run `streamlit run
# jobs/streamlit_dashboard.py` from the repo root or from inside jobs/.
DEFAULT_PATH = Path(__file__).resolve().parent.parent / "data" / "streaming_kpis.jsonl"
STREAM_FILE_PATH = Path(os.environ.get("STREAM_FILE_PATH", DEFAULT_PATH))

st.set_page_config(page_title="E-commerce Streaming KPIs", layout="wide")
st.title("E-commerce Streaming KPIs")
st.caption(f"Reading from: `{STREAM_FILE_PATH}`")


def load_data() -> pd.DataFrame:
    if not STREAM_FILE_PATH.exists():
        return pd.DataFrame(columns=["window_start", "window_end", "event_type", "event_count"])

    df = pd.read_json(STREAM_FILE_PATH, lines=True)
    if df.empty:
        return df

    # The Spark job runs in "update" output mode, so the same window can be
    # written more than once as late-arriving events adjust its count before
    # the watermark closes it. Keep only the last (freshest) value per
    # window + event_type, relying on file order = write order = recency.
    df = df.drop_duplicates(subset=["window_start", "event_type"], keep="last")
    df["window_start"] = pd.to_datetime(df["window_start"])
    df["window_end"] = pd.to_datetime(df["window_end"])
    return df.sort_values("window_start")


def render_table_markdown(rows: pd.DataFrame) -> str:
    # st.dataframe goes through pyarrow internally — on machines where a
    # Windows Application Control policy blocks pyarrow's native library
    # (common on school-managed devices), that widget fails outright. A plain
    # markdown table sidesteps pyarrow completely.
    header = "| Window start | Window end | Event type | Count |\n|---|---|---|---|\n"
    body = "\n".join(
        f"| {r.window_start} | {r.window_end} | {r.event_type} | {r.event_count} |"
        for r in rows.itertuples()
    )
    return header + body


# --- Sidebar controls: defined with session_state keys and OUTSIDE the
# fragment below, so interacting with them triggers a normal full rerun
# rather than being blocked by the fragment's own refresh cycle.
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = True
if "refresh_seconds" not in st.session_state:
    st.session_state.refresh_seconds = 10

st.sidebar.header("Controls")
st.sidebar.checkbox("Auto-refresh", key="auto_refresh")
st.sidebar.slider("Refresh interval (seconds)", 3, 30, key="refresh_seconds")
manual_refresh = st.sidebar.button("Refresh now")

run_every = st.session_state.refresh_seconds if st.session_state.auto_refresh else None


@st.fragment(run_every=run_every)
def render_dashboard():
    data = load_data()

    if data.empty:
        st.info(
            "No data yet. Make sure the Spark Structured Streaming job is running "
            "and the producer has sent at least one batch of events."
        )
        return

    totals = data.groupby("event_type")["event_count"].sum()
    cols = st.columns(len(totals) if len(totals) > 0 else 1)
    for col, (event_type, total) in zip(cols, totals.items()):
        col.metric(label=event_type.capitalize(), value=int(total))

    st.subheader("Events per minute over time")
    pivot = data.pivot_table(
        index="window_start", columns="event_type", values="event_count", fill_value=0
    )

    chart_col, bar_col = st.columns(2)

    with chart_col:
        # Built with matplotlib rather than st.line_chart, for the same pyarrow
        # reason noted above for the table.
        fig, ax = plt.subplots(figsize=(6, 4))
        x_values = pivot.index.to_pydatetime()
        for event_type in pivot.columns:
            ax.plot(x_values, pivot[event_type], label=event_type, marker="o")
        ax.set_xlabel("Window start")
        ax.set_ylabel("Event count")
        ax.set_title("Trend over time")
        ax.legend()

        if len(x_values) == 1:
            # A single point gives matplotlib nothing to scale a date axis from,
            # which can produce a huge, meaningless default range. Set an
            # explicit small window around the one point instead.
            pad = pd.Timedelta(minutes=5)
            ax.set_xlim(x_values[0] - pad, x_values[0] + pad)
            st.caption(
                "Only one time window so far — the chart will show a real trend "
                "once the producer sends events spanning more than one minute."
            )
        else:
            fig.autofmt_xdate()

        st.pyplot(fig)

    with bar_col:
        # A bar chart of totals gives a quick funnel-style comparison
        # (view -> cart -> purchase) alongside the time trend.
        bar_fig, bar_ax = plt.subplots(figsize=(6, 4))
        colors = {"view": "tab:green", "cart": "tab:blue", "purchase": "tab:orange"}
        bar_colors = [colors.get(et, "tab:gray") for et in totals.index]
        bar_ax.bar(totals.index, totals.values, color=bar_colors)
        bar_ax.set_xlabel("Event type")
        bar_ax.set_ylabel("Total events")
        bar_ax.set_title("Totals by event type")
        for i, value in enumerate(totals.values):
            bar_ax.text(i, value, str(int(value)), ha="center", va="bottom")
        st.pyplot(bar_fig)

    pie_col, cumulative_col = st.columns(2)

    with pie_col:
        # Same totals as the bar chart, shown as a proportion instead —
        # answers "what share of activity" rather than "how many".
        pie_fig, pie_ax = plt.subplots(figsize=(6, 4))
        colors = {"view": "tab:green", "cart": "tab:blue", "purchase": "tab:orange"}
        pie_colors = [colors.get(et, "tab:gray") for et in totals.index]
        percentages = 100 * totals.values / totals.values.sum()

        def label_if_large_enough(pct):
            # Skip the inline label on slices too thin to hold text without
            # overlapping their neighbor — common when one category (e.g.
            # views) dominates and the rest are single-digit percentages.
            return f"{pct:.1f}%" if pct >= 5 else ""

        pie_ax.pie(
            totals.values,
            labels=None,  # category names go in the legend instead, to avoid crowding
            autopct=label_if_large_enough,
            pctdistance=0.75,
            colors=pie_colors,
            startangle=90,
        )
        legend_labels = [f"{name} ({pct:.1f}%)" for name, pct in zip(totals.index, percentages)]
        pie_ax.legend(legend_labels, loc="center left", bbox_to_anchor=(1, 0.5))
        pie_ax.set_title("Share of total events")
        pie_ax.axis("equal")
        st.pyplot(pie_fig)

    with cumulative_col:
        # Running total over time — a different read than the per-minute
        # trend chart: this shows overall growth rather than momentary rate.
        cumulative = pivot.cumsum()
        cum_fig, cum_ax = plt.subplots(figsize=(6, 4))
        for event_type in cumulative.columns:
            cum_ax.plot(x_values, cumulative[event_type], label=event_type, marker="o")
        cum_ax.set_xlabel("Window start")
        cum_ax.set_ylabel("Cumulative event count")
        cum_ax.set_title("Cumulative growth over time")
        cum_ax.legend()

        if len(x_values) == 1:
            pad = pd.Timedelta(minutes=5)
            cum_ax.set_xlim(x_values[0] - pad, x_values[0] + pad)
        else:
            cum_fig.autofmt_xdate()

        st.pyplot(cum_fig)

    st.subheader("Most recent windows")
    recent = data.sort_values("window_start", ascending=False).head(20)
    st.markdown(render_table_markdown(recent))

    st.caption(f"Last loaded: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")


render_dashboard()

if manual_refresh:
    st.rerun()