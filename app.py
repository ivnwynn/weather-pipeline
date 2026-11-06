"""Streamlit dashboard: real-time view (Kafka) and historical view (MongoDB)."""
import json
import threading
import time
from collections import deque
from datetime import datetime, time as dtime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient

import config
import exports

st.set_page_config(page_title="Weather Streaming Dashboard", page_icon="🌦️", layout="wide")
st.markdown(
    """
    <style>
    .block-container {padding-top: 2rem;}
    [data-testid="stMetric"] {
        background: rgba(128,128,128,.08); border: 1px solid rgba(128,128,128,.2);
        padding: 12px 16px; border-radius: 12px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

COLUMNS = ["timestamp", "value", "metric_type", "sensor_id"]
LIVE_PAGE = "📈 Real-time Streaming View"
HISTORY_PAGE = "📊 Historical Data View"


# ============================================================ Kafka (live) ==
class StreamBuffer:
    """Background Kafka consumer keeping the most recent messages in memory."""

    def __init__(self, maxlen):
        self.maxlen = maxlen
        self._records = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self.connected = False
        self.status = "Connecting to Kafka..."
        threading.Thread(target=self._run, daemon=True, name="kafka-buffer").start()

    def snapshot(self):
        with self._lock:
            return list(self._records)

    @staticmethod
    def _parse(raw):
        try:
            d = json.loads(raw.decode("utf-8"))
            return {
                "timestamp": str(d["timestamp"]),
                "value": float(d["value"]),
                "metric_type": str(d["metric_type"]),
                "sensor_id": str(d["sensor_id"]),
            }
        except (ValueError, KeyError, TypeError, AttributeError):
            return None

    def _run(self):
        while True:
            consumer = None
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=config.KAFKA_BOOTSTRAP, enable_auto_commit=False
                )
                partitions = None
                while not partitions:
                    partitions = consumer.partitions_for_topic(config.KAFKA_TOPIC)
                    if not partitions:
                        self.status = f"Waiting for topic '{config.KAFKA_TOPIC}'..."
                        time.sleep(2)

                tps = [TopicPartition(config.KAFKA_TOPIC, p) for p in sorted(partitions)]
                consumer.assign(tps)
                begins, ends = consumer.beginning_offsets(tps), consumer.end_offsets(tps)
                per_partition = max(1, self.maxlen // len(tps))
                for tp in tps:  # prefill with the latest messages
                    consumer.seek(tp, max(begins[tp], ends[tp] - per_partition))

                self.connected, self.status = True, "Connected"
                for msg in consumer:
                    rec = self._parse(msg.value)
                    if rec:
                        with self._lock:
                            self._records.append(rec)
            except Exception as exc:
                self.connected, self.status = False, f"Kafka error: {exc}"
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass
            time.sleep(5)


@st.cache_resource
def get_buffer():
    return StreamBuffer(config.BUFFER_SIZE)


def consume_kafka_data(window_minutes=15, sensors=None, metrics=None):
    """Real-time processing: recent Kafka messages as a DataFrame (display timezone)."""
    records = get_buffer().snapshot()
    if not records:
        return pd.DataFrame(columns=COLUMNS)
    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(minutes=window_minutes)
    df = df[df["timestamp"] >= cutoff]
    if sensors:
        df = df[df["sensor_id"].isin(sensors)]
    if metrics:
        df = df[df["metric_type"].isin(metrics)]
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert(config.DISPLAY_TZ)
    return df


@st.fragment(run_every=config.REFRESH_SECONDS)
def live_view(sensors, metric, window):
    buf = get_buffer()
    df = consume_kafka_data(window, sensors)
    if not buf.connected:
        st.error(f"{buf.status} (broker: {config.KAFKA_BOOTSTRAP})")
    if df.empty:
        st.info("Waiting for streamed data... make sure `producer.py` is running.")
        return

    unit = config.metric_unit(metric)
    series_df = df[df["metric_type"] == metric]

    cols = st.columns(min(len(sensors), 4))
    for i, sensor in enumerate(sensors):
        s = series_df[series_df["sensor_id"] == sensor]
        with cols[i % len(cols)]:
            if s.empty:
                st.metric(config.sensor_label(sensor), "—")
                continue
            latest = s["value"].iloc[-1]
            delta = latest - s["value"].iloc[-2] if len(s) > 1 else None
            st.metric(
                config.sensor_label(sensor), f"{latest:.1f} {unit}",
                None if delta is None else f"{delta:+.2f}", delta_color="off",
            )

    if not series_df.empty:
        plot_df = series_df.assign(City=series_df["sensor_id"].map(config.sensor_label))
        fig = px.line(
            plot_df, x="timestamp", y="value", color="City", markers=True,
            labels={"timestamp": "Time", "value": config.metric_label(metric)},
        )
        fig.update_layout(
            height=420, margin=dict(l=10, r=10, t=30, b=10),
            legend=dict(orientation="h", y=1.1, x=0),
        )
        st.plotly_chart(fig, use_container_width=True)

    latest = (
        df.sort_values("timestamp").groupby(["sensor_id", "metric_type"]).tail(1)
        .pivot(index="sensor_id", columns="metric_type", values="value")
    )
    latest.index = [config.sensor_label(i) for i in latest.index]
    latest.columns = [config.metric_label(c) for c in latest.columns]
    st.subheader("Latest readings")
    st.dataframe(latest.round(2), use_container_width=True)

    now = pd.Timestamp.now(tz=config.DISPLAY_TZ).strftime("%H:%M:%S")
    st.caption(f"Last refresh {now} · {len(df):,} messages in window · Kafka: {buf.status}")


def render_live_page():
    st.title(LIVE_PAGE)
    st.caption(
        f"Live readings from Kafka topic `{config.KAFKA_TOPIC}` · refreshes every "
        f"{config.REFRESH_SECONDS}s · times in {config.DISPLAY_TZ}"
    )
    c1, c2, c3 = st.columns([2, 2, 1])
    sensors = c1.multiselect(
        "Cities", config.SENSORS, default=config.SENSORS, format_func=config.sensor_label
    )
    metric = c2.selectbox("Metric", list(config.METRIC_FIELDS), format_func=config.metric_label)
    window = c3.slider("Window (min)", 5, 60, 15, step=5)
    if not sensors:
        st.warning("Select at least one city.")
        return
    live_view(sensors, metric, window)


# ======================================================== MongoDB (history) ==
@st.cache_resource
def get_mongo():
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    return client


def mongo_ok():
    try:
        get_mongo()
        return True
    except Exception:
        return False


@st.cache_data(ttl=30, show_spinner=False)
def filter_options():
    coll = get_mongo()[config.MONGO_DB][config.RAW_COLLECTION]
    return sorted(coll.distinct("sensor_id")), sorted(coll.distinct("metric_type"))


def _to_frame(rows, time_col):
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df[time_col] = pd.to_datetime(df[time_col], utc=True).dt.tz_convert(config.DISPLAY_TZ)
    return df.sort_values(time_col).reset_index(drop=True)


def query_historical_data(sensors, metrics, start_utc, end_utc, limit):
    """Data retrieval from MongoDB (start/end are naive UTC datetimes)."""
    coll = get_mongo()[config.MONGO_DB][config.RAW_COLLECTION]
    query = {"timestamp": {"$gte": start_utc, "$lte": end_utc}}
    if sensors:
        query["sensor_id"] = {"$in": list(sensors)}
    if metrics:
        query["metric_type"] = {"$in": list(metrics)}
    cursor = (
        coll.find(query, {"_id": 0, "timestamp": 1, "value": 1, "metric_type": 1, "sensor_id": 1})
        .sort("timestamp", -1)
        .limit(int(limit))
    )
    return _to_frame(list(cursor), "timestamp")


def query_aggregates(sensors, metrics, start_utc, end_utc):
    """Windowed aggregates computed by Spark."""
    coll = get_mongo()[config.MONGO_DB][config.AGG_COLLECTION]
    query = {"window_start": {"$gte": start_utc, "$lte": end_utc}}
    if sensors:
        query["sensor_id"] = {"$in": list(sensors)}
    if metrics:
        query["metric_type"] = {"$in": list(metrics)}
    rows = list(coll.find(query, {"_id": 0}).sort("window_start", -1).limit(50000))
    for r in rows:
        r.pop("window_end", None)
    return _to_frame(rows, "window_start")


def naive_utc(ts):
    return ts.tz_convert("UTC").tz_localize(None).to_pydatetime()


PRESETS = {
    "Last 15 minutes": timedelta(minutes=15),
    "Last hour": timedelta(hours=1),
    "Last 6 hours": timedelta(hours=6),
    "Last 24 hours": timedelta(hours=24),
    "Last 7 days": timedelta(days=7),
    "All time": None,
    "Custom": "custom",
}


def sidebar_filters(sensors_avail, metrics_avail):
    st.sidebar.markdown("### Filters")
    sensors = st.sidebar.multiselect(
        "Cities", sensors_avail, default=sensors_avail, format_func=config.sensor_label
    )
    metrics = st.sidebar.multiselect(
        "Metrics", metrics_avail, default=metrics_avail, format_func=config.metric_label
    )
    preset = st.sidebar.selectbox("Time range", list(PRESETS), index=3)

    now = pd.Timestamp.now(tz="UTC")
    delta = PRESETS[preset]
    if delta is None:
        start, end = pd.Timestamp("2000-01-01", tz="UTC"), now + pd.Timedelta(days=1)
    elif delta == "custom":
        today = pd.Timestamp.now(tz=config.DISPLAY_TZ)
        d1 = st.sidebar.date_input("From date", value=today.date())
        t1 = st.sidebar.time_input("From time", value=dtime(0, 0))
        d2 = st.sidebar.date_input("To date", value=today.date())
        t2 = st.sidebar.time_input("To time", value=dtime(23, 59))
        start = pd.Timestamp(datetime.combine(d1, t1)).tz_localize(config.DISPLAY_TZ).tz_convert("UTC")
        end = pd.Timestamp(datetime.combine(d2, t2)).tz_localize(config.DISPLAY_TZ).tz_convert("UTC")
    else:
        start, end = now - pd.Timedelta(delta), now
    limit = st.sidebar.number_input("Max rows", 1000, 500000, 50000, step=5000)
    st.sidebar.button("🔄 Refresh data", use_container_width=True)
    return sensors, metrics, preset, start, end, limit


def build_summary(df):
    return (
        df.groupby(["sensor_id", "metric_type"])["value"]
        .agg(count="count", mean="mean", min="min", max="max", std="std")
        .reset_index()
    )


def render_export_tab(df, summary, agg_df, sig, filters_text):
    st.markdown("Generate formatted exports of the **currently filtered** data.")
    if st.button("⚙️ Prepare export files", type="primary"):
        with st.spinner("Building files..."):
            st.session_state["exports"] = {
                "sig": sig,
                "csv": exports.to_csv_bytes(df),
                "xlsx": exports.to_excel_bytes(df, summary, agg_df),
                "png": exports.make_chart_png(df),
                "pdf": exports.to_pdf_bytes(df, summary, filters_text),
            }
    ready = st.session_state.get("exports")
    if not ready or ready["sig"] != sig:
        st.info("Click **Prepare export files** after changing filters.")
        return
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    c1, c2, c3, c4 = st.columns(4)
    c1.download_button("⬇️ CSV", ready["csv"], f"weather_{stamp}.csv", "text/csv", use_container_width=True)
    c2.download_button(
        "⬇️ Excel", ready["xlsx"], f"weather_{stamp}.xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True,
    )
    c3.download_button("⬇️ PDF report", ready["pdf"], f"weather_{stamp}.pdf", "application/pdf", use_container_width=True)
    c4.download_button("⬇️ Chart (PNG)", ready["png"], f"weather_{stamp}.png", "image/png", use_container_width=True)


def render_history_page():
    st.title(HISTORY_PAGE)
    try:
        sensors_avail, metrics_avail = filter_options()
    except Exception as exc:
        st.error(f"MongoDB not reachable at {config.MONGO_URI}: {exc}")
        return
    if not sensors_avail:
        st.info("No stored data yet. Start `spark_streaming.py` and `producer.py`, then refresh.")
        return

    sensors, metrics, preset, start, end, limit = sidebar_filters(sensors_avail, metrics_avail)
    if start >= end:
        st.error("The start time must be before the end time.")
        return
    if not sensors or not metrics:
        st.warning("Select at least one city and one metric.")
        return

    start_utc, end_utc = naive_utc(start), naive_utc(end)
    df = query_historical_data(sensors, metrics, start_utc, end_utc, limit)
    if df.empty:
        st.warning("No records match the selected filters.")
        return
    if len(df) >= limit:
        st.warning(f"Row limit reached ({limit:,}); showing the most recent records only.")

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Records", f"{len(df):,}")
    k2.metric("Cities", df["sensor_id"].nunique())
    k3.metric("Metrics", df["metric_type"].nunique())
    k4.metric("Span", str(df["timestamp"].max() - df["timestamp"].min()).split(".")[0])

    summary = build_summary(df)
    agg_df = query_aggregates(sensors, metrics, start_utc, end_utc)
    tab_trend, tab_agg, tab_raw, tab_export = st.tabs(["📉 Trends", "🧮 Aggregation", "🗂️ Raw data", "📤 Export"])

    with tab_trend:
        for metric in sorted(df["metric_type"].unique()):
            sub = df[df["metric_type"] == metric]
            sub = sub.assign(City=sub["sensor_id"].map(config.sensor_label))
            fig = px.line(sub, x="timestamp", y="value", color="City",
                          labels={"timestamp": "Time", "value": config.metric_label(metric)},
                          title=config.metric_label(metric))
            fig.update_layout(height=340, margin=dict(l=10, r=10, t=50, b=10),
                              legend=dict(orientation="h", y=1.15, x=0))
            st.plotly_chart(fig, use_container_width=True)

    with tab_agg:
        st.subheader("Statistical summary")
        show = summary.assign(
            sensor_id=summary["sensor_id"].map(config.sensor_label),
            metric_type=summary["metric_type"].map(config.metric_label),
        ).rename(columns={"sensor_id": "City", "metric_type": "Metric"})
        st.dataframe(show.round(2), use_container_width=True, hide_index=True)

        st.subheader(f"Spark windowed aggregates ({config.AGG_WINDOW} windows)")
        if agg_df.empty:
            st.info("No aggregates yet. Make sure `spark_streaming.py` is running.")
        else:
            pick = st.selectbox("Metric", sorted(agg_df["metric_type"].unique()),
                                format_func=config.metric_label, key="agg_metric")
            sub = agg_df[agg_df["metric_type"] == pick]
            sub = sub.assign(City=sub["sensor_id"].map(config.sensor_label))
            fig = px.line(sub, x="window_start", y="avg_value", color="City", markers=True,
                          labels={"window_start": "Window start", "avg_value": f"Average {config.metric_label(pick)}"})
            fig.update_layout(height=360, margin=dict(l=10, r=10, t=30, b=10),
                              legend=dict(orientation="h", y=1.15, x=0))
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(
                agg_df.assign(window_start=agg_df["window_start"].dt.strftime("%Y-%m-%d %H:%M:%S"))
                .sort_values("window_start", ascending=False),
                use_container_width=True, hide_index=True,
            )

    with tab_raw:
        st.caption(f"{len(df):,} records · times in {config.DISPLAY_TZ}")
        st.dataframe(
            df.assign(timestamp=df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S"))
            .sort_values("timestamp", ascending=False),
            use_container_width=True, hide_index=True,
        )

    with tab_export:
        sig = (tuple(sensors), tuple(metrics), preset, str(start), str(end), len(df))
        filters_text = (
            f"Cities: {', '.join(config.sensor_label(s) for s in sensors)} | "
            f"Metrics: {', '.join(config.metric_label(m) for m in metrics)} | Range: {preset}"
        )
        render_export_tab(df, summary, agg_df, sig, filters_text)


# ================================================================== router ==
get_buffer()  # start the Kafka consumer thread as early as possible

st.sidebar.title("🌦️ Weather Pipeline")
page = st.sidebar.radio("Navigation", [LIVE_PAGE, HISTORY_PAGE], label_visibility="collapsed")
st.sidebar.caption("WeatherAPI → Kafka → Spark → MongoDB → Streamlit")
st.sidebar.markdown(
    f"**Kafka:** {'🟢' if get_buffer().connected else '🔴'}  \n"
    f"**MongoDB:** {'🟢' if mongo_ok() else '🔴'}"
)
st.sidebar.divider()

if page == LIVE_PAGE:
    render_live_page()
else:
    render_history_page()
