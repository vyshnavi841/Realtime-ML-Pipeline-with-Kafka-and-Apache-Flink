import os
import json
import threading
import time
import requests
from datetime import datetime, timezone
import streamlit as st
import pandas as pd
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
FLINK_HOST = os.getenv("FLINK_JOBMANAGER_HOST", "flink-jobmanager")

# Initialize session state for feature store and metrics
if 'store' not in st.session_state:
    st.session_state['store'] = {}
if 'metrics' not in st.session_state:
    st.session_state['metrics'] = {
        "late_events_dropped": 0,
        "latest_event_ts": 0,
        "watermark": 0,
        "watermark_lag": 0.0
    }

# Thread 1: Consume computed features from the feature-store topic
def feature_listener():
    try:
        consumer = KafkaConsumer(
            "feature-store",
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for msg in consumer:
            val = msg.value
            entity_id = val.get("entity_id")
            feat_name = val.get("feature_name")
            if entity_id and feat_name:
                if entity_id not in st.session_state['store']:
                    st.session_state['store'][entity_id] = {}
                st.session_state['store'][entity_id][feat_name] = {
                    "value": val.get("feature_value"),
                    "computed_at": val.get("computed_at"),
                    "received_at": time.time()
                }
    except Exception as e:
        print(f"Error in feature listener: {e}")

# Thread 2: Consume user-events to track the latest event time for watermark lag calculations
def user_events_listener():
    try:
        consumer = KafkaConsumer(
            "user-events",
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for msg in consumer:
            val = msg.value
            ts_str = val.get("timestamp")
            if ts_str:
                try:
                    dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    ts_ms = int(dt.timestamp() * 1000)
                    if ts_ms > st.session_state['metrics']['latest_event_ts']:
                        st.session_state['metrics']['latest_event_ts'] = ts_ms
                except Exception as e:
                    pass
    except Exception as e:
        print(f"Error in user events listener: {e}")

# Thread 3: Query the Flink REST API for job metrics (Late events dropped and watermark lag)
def flink_metrics_listener():
    while True:
        try:
            resp = requests.get(f"http://{FLINK_HOST}:8081/jobs")
            jobs = resp.json().get("jobs", [])
            running_jobs = [j for j in jobs if j.get("status") == "RUNNING"]
            if running_jobs:
                job_id = running_jobs[0]["id"]
                
                details_resp = requests.get(f"http://{FLINK_HOST}:8081/jobs/{job_id}")
                vertices = details_resp.json().get("vertices", [])
                
                total_late = 0
                max_watermark = 0
                
                for v in vertices:
                    v_id = v["id"]
                    m_list_resp = requests.get(f"http://{FLINK_HOST}:8081/jobs/{job_id}/vertices/{v_id}/metrics")
                    metric_ids = [m["id"] for m in m_list_resp.json()]
                    
                    late_ids = [m for m in metric_ids if "numLateRecordsDropped" in m]
                    watermark_ids = [m for m in metric_ids if "currentInputWatermark" in m]
                    
                    if late_ids:
                        get_late = ",".join(late_ids)
                        m_val_resp = requests.get(f"http://{FLINK_HOST}:8081/jobs/{job_id}/vertices/{v_id}/metrics?get={get_late}")
                        for m_val in m_val_resp.json():
                            total_late += float(m_val.get("value", 0))
                            
                    if watermark_ids:
                        get_wm = ",".join(watermark_ids)
                        m_val_resp = requests.get(f"http://{FLINK_HOST}:8081/jobs/{job_id}/vertices/{v_id}/metrics?get={get_wm}")
                        for m_val in m_val_resp.json():
                            val = float(m_val.get("value", -9223372036854775808))
                            if val > max_watermark:
                                max_watermark = val
                                
                st.session_state['metrics']['late_events_dropped'] = int(total_late)
                if max_watermark > 0:
                    st.session_state['metrics']['watermark'] = max_watermark
                    
                    latest_et = st.session_state['metrics']['latest_event_ts']
                    if latest_et > 0:
                        # Watermark lag is the difference between latest event-time and latest watermark
                        st.session_state['metrics']['watermark_lag'] = max(0.0, (latest_et - max_watermark) / 1000.0)
        except Exception as e:
            pass
        time.sleep(1)

# Start background listener threads
if 'threads_started' not in st.session_state:
    t1 = threading.Thread(target=feature_listener, daemon=True)
    t1.start()
    t2 = threading.Thread(target=user_events_listener, daemon=True)
    t2.start()
    t3 = threading.Thread(target=flink_metrics_listener, daemon=True)
    t3.start()
    st.session_state['threads_started'] = True

# Main Streamlit UI layout
st.set_page_config(page_title="Real-Time ML Feature Pipeline Dashboard", layout="wide")

st.title("⚡ Real-Time ML Feature Pipeline Observability Dashboard")
st.markdown("This dashboard provides visibility into the Flink streaming pipeline health, latency metrics, and computed features in the feature store.")
st.markdown("---")

# Subheader: Pipeline Health and Metrics
st.subheader("📊 Pipeline System Observability Metrics")
col1, col2, col3, col4 = st.columns(4)

# Calculate feature freshness (wall-clock duration since receipt of update)
now = time.time()
click_rate_freshness = "N/A"
engagement_rate_freshness = "N/A"
latest_click_time = 0
latest_engage_time = 0

for ent_id, feats in st.session_state['store'].items():
    if 'click_rate' in feats:
        latest_click_time = max(latest_click_time, feats['click_rate']['received_at'])
    if 'engagement_rate' in feats:
        latest_engage_time = max(latest_engage_time, feats['engagement_rate']['received_at'])

if latest_click_time > 0:
    click_rate_freshness = f"{round(now - latest_click_time, 1)}s ago"
if latest_engage_time > 0:
    engagement_rate_freshness = f"{round(now - latest_engage_time, 1)}s ago"

with col1:
    st.metric(label="Click Rate Freshness", value=click_rate_freshness)
with col2:
    st.metric(label="Engagement Rate Freshness", value=engagement_rate_freshness)
with col3:
    st.metric(label="Late Events Dropped Counter", value=st.session_state['metrics']['late_events_dropped'])
with col4:
    lag_val = st.session_state['metrics']['watermark_lag']
    st.metric(label="Current Watermark Lag", value=f"{round(lag_val, 1)}s lag" if lag_val > 0 else "0s")

st.markdown("---")

# Subheader: Entity Lookup
st.subheader("🔍 Real-time Entity Feature Lookup")
search_id = st.text_input("Enter Target Entity ID (User ID or Content ID):", value="user_alpha")

if search_id in st.session_state['store']:
    entity_data = st.session_state['store'][search_id]
    rows = []
    for f_name, f_info in entity_data.items():
        try:
            comp_at = datetime.strptime(f_info["computed_at"].replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z")
            freshness = f"{round(now - f_info['received_at'], 2)}s ago"
        except Exception as e:
            freshness = "Just now"
            
        rows.append({
            "Feature Name": f_name,
            "Feature Value": f_info["value"],
            "Computed At (Event-Time)": f_info["computed_at"],
            "Freshness Metric (Wall-Clock)": freshness
        })
        
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True)
else:
    st.info(f"No computed features captured yet for entity: `{search_id}`. Waiting for Flink windows to emit updates...")

# Refresh UI periodically
time.sleep(1)
st.rerun()