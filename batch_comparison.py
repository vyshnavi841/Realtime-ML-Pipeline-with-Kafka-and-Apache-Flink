import os
import json
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def consume_topic(topic, timeout_ms=5000):
    """Consume all messages from a topic up to the current high watermark."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    messages = []
    start_time = time.time()
    while True:
        records = consumer.poll(timeout_ms=1000)
        if not records:
            break
        for partition, msgs in records.items():
            for msg in msgs:
                messages.append(msg.value)
        if time.time() - start_time > timeout_ms / 1000.0:
            break
            
    consumer.close()
    return messages

def main():
    print("Consuming raw user-events, content-metadata, and feature-store logs...")
    user_events = consume_topic("user-events")
    content_metadata = consume_topic("content-metadata")
    streaming_features = consume_topic("feature-store")
    
    if not user_events:
        print("No user events found. Is the pipeline running?")
        return
        
    print(f"Captured {len(user_events)} user events and {len(content_metadata)} metadata events.")
    print(f"Captured {len(streaming_features)} streaming feature updates from the feature store.")
    
    # 1. Prepare DataFrames
    df_events = pd.DataFrame(user_events)
    df_events['timestamp'] = pd.to_datetime(df_events['timestamp'])
    
    df_meta = pd.DataFrame(content_metadata)
    # Deduplicate metadata to get the latest category per content_id
    if not df_meta.empty:
        df_meta = df_meta.sort_values('publish_timestamp').groupby('content_id').last().reset_index()
    
    batch_features = {}
    
    # 2. Compute per-user features (1-Hour Tumbling Window)
    df_events['hour_start'] = df_events['timestamp'].dt.floor('1h')
    for (user_id, hour_start), group in df_events.groupby(['user_id', 'hour_start']):
        clicks = (group['event_type'] == 'click').sum()
        total = len(group)
        click_rate = float(clicks) / total
        avg_dwell = group['dwell_time_ms'].mean()
        
        # Flink rowtime for 1-hour tumbling window
        rowtime = hour_start + timedelta(hours=1) - timedelta(seconds=1)
        computed_at = rowtime.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Click Rate
        batch_features[(user_id, "click_rate", computed_at)] = str(click_rate)
        # Avg Dwell Time
        batch_features[(user_id, "avg_dwell_time", computed_at)] = str(int(avg_dwell))

    # 3. Compute per-content features (15-min Sliding Window, 5-min slide)
    if not df_events.empty:
        min_time = df_events['timestamp'].min().floor('5min')
        max_time = df_events['timestamp'].max().ceil('5min')
        
        current_start = min_time
        # Loop through possible sliding window starts
        while current_start + timedelta(minutes=15) <= max_time + timedelta(minutes=15):
            window_end = current_start + timedelta(minutes=15)
            mask = (df_events['timestamp'] >= current_start) & (df_events['timestamp'] < window_end)
            win_df = df_events[mask]
            
            rowtime = window_end - timedelta(seconds=1)
            computed_at = rowtime.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            if not win_df.empty:
                for cid, group in win_df.groupby('content_id'):
                    views = (group['event_type'] == 'view').sum()
                    likes = (group['event_type'] == 'like').sum()
                    shares = (group['event_type'] == 'share').sum()
                    
                    engagement_rate = 0.0
                    if views > 0:
                        engagement_rate = (likes + shares) / views
                        
                    batch_features[(cid, "engagement_rate", computed_at)] = str(engagement_rate)
            current_start += timedelta(minutes=5)

    # 4. Compute Category Affinity Score (Stream-Table Join, 1-Hour Tumbling Window)
    if not df_events.empty and not df_meta.empty:
        enriched_df = pd.merge(df_events, df_meta, on='content_id', suffixes=('_event', '_meta'))
        for (user_id, category, hour_start), group in enriched_df.groupby(['user_id', 'category', 'hour_start']):
            rowtime = hour_start + timedelta(hours=1) - timedelta(seconds=1)
            computed_at = rowtime.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            feat_name = f"affinity_{category}"
            batch_features[(user_id, feat_name, computed_at)] = str(len(group))

    # 5. Extract streaming features from feature store
    stream_features_dict = {}
    for feat in streaming_features:
        ent_id = feat.get("entity_id")
        fname = feat.get("feature_name")
        fval = feat.get("feature_value")
        comp_at = feat.get("computed_at")
        if ent_id and fname and comp_at:
            stream_features_dict[(ent_id, fname, comp_at)] = str(fval)

    # 6. Compare features
    print("\n" + "="*95)
    print(f"{'Entity ID':<12} | {'Feature Name':<16} | {'Window End Time':<20} | {'Batch Val':<12} | {'Stream Val':<12} | {'Match?':<6}")
    print("="*95)
    
    matches_count = 0
    failures_count = 0
    
    # We only compare keys that exist in the Flink stream output (representing completed windows)
    for key, stream_val in stream_features_dict.items():
        ent_id, fname, comp_at = key
        batch_val = batch_features.get(key)
        
        if batch_val is not None:
            try:
                # convert to float for comparison if numeric
                val_match = abs(float(batch_val) - float(stream_val)) < 1e-5
            except ValueError:
                val_match = batch_val == stream_val
                
            if val_match:
                match_str = "YES"
                matches_count += 1
            else:
                match_str = "NO"
                failures_count += 1
        else:
            match_str = "PENDING"
            batch_val = "N/A"
            
        print(f"{ent_id:<12} | {fname:<16} | {comp_at:<20} | {batch_val[:12]:<12} | {stream_val[:12]:<12} | {match_str:<6}")
        
    print("="*95)
    print(f"Summary: {matches_count} matches, {failures_count} mismatches.")

if __name__ == "__main__":
    main()
