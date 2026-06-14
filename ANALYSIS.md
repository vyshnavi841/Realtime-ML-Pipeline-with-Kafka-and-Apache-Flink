# Real-Time Feature Engineering System Analysis

## Batch vs. Streaming Divergence
When evaluating streaming window calculations against historical batch datasets, deviations occur due to:
1. **Window Boundaries:** Batch systems group records along clean archival breaks, whereas streaming components rely strictly on event-time watermarks.
2. **Late-Arriving Variations:** Backdated modifications are naturally included in standard batch runs, but they are dropped by streaming architectures if they arrive past the watermark boundary.
3. **Temporal Joins:** Flink matches data dynamically using historical system state (`FOR SYSTEM_TIME AS OF`), whereas batch joins use standard static states. This can cause discrepancies if metadata properties shift mid-flight.

### Downstream ML Implications
Using features affected by training-serving skew can cause model performance to degrade. This issue can be resolved by transitioning downstream tracking models to sliding frames or unified hybrid lambda layers.

## Late Event Handling
This architecture manages late-arriving anomalies using an exact **30-second** bounded out-of-orderness watermark strategy. 

When user events arrive with a delay greater than 30 seconds, Flink evaluates them as falling behind the current watermark and drops them. This state behavior can be verified by checking for dropped element logs inside the `flink-taskmanager` container or by monitoring the *Late Events Dropped Counter* on the dashboard.