import os
import time
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("table.exec.source.idle-timeout", "5000 ms")
    
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    
    # User Interactions Stream with a 30-second Bounded Out-Of-Orderness Watermark
    t_env.execute_sql(f"""
        CREATE TABLE user_events (
            user_id STRING,
            content_id STRING,
            event_type STRING,
            dwell_time_ms INT,
            `timestamp` STRING,
            row_time AS TO_TIMESTAMP(REPLACE(REPLACE(`timestamp`, 'T', ' '), 'Z', '')),
            proc_time AS PROCTIME(),
            WATERMARK FOR row_time AS row_time - INTERVAL '30' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'user-events',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'properties.group.id' = 'flink-feature-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # Compacted Changelog Content Metadata Table using upsert-kafka (No watermark to avoid stuck watermarks on idle metadata streams)
    t_env.execute_sql(f"""
        CREATE TABLE content_metadata (
            content_id STRING,
            category STRING,
            creator_id STRING,
            publish_timestamp STRING,
            PRIMARY KEY (content_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = 'content-metadata',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'key.format' = 'raw',
            'value.format' = 'json'
        )
    """)

    # Unified Feature Store Sink Topic
    t_env.execute_sql(f"""
        CREATE TABLE feature_store (
            kafka_key STRING,
            entity_id STRING,
            feature_name STRING,
            feature_value STRING,
            computed_at STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'feature-store',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'key.format' = 'raw',
            'key.fields' = 'kafka_key',
            'value.format' = 'json',
            'value.fields-include' = 'EXCEPT_KEY'
        )
    """)

    # Create a view for Enriched User Events by joining stream with metadata table using a regular join
    t_env.execute_sql("""
        CREATE VIEW enriched_events AS
        SELECT u.user_id, u.row_time, m.category
        FROM user_events u
        JOIN content_metadata m
        ON u.content_id = m.content_id
    """)

    statement_set = t_env.create_statement_set()

    # Feature Set 1: User 1-Hour Tumbling Windows (click_rate) using Group-By TUMBLE
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            CONCAT(user_id, ':', 'click_rate') AS kafka_key,
            user_id AS entity_id,
            'click_rate' AS feature_name,
            CAST(CAST(SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(1) AS STRING) AS feature_value,
            CONCAT(REPLACE(DATE_FORMAT(TUMBLE_ROWTIME(row_time, INTERVAL '1' HOUR), 'yyyy-MM-dd HH:mm:ss'), ' ', 'T'), 'Z') AS computed_at
        FROM user_events
        GROUP BY user_id, TUMBLE(row_time, INTERVAL '1' HOUR)
    """)

    # Feature Set 1: User 1-Hour Tumbling Windows (avg_dwell_time) using Group-By TUMBLE
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            CONCAT(user_id, ':', 'avg_dwell_time') AS kafka_key,
            user_id AS entity_id,
            'avg_dwell_time' AS feature_name,
            CAST(AVG(dwell_time_ms) AS STRING) AS feature_value,
            CONCAT(REPLACE(DATE_FORMAT(TUMBLE_ROWTIME(row_time, INTERVAL '1' HOUR), 'yyyy-MM-dd HH:mm:ss'), ' ', 'T'), 'Z') AS computed_at
        FROM user_events
        GROUP BY user_id, TUMBLE(row_time, INTERVAL '1' HOUR)
    """)

    # Feature Set 2: Content 15-Minute Sliding Window (Sliding every 5 minutes) (engagement_rate) using Group-By HOP
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            CONCAT(content_id, ':', 'engagement_rate') AS kafka_key,
            content_id AS entity_id,
            'engagement_rate' AS feature_name,
            CAST(
                CASE WHEN SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) = 0 THEN 0.0
                ELSE CAST(SUM(CASE WHEN event_type IN ('like', 'share') THEN 1 ELSE 0 END) AS DOUBLE) / SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END)
                END AS STRING
            ) AS feature_value,
            CONCAT(REPLACE(DATE_FORMAT(HOP_ROWTIME(row_time, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE), 'yyyy-MM-dd HH:mm:ss'), ' ', 'T'), 'Z') AS computed_at
        FROM user_events
        GROUP BY content_id, HOP(row_time, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)
    """)

    # Feature Set 3: Category Affinity via Temporal Stream-Table Join & 1-Hour Tumbling Window using Group-By TUMBLE
    statement_set.add_insert_sql("""
        INSERT INTO feature_store
        SELECT 
            CONCAT(user_id, ':', 'affinity_', category) AS kafka_key,
            user_id AS entity_id,
            CONCAT('affinity_', category) AS feature_name,
            CAST(COUNT(1) AS STRING) AS feature_value,
            CONCAT(REPLACE(DATE_FORMAT(TUMBLE_ROWTIME(row_time, INTERVAL '1' HOUR), 'yyyy-MM-dd HH:mm:ss'), ' ', 'T'), 'Z') AS computed_at
        FROM enriched_events
        GROUP BY user_id, category, TUMBLE(row_time, INTERVAL '1' HOUR)
    """)

    print("Submitting feature pipeline statements to Flink...")
    table_result = statement_set.execute()
    job_client = table_result.get_job_client()
    if job_client is not None:
        print(f"Pipeline job started. Job ID: {job_client.get_job_id()}")
        # Block until the job finishes or fails, keeping the container alive
        while True:
            try:
                status = job_client.get_job_status().result()
                status_str = str(status)
                if "RUNNING" not in status_str and "INITIALIZING" not in status_str:
                    print(f"Job status changed to {status}. Exiting...")
                    break
            except Exception as e:
                print(f"Error querying job status: {e}")
            time.sleep(5)
    else:
        print("Pipeline job started, but JobClient was not returned.")

if __name__ == '__main__':
    main()