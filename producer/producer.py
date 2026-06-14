import os
import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

def init_topics():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id='init-client')
    topics = [
        NewTopic(name="user-events", num_partitions=3, replication_factor=1),
        NewTopic(name="content-metadata", num_partitions=1, replication_factor=1, 
                 topic_configs={"cleanup.policy": "compact"}),
        NewTopic(name="feature-store", num_partitions=1, replication_factor=1, 
                 topic_configs={"cleanup.policy": "compact"})
    ]
    for topic in topics:
        try:
            admin_client.create_topics(new_topics=[topic])
            print(f"Created topic: {topic.name}")
        except TopicAlreadyExistsError:
            print(f"Topic {topic.name} already exists.")

def main():
    init_topics()
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda v: v.encode('utf-8') if v else None,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Push initial metadata contracts
    contents = ["content_101", "content_102", "content_103"]
    categories = {"content_101": "sci-fi", "content_102": "news", "content_103": "comedy"}
    
    for cid in contents:
        meta = {
            "content_id": cid,
            "category": categories[cid],
            "creator_id": f"creator_{random.randint(1,5)}",
            "publish_timestamp": (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        producer.send("content-metadata", key=cid, value=meta)
    
    users = ["user_alpha", "user_beta", "user_gamma"]
    event_types = ["view", "click", "like", "share"]
    
    # Start the simulation clock 2 hours in the past
    sim_time = datetime.utcnow() - timedelta(hours=2)
    
    print("Starting interaction simulation loop with accelerated time...")
    while True:
        # Advance the simulation clock by 30 seconds per iteration
        sim_time += timedelta(seconds=30)
        
        # Inject 5% deliberate late anomalies (35-90 seconds delayed in event-time)
        if random.random() < 0.05:
            delay_sec = random.randint(35, 90)
            event_time = sim_time - timedelta(seconds=delay_sec)
        else:
            event_time = sim_time
            
        user = random.choice(users)
        content = random.choice(contents)
        etype = random.choice(event_types)
        
        payload = {
            "user_id": user,
            "content_id": content,
            "event_type": etype,
            "dwell_time_ms": random.randint(1000, 60000) if etype == "view" else 0,
            "timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        producer.send("user-events", value=payload)
        producer.flush()
        # Sleep 0.2 seconds to achieve 150x acceleration factor (1 hour of sim time = 24 seconds of wall-clock time)
        time.sleep(0.2)

if __name__ == "__main__":
    main()