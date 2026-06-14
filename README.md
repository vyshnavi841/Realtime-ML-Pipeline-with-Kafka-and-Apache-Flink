# Real-Time ML Feature Engineering Pipeline with Kafka and Apache Flink

This project implements a production-grade, real-time ML feature engineering pipeline using Apache Kafka, Apache Flink, and Streamlit for live observability. 

## System Architecture
- **Data Producer (Python)**: Simulates user interactions in accelerated event-time and writes them to the `user-events` Kafka topic. It also initializes Kafka topics and publishes metadata updates to `content-metadata`.
- **Apache Flink Job (PyFlink)**: Computes streaming features over tumbling and sliding event-time windows, performs a stream-table temporal join, and writes computed features to the compacted `feature-store` topic.
- **Observability Dashboard (Streamlit)**: Consumes computed features in real-time and queries Flink REST API to track watermark lag, late event drops, and feature freshness.
- **Batch Comparison Script (Pandas)**: Runs standard batch aggregations on historical Kafka logs to compare Flink's streaming feature values against a batch baseline to evaluate training-serving skew.

---

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.9+ (optional, for running the host-level comparison script)

### Configuration
Environment variables are managed in `.env.example`. Copy it to `.env` if custom configurations are needed:
```bash
cp .env.example .env
```

### Quick Start
To build and start the entire pipeline (Zookeeper, Kafka, Flink cluster, Producer, and Dashboard), run:
```bash
docker-compose up --build -d
```

### Accessing the System
- **Observability Dashboard**: [http://localhost:8501](http://localhost:8501)
- **Flink JobManager Dashboard**: [http://localhost:8081](http://localhost:8081)

---

## Verification and Analysis

### Host-Level Batch Feature Comparison
To verify the streaming pipeline's correctness, run the host-level batch comparison script. It will read the exact events processed by the streaming job, calculate batch features using Pandas, and print a side-by-side comparison:
```bash
pip install kafka-python pandas
python batch_comparison.py
```

### Analysis Report
Review the written evaluation of batch vs. streaming windowing semantics, stuck watermark resolution, and watermark tolerance details in [ANALYSIS.md](ANALYSIS.md).
