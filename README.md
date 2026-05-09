# E-Commerce Clickstream Pipeline

An end-to-end Lambda architecture data pipeline for real-time clickstream processing and analytics in an online electronics store.

## Project Overview

This project implements a scalable, fault-tolerant system to:
- **Ingest** user clickstream events in real-time via Apache Kafka
- **Process** streams with Apache Spark for immediate insights and alerts
- **Orchestrate** batch analytics jobs with Apache Airflow
- **Store** data in distributed file systems for analysis and reporting

### Business Objectives

1. **Real-Time Alerts**: Detect high-interest products (>100 views, <5 purchases) and trigger flash sale recommendations
2. **User Segmentation**: Classify users as Buyers, Cart Abandoners, or Window Shoppers
3. **Conversion Analytics**: Calculate daily conversion rates by product category
4. **Daily Reporting**: Generate top 5 most viewed products summary

---

## Architecture

### Technology Stack Justification

| Component | Technology | Reason |
|-----------|-----------|--------|
| **Data Ingestion** | Apache Kafka | Scalable, fault-tolerant pub-sub system. Handles high-throughput event streams with partitioning for parallel processing. |
| **Stream Processing** | Apache Spark Structured Streaming | Native support for watermarking, windowing, and stateful operations. Integrates seamlessly with batch workloads. |
| **Orchestration** | Apache Airflow | DAG-based workflow management with task dependencies, retries, and monitoring. Perfect for daily batch jobs and reporting. |
| **Storage** | HDFS/Parquet + PostgreSQL | Parquet provides columnar compression for analytics. Partitioning enables efficient batch querying. |
| **Architecture** | Lambda (Batch + Speed) | Combines real-time stream processing with daily batch analytics for comprehensive insights. |

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SPEED LAYER (Real-Time)                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Kafka Topic ──> Spark Structured Streaming                         │
│  (clickstream)        │                                              │
│                       ├──> Raw Events Sink (Parquet)                │
│                       ├──> Aggregations (10-min windows)            │
│                       └──> Alert Trigger (>100 views, <5 purchases) │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     BATCH LAYER (Daily Jobs)                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Airflow Scheduler                                                   │
│  ├── Daily User Segmentation                                        │
│  │   ├── Read raw parquet events                                    │
│  │   ├── Classify users (Buyers, Cart Abandoners, Window Shoppers) │
│  │   └── Generate top 5 products report                            │
│  │                                                                   │
│  └── Daily Conversion Report                                        │
│      ├── Read streaming events                                      │
│      ├── Aggregate by category                                      │
│      └── Calculate conversion rates                                 │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
ecommerce-clickstream-pipeline/
├── airflow/
│   └── dags/
│       ├── daily_user_segmentation.py    # User classification + top products
│       └── daily_conversion_report.py    # Conversion rate analytics
│
├── spark/
│   ├── stream_processor.py               # Real-time stream processing
│   └── conversion_report.py              # Batch conversion analytics
│
├── producers/
│   └── clickstream_producer.py           # Kafka event generator
│
├── data/
│   ├── raw/                             # Raw streaming events
│   ├── processed/                       # Aggregated metrics
│   ├── alerts/                          # Real-time alerts
│   ├── reports/                         # Daily reports
│   ├── checkpoints/                     # Spark checkpoint metadata
│   └── product_catalog.csv              # Product reference data
│
├── docker-compose.yml                   # Service orchestration
├── requirements.txt                     # Python dependencies
└── README.md                            # This file
```

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Git
- 4GB+ available RAM

### Setup Instructions

1. **Clone the repository**
   ```bash
   git clone https://github.com/Sherio99/ecommerce-clickstream-pipeline.git
   cd ecommerce-clickstream-pipeline
   ```

2. **Prepare product catalog** (if not present)
   ```bash
   mkdir -p data
   # Ensure data/product_catalog.csv exists with columns: product_id, category
   ```

3. **Start all services**
   ```bash
   docker-compose up -d
   ```

4. **Verify services are running**
   ```bash
   docker-compose ps
   ```

   Expected output:
   ```
   zookeeper              running ✓
   kafka                  running ✓
   postgres               running ✓
   spark-master           running ✓
   spark-worker           running ✓
   airflow-webserver      running ✓
   airflow-scheduler      running ✓
   ```

5. **Access web interfaces**
   - **Airflow DAG Dashboard**: http://localhost:8082
     - Username: `admin`
     - Password: `admin`
   - **Spark Master UI**: http://localhost:8080
   - **PostgreSQL**: `localhost:5432` (User: `airflow`, Password: `airflow`)

---

## Running the Pipeline

### Step 1: Start the Kafka Producer

```bash
docker exec -it airflow-webserver bash
cd /opt/airflow
python -m pip install kafka-python pandas
python -u /opt/airflow/producers/clickstream_producer.py
```

The producer will start generating clickstream events and sending them to Kafka.

**Expected Output:**
```
2026-05-09 10:15:30,123 - INFO - ============================================================
2026-05-09 10:15:30,123 - INFO - Clickstream Event Producer Starting
2026-05-09 10:15:30,123 - INFO - ============================================================
2026-05-09 10:15:31,456 - INFO - Producing clickstream events...
{'user_id': 'U0045', 'product_id': 'P0001', 'category': 'Electronics', 'event_type': 'view', 'timestamp': '2026-05-09T10:15:31.789123+00:00'}
```

### Step 2: Start the Spark Stream Processor

In a new terminal:

```bash
docker exec -it spark-master bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/airflow/spark/stream_processor.py
```

**Expected Output:**
```
============================================================
All streams started successfully!
============================================================
Waiting for incoming data...
Press Ctrl+C to stop the processor
```

### Step 3: Trigger Airflow DAGs

1. Enable the DAGs in Airflow UI (http://localhost:8082)
2. Manually trigger or wait for scheduled run:
   - `daily_user_segmentation` runs daily at 00:00
   - `daily_conversion_report` runs daily at 01:00

---

## Data Flow Examples

### Real-Time Alert Example

When a product receives >100 views and <5 purchases within a 10-minute window:

**Alert Generated:**
```
============================================================
ALERT TRIGGERED (Batch ID: 0)
============================================================
+----------+------------+-------+---------+--------+
|product_id|    category| views|purchases|cart_add|
+----------+------------+-------+---------+--------+
|    P0001 |  Electronics|  150  |    3    |  25    |
+----------+------------+-------+---------+--------+
RECOMMENDATION: Launch a Flash Sale!
============================================================
```

### User Segmentation Example

**Input**: Raw clickstream events for a user

```json
[
  {"user_id": "U0025", "event_type": "view"},
  {"user_id": "U0025", "event_type": "view"},
  {"user_id": "U0025", "event_type": "add_to_cart"},
  {"user_id": "U0025", "event_type": "purchase"}
]
```

**Output**: `user_segments.csv`

```
user_id,segment
U0025,Buyer
```

### Conversion Rate Report

**Output**: `conversion_rate_report/`

```
category,views,purchases,cart_adds,conversion_rate,cart_to_purchase_rate
Electronics,4523,156,892,0.0345,0.1748
Fashion,3812,95,654,0.0249,0.1452
Books,2105,203,456,0.0964,0.4451
```

---

## Monitoring & Troubleshooting

### Common Issues

**1. Kafka Connection Error**
```
Failed to connect to Kafka broker
```
**Solution**: Ensure Kafka service is running
```bash
docker-compose restart kafka zookeeper
```

**2. Spark Job Fails to Start**
```
Failed to connect to master
```
**Solution**: Check Spark master is accessible
```bash
docker logs spark-master
```

**3. Airflow DAGs Not Visible**
```
No DAGs found
```
**Solution**: Restart Airflow services and verify DAG files
```bash
docker-compose restart airflow-webserver airflow-scheduler
docker-compose logs airflow-scheduler | grep -i dag
```

### Monitoring Queries

Check Kafka topics:
```bash
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

Monitor Spark streaming:
```bash
docker logs spark-master -f
```

View Airflow task logs:
```bash
docker logs airflow-scheduler -f
```

---

## Performance Optimization

### Tuning Parameters

**Spark Streaming:**
- Window size: 10 minutes (balance between latency and accuracy)
- Slide interval: 5 minutes (50% overlap for continuous insights)
- Watermark: 10 minutes (late data tolerance)

**Airflow:**
- Executor type: LocalExecutor (suitable for development)
- Parallelism: 2 (adjust based on CPU cores)
- DAG serialization: Default

**Kafka:**
- Partitions: 1 (increase for parallelism)
- Replication factor: 1 (increase for HA)

---

## Future Enhancements

1. **Real-Time Notifications**: Integrate with Slack/Email for alerts
2. **Dashboard**: Grafana/Kibana for visualization
3. **ML Pipeline**: Predictive pricing model based on demand
4. **Scalability**: Deploy to Kubernetes with dynamic scaling
5. **Data Warehouse**: Integrate with Snowflake/Redshift for BI
6. **Testing**: Add unit tests and data quality checks
7. **CI/CD**: GitHub Actions for automated testing and deployment

---

## Team & Contact

**Project**: Applied Big Data Engineering - Mini Project Assessment
**Date**: May 2026
**Author**: Sherio99

---

## References

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Lambda Architecture](https://lambda-architecture.net/)

---

## License

Educational project - MIT License