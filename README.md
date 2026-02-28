# E‑Commerce Clickstream & Inventory Watch

End‑to‑end clickstream pipeline for **real‑time analytics** and **daily batch reporting** using **Kafka**, **Spark Structured Streaming**, and **Airflow**.

---

## ✅ Features (Meets Scenario 3 Requirements)

### **Producer**
- Simulates clickstream events:
  ```
  { user_id, product_id, event_type, timestamp }
  ```
- Event types: `view`, `add_to_cart`, `purchase`

### **Real‑Time Stream Layer**
- Kafka ingestion  
- Spark streaming aggregation (sliding windows: **10 mins window / 5 mins slide**)  
- Alert rule:
  ```
  views > 100 AND purchases < 5
  ```
  → outputs “high interest, low conversion” alerts

### **Batch / Airflow Layer**
- Daily User Segmentation:
  - **Buyer** (user has a purchase)
  - **Window Shopper** (no purchase)
- Generates:
  - `user_segments.csv`
  - `user_segments_summary.txt`
  - `top5_products.txt`
- Conversion Reports:
  - per product
  - per **category**

---

## 📦 Project Structure

```
ecommerce-clickstream-pipeline/
├── airflow/
│   └── dags/
│       └── daily_user_segmentation.py
├── data/
│   ├── raw/                 # raw clickstream parquet from Spark
│   ├── processed/           # aggregated parquet from Spark
│   ├── alerts/              # JSON alerts
│   ├── reports/             # Airflow outputs
│   └── product_catalog.csv  # product_id → category mapping
├── producers/
│   └── clickstream_producer.py
├── spark/
│   └── stream_processor.py
└��─ docker-compose.yml
```

---

## ⚙️ Prerequisites
- Docker + Docker Compose
- Python 3.8+ (for running producer)

---

## 🚀 Quick Start (From Scratch)

### 1) Start infrastructure
```powershell
docker-compose up -d zookeeper kafka postgres spark-master spark-worker
```

### 2) Initialize Airflow DB
```powershell
docker-compose run --rm airflow-webserver airflow db init
```

### 3) Create Airflow admin user
```powershell
docker-compose run --rm airflow-webserver airflow users create `
  --username admin --firstname Admin --lastname User `
  --role Admin --email admin@example.com --password admin
```

### 4) Start Airflow
```powershell
docker-compose up -d airflow-webserver airflow-scheduler
```

### 5) Create Kafka topic
```powershell
docker exec -it kafka /usr/bin/kafka-topics `
  --create --topic clickstream-events `
  --bootstrap-server kafka:9092 `
  --partitions 3 --replication-factor 1
```

### 6) Start Producer
```powershell
python .\producers\clickstream_producer.py
```

### 7) Start Spark Stream
```powershell
docker cp .\spark\stream_processor.py spark-master:/tmp/stream_processor.py

docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf "spark.driver.extraJavaOptions=-Divy.cache.dir=/tmp/ivy -Divy.home=/tmp/ivy" `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 `
  /tmp/stream_processor.py
```

### 8) Run Airflow DAG
Open:
```
http://localhost:8082
```

Login:
- user: `admin`
- pass: `admin`

Then click **Trigger DAG** for `daily_user_segmentation`.

---

## 📊 Outputs

### Spark Streaming
- `data/raw/` → raw clickstream parquet
- `data/processed/` → per‑product aggregates
- `data/alerts/` → JSON alerts

### Airflow Reports
- `data/reports/user_segments.csv`
- `data/reports/user_segments_summary.txt`
- `data/reports/conversion_report.csv`
- `data/reports/category_conversion_report.csv`
- `data/reports/top5_products.txt`

---

## ✅ User Segmentation Logic
- **Buyer** → user has at least one `purchase`
- **Window Shopper** → only `view/add_to_cart`

---

## ✅ Category Conversion Logic
Based on `data/product_catalog.csv`:
```
conversion_rate = total_purchases / total_views
```

---

## 🛠 Troubleshooting

### 403 log error in Airflow
Ensure the same `AIRFLOW__WEBSERVER__SECRET_KEY` is set in **webserver + scheduler**.

### Spark crash: `delta file does not exist`
Your checkpoint was deleted or corrupted.
Fix by stopping Spark and deleting checkpoints:

```powershell
rmdir .\data\checkpoints -Recurse -Force
mkdir .\data\checkpoints
```

Then restart Spark stream.

### Spark crash: OffsetOutOfRange
Reset Kafka or checkpoints. Best clean reset:

```powershell
docker exec -it kafka /usr/bin/kafka-topics --delete --topic clickstream-events --bootstrap-server kafka:9092
docker exec -it kafka /usr/bin/kafka-topics --create --topic clickstream-events --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```

---

## ✅ Scenario Compliance Summary

| Requirement | Status |
|---|---|
| Producer emits correct event format | ✅ |
| Kafka ingestion | ✅ |
| Sliding window views per product | ✅ |
| High‑interest / low‑conversion alert | ✅ |
| Airflow daily user segmentation | ✅ |
| Window Shoppers vs Buyers | ✅ |
| Top 5 most viewed report | ✅ |
| Conversion rate per category | ✅ |

---

## 🧩 Notes
- Keep Spark streaming running while data is being produced.
- Never delete checkpoints while Spark is running.
