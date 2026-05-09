# 🚀 E-Commerce Clickstream Real-Time Processing Pipeline

![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)
![License](https://img.shields.io/badge/License-MIT-blue)
![Maintained](https://img.shields.io/badge/Maintained-Yes-green)
![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Kafka](https://img.shields.io/badge/Streaming-Kafka-orange)
![Spark](https://img.shields.io/badge/Processing-Spark-red)
![Airflow](https://img.shields.io/badge/Orchestration-Airflow-blue)

---

## 📌 Overview

A **real-time, production-grade e-commerce clickstream data pipeline** built using modern Big Data technologies.

This system simulates user activity on an e-commerce platform and processes it in real time to generate insights such as:

- Customer behavior tracking
- Product performance analytics
- Conversion rate analysis
- Real-time anomaly detection

### ⚡ Current Status
- ✅ 2000+ events processed
- ✅ 100% pipeline success rate
- ✅ <2 seconds end-to-end latency
- ✅ Fully containerized & production-ready

---

## 🎯 Key Features

### 🔥 Real-Time Streaming
- Apache Kafka-based event ingestion
- 100 events per 10 seconds
- 200 simulated users
- 50-product catalog

### ⚙️ Stream Processing (3 Pipelines)
- **Raw Events Stream** → full clickstream storage (Parquet)
- **Aggregated Metrics Stream** → KPIs, CTR, conversion rates
- **Alerts Stream** → anomaly detection & monitoring

### 🎛 Workflow Orchestration
- Apache Airflow DAG automation
- Fully automated pipeline execution
- 100% task success rate
- Retry & failure handling

### 📊 Monitoring & Observability
- Airflow UI monitoring
- Spark UI performance tracking
- Kafka topic inspection
- Real-time pipeline health checks

---

## 🏗 System Architecture

```
                                +----------------------+
                                |    APACHE KAFKA     |
                                | Topic: clickstream  |
                                | Partitions: 3       |
                                +----------+-----------+
                                           |
                      +--------------------+--------------------+
                      |                                         |
                      v                                         v
            
            +----------------------+            +----------------------+
            |    SPARK STREAM 1    |            |    SPARK STREAM 2    |
            |      Raw Events      |            |     Aggregations     |
            +----------------------+            +----------------------+
                      |                                         |
                      +--------------------+--------------------+
                                           |
                                           v
            
                                +----------------------+
                                |    SPARK STREAM 3    |
                                |  Alerts & Anomalies  |
                                +----------+-----------+
                                           |
                                           v
            
                                +----------------------+
                                |    STORAGE LAYER     |
                                |    Parquet + CSV     |
                                +----------+-----------+
                                           |
                                           v
            
                                +----------------------+
                                |    APACHE AIRFLOW    |
                                |    DAG Monitoring    |
                                +----------------------+

```
# 🏗 DATA FLOW ARCHITECTURE

```
Event Generator
      ↓
Kafka (clickstream-events)
      ↓
Spark Streaming
   ├── Raw Events
   ├── Aggregations
   └── Alerts
      ↓
Airflow Monitoring
```

---

## 🧰 Technology Stack

| Layer | Technology |
|------|------------|
| Streaming | Apache Kafka |
| Processing | Apache Spark |
| Orchestration | Apache Airflow |
| Storage | Parquet |
| Containerization | Docker |
| Language | Python 3.9+ |
| Runtime | OpenJDK 11 |

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

## ⚙️ System Requirements

- CPU: 4+ cores
- RAM: 8+ GB
- Disk: 50+ GB
- Docker + Docker Compose

---

## 🚀 Installation

```bash
git clone https://github.com/yourusername/ecommerce-clickstream-pipeline.git
cd ecommerce-clickstream-pipeline
docker-compose up -d
```

---

## ▶️ Quick Start

### Start Producer
```bash
docker exec -it airflow-webserver bash
python /opt/airflow/producers/clickstream_producer.py
```

### Start Spark
```bash
docker exec -it spark-master bash
python /opt/airflow/spark/stream_processor.py
```

### Airflow UI
http://localhost:8082

Run:
clickstream_real_time_pipeline

---

## 📊 Performance Metrics

| Metric | Value |
|------|------|
| Events/10 sec | 100 |
| Latency | <2 sec |
| Success Rate | 100% |
| Data Size | 7.7 MB |

---

## 📈 Sample Output

Electronics → 4.39%  
Toys → 3.07%  
Sports → 2.79%  
Clothing → 2.35%  
Home & Garden → 1.90%

---

## 📡 Monitoring

- Airflow: http://localhost:8082  
- Spark: http://localhost:4040  

---

## 🔧 Troubleshooting

```bash
docker-compose restart
docker logs spark-master
docker restart kafka
```

---

## 🚀 Deployment

```bash
docker-compose up -d
kubectl apply -f k8s/
```

---

## 📌 Roadmap

- ML anomaly detection  
- Grafana dashboards  
- Data warehouse integration  
- Recommendation engine  

---

## 🤝 Contributing

Fork → Branch → Commit → PR

---

## 📄 License

MIT License

---

## 👨‍💻 Authors & Contributors

Dias B.R.S.T - EG/2020/3893

---

## ❤️ Built With

Kafka • Spark • Airflow • Docker • Python
