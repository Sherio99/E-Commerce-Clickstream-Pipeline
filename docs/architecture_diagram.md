```mermaid
flowchart LR
    A[Clickstream Producer] -->|JSON Events| B[Kafka Topic: clickstream-events]
    B --> C[Spark Structured Streaming]
    C --> D[Parquet Storage (data/processed)]
    C --> E[Alerts (data/alerts)]
    D --> F[Airflow Daily DAG]
    F --> G[Reports (conversion_report.csv, top5_products.txt)]
```