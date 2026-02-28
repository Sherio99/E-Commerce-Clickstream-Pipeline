from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import glob

DATA_PATH = "/opt/airflow/data/processed"
RAW_PATH = "/opt/airflow/data/raw"
REPORT_PATH = "/opt/airflow/data/reports"
CATALOG_PATH = "/opt/airflow/data/product_catalog.csv"

def segment_users():
    # Read raw clickstream events
    raw_files = glob.glob(os.path.join(RAW_PATH, "**", "*.parquet"), recursive=True)
    if not raw_files:
        print("No raw clickstream data found.")
        return

    raw_df = pd.concat([pd.read_parquet(f) for f in raw_files])

    # User segmentation: Buyers vs Window Shoppers
    user_events = raw_df.groupby("user_id")["event_type"].apply(list).reset_index()

    def classify(events):
        if "purchase" in events:
            return "Buyer"
        return "Window Shopper"

    user_events["segment"] = user_events["event_type"].apply(classify)
    os.makedirs(REPORT_PATH, exist_ok=True)
    user_events[["user_id", "segment"]].to_csv(f"{REPORT_PATH}/user_segments.csv", index=False)

    # Summary text file
    summary = user_events["segment"].value_counts().to_dict()
    with open(f"{REPORT_PATH}/user_segments_summary.txt", "w") as f:
        f.write("Daily User Segmentation Summary\n")
        f.write("================================\n")
        for k, v in summary.items():
            f.write(f"{k}: {v}\n")

    # Read processed aggregation for conversion report
    agg_files = glob.glob(os.path.join(DATA_PATH, "**", "*.parquet"), recursive=True)
    if not agg_files:
        print("No processed data found.")
        return

    agg_df = pd.concat([pd.read_parquet(f) for f in agg_files])
    summary = agg_df.groupby("product_id")[["views", "purchases"]].sum().reset_index()
    summary["conversion_rate"] = summary["purchases"] / summary["views"]

    # Save per-product conversion
    summary.to_csv(f"{REPORT_PATH}/conversion_report.csv", index=False)

    # Category conversion report
    if os.path.exists(CATALOG_PATH):
        catalog = pd.read_csv(CATALOG_PATH)
        cat_df = summary.merge(catalog, on="product_id", how="left")
        category_summary = cat_df.groupby("category")[["views", "purchases"]].sum().reset_index()
        category_summary["conversion_rate"] = category_summary["purchases"] / category_summary["views"]
        category_summary.to_csv(f"{REPORT_PATH}/category_conversion_report.csv", index=False)
    else:
        print("Product catalog not found. Skipping category report.")

def top_products_summary():
    report_file = f"{REPORT_PATH}/conversion_report.csv"
    if not os.path.exists(report_file):
        print("Report not found.")
        return

    df = pd.read_csv(report_file)
    top5 = df.sort_values("views", ascending=False).head(5)

    with open(f"{REPORT_PATH}/top5_products.txt", "w") as f:
        f.write("Top 5 Most Viewed Products\n")
        f.write("===========================\n")
        for _, row in top5.iterrows():
            f.write(f"{row['product_id']} | Views: {row['views']} | Purchases: {row['purchases']}\n")

with DAG(
    dag_id="daily_user_segmentation",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    segment_task = PythonOperator(
        task_id="segment_users",
        python_callable=segment_users
    )

    top_products_task = PythonOperator(
        task_id="top_products_summary",
        python_callable=top_products_summary
    )

    segment_task >> top_products_task