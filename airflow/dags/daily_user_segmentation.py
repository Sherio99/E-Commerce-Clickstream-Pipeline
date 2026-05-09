from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import glob
import os

logger = logging.getLogger(__name__)

RAW_PATH = "/opt/airflow/data/raw/streaming"
REPORT_PATH = "/opt/airflow/data/reports"

def segment_users(**context):
    """
    Segment users into categories based on their event history.
    - Buyer: Users who made at least one purchase
    - Cart_Abandoner: Users who added to cart but never purchased
    - Window_Shopper: Users who viewed products but never purchased
    - Unknown: Users with no identifiable pattern
    """
    try:
        os.makedirs(REPORT_PATH, exist_ok=True)
        
        # Get execution date and calculate date range
        execution_date = context['execution_date']
        # Process data from the previous day (for daily jobs)
        date_str = execution_date.strftime("%Y-%m-%d")
        
        logger.info(f"Starting user segmentation for date: {date_str}")
        
        # Find parquet files for the specified date
        files = glob.glob(
            os.path.join(RAW_PATH, "**", "*.parquet"),
            recursive=True
        )
        
        if not files:
            logger.warning(f"No parquet files found for {date_str}.")
            return
        
        logger.info(f"Found {len(files)} parquet files")
        
        # Read and concatenate all parquet files
        df_list = []
        for file in files:
            try:
                df_list.append(pd.read_parquet(file))
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")
                continue
        
        if not df_list:
            logger.warning("No valid parquet files to process")
            return
        
        df = pd.concat(df_list, ignore_index=True)
        logger.info(f"Loaded {len(df)} total events")
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Filter for the current day only (to avoid reprocessing)
        today = pd.Timestamp.now().normalize()
        df_today = df[df['timestamp'] >= today]
        
        if df_today.empty:
            logger.warning("No events found for today")
            return
        
        logger.info(f"Processing {len(df_today)} events from today")
        
        # Group events by user
        user_events = df_today.groupby("user_id")["event_type"].apply(list).reset_index()
        
        def classify_user(events):
            """Classify user based on their event history."""
            purchase_count = events.count("purchase")
            cart_count = events.count("add_to_cart")
            view_count = events.count("view")
            
            if purchase_count >= 1:
                return "Buyer"
            elif cart_count >= 1 and purchase_count == 0:
                return "Cart_Abandoner"
            elif view_count > 0 and purchase_count == 0:
                return "Window_Shopper"
            else:
                return "Unknown"
        
        # Apply classification
        user_events["segment"] = user_events["event_type"].apply(classify_user)
        
        # Save user segments
        segment_output = f"{REPORT_PATH}/user_segments_{date_str}.csv"
        user_events[["user_id", "segment"]].to_csv(segment_output, index=False)
        logger.info(f"User segments saved to {segment_output}")
        
        # Generate summary statistics
        segment_summary = user_events["segment"].value_counts().to_dict()
        logger.info(f"Segment Summary: {segment_summary}")
        
    except Exception as e:
        logger.error(f"Error in segment_users: {e}", exc_info=True)
        raise


def generate_top_products_report(**context):
    """
    Generate a summary of top 5 most viewed products for the day.
    Includes product_id, category, and view count.
    """
    try:
        os.makedirs(REPORT_PATH, exist_ok=True)
        
        execution_date = context['execution_date']
        date_str = execution_date.strftime("%Y-%m-%d")
        
        logger.info(f"Generating top 5 products report for {date_str}")
        
        # Find parquet files
        files = glob.glob(
            os.path.join(RAW_PATH, "**", "*.parquet"),
            recursive=True
        )
        
        if not files:
            logger.warning("No parquet files found")
            return
        
        # Read and concatenate files
        df_list = []
        for file in files:
            try:
                df_list.append(pd.read_parquet(file))
            except Exception as e:
                logger.error(f"Error reading {file}: {e}")
                continue
        
        if not df_list:
            logger.warning("No valid parquet files to process")
            return
        
        df = pd.concat(df_list, ignore_index=True)
        
        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Filter for today's events
        today = pd.Timestamp.now().normalize()
        df_today = df[df['timestamp'] >= today]
        
        if df_today.empty:
            logger.warning("No events found for today")
            return
        
        # Filter for view events only
        df_views = df_today[df_today['event_type'] == 'view']
        
        if df_views.empty:
            logger.warning("No view events found for today")
            return
        
        # Get top 5 viewed products
        top_products = df_views.groupby(['product_id', 'category']).size() \
            .reset_index(name='views') \
            .nlargest(5, 'views') \
            .reset_index(drop=True)
        
        # Generate formatted report
        report_output = f"{REPORT_PATH}/top_5_products_{date_str}.txt"
        
        with open(report_output, "w") as f:
            f.write("=" * 60 + "\n")
            f.write(f"TOP 5 MOST VIEWED PRODUCTS - {date_str}\n")
            f.write("=" * 60 + "\n\n")
            
            if top_products.empty:
                f.write("No products viewed today.\n")
            else:
                for idx, row in top_products.iterrows():
                    f.write(f"{idx + 1}. Product ID: {row['product_id']}\n")
                    f.write(f"   Category: {row['category']}\n")
                    f.write(f"   Views: {row['views']}\n\n")
        
        logger.info(f"Top products report saved to {report_output}")
        logger.info(f"\nTop 5 Products:\n{top_products.to_string(index=True)}")
        
    except Exception as e:
        logger.error(f"Error in generate_top_products_report: {e}", exc_info=True)
        raise


# DAG Definition
default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id="daily_user_segmentation",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    description="Daily user segmentation and top products report"
) as dag:

    segment_task = PythonOperator(
        task_id="segment_users",
        python_callable=segment_users,
        provide_context=True,
        doc="Classify users into Buyers, Cart Abandoners, and Window Shoppers"
    )
    
    top_products_task = PythonOperator(
        task_id="generate_top_products_report",
        python_callable=generate_top_products_report,
        provide_context=True,
        doc="Generate top 5 most viewed products summary"
    )
    
    # Task dependencies
    segment_task >> top_products_task