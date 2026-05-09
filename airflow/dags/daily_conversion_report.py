from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger(__name__)

REPORT_PATH = "/opt/airflow/data/reports"

def validate_report(**context):
    """Validate that the conversion report was generated successfully."""
    try:
        report_file = os.path.join(REPORT_PATH, "conversion_rate_report")
        if os.path.exists(report_file):
            logger.info(f"Conversion report validated at {report_file}")
            return True
        else:
            logger.warning(f"Conversion report not found at {report_file}")
            return False
    except Exception as e:
        logger.error(f"Error validating report: {e}")
        raise

# DAG Definition
default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

with DAG(
    dag_id='daily_conversion_report',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description='Daily conversion rate analytics by product category'
) as dag:

    generate_report = BashOperator(
        task_id='generate_conversion_report',
        bash_command='spark-submit /opt/airflow/spark/conversion_report.py',
        doc='Generate conversion rate report from streaming data'
    )
    
    validate_report_task = PythonOperator(
        task_id='validate_conversion_report',
        python_callable=validate_report,
        provide_context=True,
        doc='Validate that the conversion report was generated'
    )
    
    # Task dependencies
    generate_report >> validate_report_task