from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

with DAG(
    'clickstream_real_time_pipeline',
    default_args=default_args,
    description='E-commerce Clickstream Real-time Processing Pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['clickstream', 'streaming', 'kafka', 'spark'],
) as dag:

    verify_raw = BashOperator(
        task_id='verify_raw_events_stream',
        bash_command='docker exec spark-master bash -c "du -sh /tmp/clickstream_raw && echo OK"',
    )

    verify_agg = BashOperator(
        task_id='verify_aggregated_metrics',
        bash_command='docker exec spark-master bash -c "du -sh /tmp/clickstream_agg && echo OK"',
    )

    verify_alerts = BashOperator(
        task_id='verify_alerts_stream',
        bash_command='docker exec spark-master bash -c "du -sh /tmp/clickstream_alerts && echo OK"',
    )

    status = BashOperator(
        task_id='pipeline_status',
        bash_command='echo "Clickstream Pipeline is running successfully" && date',
    )

    verify_raw >> verify_agg >> verify_alerts >> status
