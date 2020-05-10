from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'edsm_bq_etl',
    default_args=default_args,
    description='Loads EDSM data into BigQuery',
    schedule_interval=timedelta(days=1),
)

edsm_to_s3 = DummyOperator(
    task_id='edsm_to_s3',
    dag=dag,
)

edsm_to_s3
