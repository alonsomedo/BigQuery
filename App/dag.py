from airflow import models
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

default_args = {
    'owner' : 'Airflow',
    'start_date': datetime(2022, 6, 29),
    'retries': 1,
    'retry_delay': timedelta(seconds=50)

}