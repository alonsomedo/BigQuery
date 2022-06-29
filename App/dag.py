from airflow import models
from datetime import datetime, timedelta
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

default_args = {
    'owner' : 'Airflow',
    'start_date': datetime(2022, 6, 29),
    'retries': 1,
    'retry_delay': timedelta(seconds=50),
    'dataflow_default_options': {
        'project' : 'bigquery-demo-354214',
        'region' : 'us-east1',
        'runner': 'DAtaflowRunner'
    }
}


with models.DAG(
                'food_orders_dag',
                default_args=default_args,
                schedule_interval='@daily',
                catchup=False
            ) as dag:
    
    t1 = DataFlowPythonOperator(
            task_id='beamtask',
            py_file='gs://us-east1-food-orders-07f754b8-bucket/pipeline.py',
            options={
                'input' : 'gs://daily_food_orders_md/food_daily.csv' 
            } 
        )   

    