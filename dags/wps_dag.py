# wps_dag
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def wps_call(**kwargs):
    # Your code to call OGC WPS goes here
    print('Executing WPS call...', kwargs)
    return True

default_args = {
    'owner': 'you',
    'start_date': datetime(2023, 9, 27),
    'retries': 1,
}

dag = DAG(
    dag_id='wps_dag',
    description='Execute WPS call',
    schedule_interval=None,
    default_args=default_args,
)

wps_task = PythonOperator(
    task_id='wps_task',
    python_callable=wps_call,
    dag=dag,
)
