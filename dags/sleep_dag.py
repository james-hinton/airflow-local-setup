from datetime import datetime
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'you',
    'start_date': datetime(2023, 9, 26),
    'retries': 1,
}

dag = DAG(
    dag_id='sleep_dag',
    description='A secondary test',
    schedule_interval=None,  # Manual trigger for simplicity
    default_args=default_args,
)

def sleep(**kwargs):
    time.sleep(5)
    return True
    


sleep_task = PythonOperator(
    task_id='sleep_task',
    python_callable=sleep,
    dag=dag,
)

trigger_second_dag = TriggerDagRunOperator(
    trigger_dag_id="print_dag",
    conf={"message": "Hello from first DAG!"},
    task_id='trigger_second_dag',
    dag=dag,  # Add this line to associate the task with the DAG.
)

sleep_task >> trigger_second_dag
