from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def print_hello(**kwargs):
    print('Arrived in the print_hello function with kwargs:', kwargs)

    message = kwargs.get('params', {}).get('message')

    if message:
        print(message)
    else:
        print("No message received or it's not in the expected format.")


default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 29),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(dag_id='print_dag', default_args=default_args, schedule_interval=None)

print_hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag,
)
