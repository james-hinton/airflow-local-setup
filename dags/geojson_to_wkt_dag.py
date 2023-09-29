from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "you",
    "start_date": datetime(2023, 9, 25),
    "retries": 1,
}

dag = DAG(
    dag_id="geojson_to_wkt_dag",
    description="Transform GEOJSON to WKT",
    schedule_interval=None,  # Manual trigger
    default_args=default_args,
)


def process_file(**kwargs):
    # Retrieve the file path passed from the triggered DAG
    filename = kwargs["dag_run"].conf["file_path"]
    # Now, you can process the file with the path 'filename'
    print("Got file with path: {}".format(filename))


process_task = PythonOperator(
    task_id="process_file_task",
    python_callable=process_file,
    provide_context=True,
    dag=dag,
)
