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
    dag_id="bbox_to_wkt_dag",
    description="Transform BBOX to WKT",
    schedule_interval=None,  # Manual trigger
    default_args=default_args,
)


# Trigger print_dag 
trigger_print_start = TriggerDagRunOperator(
    task_id='trigger_print_start',
    trigger_dag_id="print_dag",
    conf={"message": "Starting BBOX to WKT transformation..."},
    dag=dag
)

# Trigger bbox_to_wkt_dag
trigger_wps = TriggerDagRunOperator(
    task_id='trigger_wps',
    trigger_dag_id="wps_dag",
    dag=dag
)

# Trigger print_dag again after the WPS process
trigger_print_end = TriggerDagRunOperator(
    task_id='trigger_print_end',
    trigger_dag_id="print_dag",
    conf={"message": "BBOX to WKT transformation complete!"},
    dag=dag
)

# Setting up the sequence
trigger_print_start >> trigger_wps >> trigger_print_end