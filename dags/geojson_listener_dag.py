from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from Sensors.FileSensor import FileSensorWithXCom
from Operators.TriggerDAGRun import CustomTriggerDagRunOperator
from datetime import datetime, timedelta

# The DAG definition
dag = DAG(
    'geojson_listener_dag',
    description="Listen for new .geojson files",
    schedule_interval=None,
    start_date=datetime(2023, 9, 1),
    catchup=False
)

sensor = FileSensorWithXCom(
    task_id='sense_geojson_file',
    filepath='/opt/airflow/data/*.geojson',
    poke_interval=3,  # How often to check for the file, in seconds
    timeout=604800,  # Poking for a week as an example
    mode='poke',
    dag=dag,
)

trigger_wkt_dag = CustomTriggerDagRunOperator(
    task_id="trigger_geojson_to_wkt",
    trigger_dag_id="geojson_to_wkt_dag",  # The ID of the DAG you wish to run
    dag=dag 
)

sensor >> trigger_wkt_dag
