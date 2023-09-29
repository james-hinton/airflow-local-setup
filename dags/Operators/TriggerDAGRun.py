from airflow.operators.trigger_dagrun import TriggerDagRunOperator as OriginalTriggerDagRunOperator

class CustomTriggerDagRunOperator(OriginalTriggerDagRunOperator):
    def pre_execute(self, context):
        ti = context['ti']
        detected_file_path = ti.xcom_pull(task_ids='sense_geojson_file', key='detected_file_path')
        self.conf = {"file_path": detected_file_path}
