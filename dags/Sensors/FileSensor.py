from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
import os
import glob

class FileSensorWithXCom(FileSensor):
    def poke(self, context):
        hook = FSHook(self.fs_conn_id)
        basepath = hook.get_path()
        full_path_pattern = '/opt/airflow/data/*.geojson'
        self.log.info('Poking for files matching: %s', full_path_pattern)
        
        matching_files = glob.glob(full_path_pattern)
        
        if matching_files:  # Check if any files were found
            detected_file = matching_files[0]  # Using the first file if multiple are found
            self.log.info('Detected file: %s', detected_file)
            context['ti'].xcom_push(key='detected_file_path', value=detected_file)
            return True
        
        return False
