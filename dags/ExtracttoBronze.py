
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from utils import AdditionalFunctions

import json
import pandas as pd

api_endpoint = Variable.get("endpoint_api")


def processing_api_json(ti):

    # Pulls the API response using the task instance (ti) from Airflow
    resp_api = ti.xcom_pull(task_ids=['get_api'])
    pd.set_option('display.max_columns', None)

    # Checks if the API response is empty
    if not len(resp_api):
        raise ValueError('API is empty')
    print(resp_api)

    format_dt = datetime.now().strftime('%Y%m%d')

    # Sets the filename using the formatted date
    filename = "breweries" + str(format_dt) + ".json"

    # Define the path where the file will be saved
    path = "datalake/bronze/"
    print("Persisting file in Path: " + path)
    file_path = path + filename

    # Checks if the directory exists and creates it if necessary
    print("VERIFICANDO DIRETORIO")
    AdditionalFunctions.ensure_directory_exists(file_path)

    df = pd.DataFrame(resp_api)
    print(f"DF: {df}")

    # Saves the DataFrame as a JSON file to the specified path
    df.to_json(file_path, index=False)

    # Sets an Airflow variable with the path of the saved file
    Variable.set("path_bronze", file_path)

    return file_path


with DAG(
    'extractAPItoBronze',
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime.now()
) as dag:

    # Sensor task to check if the API is available
    api_available = HttpSensor(
        task_id='api_available',
        http_conn_id='base_api',
        endpoint=api_endpoint
    )
    # Operator task to get data from the API
    get_api = SimpleHttpOperator(
        task_id='get_api',
        http_conn_id='base_api',  # Connection ID defined in Airflow connections
        endpoint=api_endpoint,  # API endpoint to get data from
        method='GET',
        response_filter=lambda response: json.loads(
            response.text),  # Process the response to JSON
        log_response=True  # Log the response for debugging
    )
    processing_json = PythonOperator(
        task_id='processing_json',
        python_callable=processing_api_json
    )

    triggerDagToSilver = TriggerDagRunOperator(
        task_id='triggerDagToSilver',
        trigger_dag_id='TransformToSilver'
    )

    api_available >> get_api >> processing_json >> triggerDagToSilver
