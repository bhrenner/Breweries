
from airflow import DAG
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from utils import AdditionalFunctions


import json
import pandas as pd
from pandas import json_normalize
import sys
import os

# https://api.openweathermap.org/data/2.5/
# weather?lat=-22.909938&lon=-47.062633&appid=55f6b58f59a9faf94fd93576f3f22273

# https://api.openbrewerydb.org
# /breweries
api_endpoint = Variable.get("endpoint_api")
# param_lon = Variable.get("param_lon")
# param_lat = Variable.get("param_lat")
# param_apid = Variable.get("param_apid")


def processing_api_json(ti):

    resp_api = ti.xcom_pull(task_ids=['get_api'])
    pd.set_option('display.max_columns', None)
    if not len(resp_api):
        raise ValueError('API is empty')
    # df = pd.DataFrame(resp_api)
    print(resp_api)
    # df = pd.json_normalize(resp_api, sep='_')

    format_dt = datetime.now().strftime('%Y%m%d')

    filename = "breweries" + str(format_dt) + ".json"
    path = "datalake/bronze/"

    print("Persisting file... \n Path: " + path)
    file_path = path + filename

    print("VERIFICANDO DIRETORIO")
    AdditionalFunctions.ensure_directory_exists(file_path)

    df = pd.DataFrame(resp_api)
    print(f"DF: {df}")
    # Persist df
    df.to_json(file_path, index=False)

    Variable.set("path_bronze", file_path)

    return file_path


with DAG(
    'extractAPItoBronze',
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime.now()
) as dag:

    # VALIDA API USANDO CONECCTIONS DO AIRFLOW
    api_available = HttpSensor(
        task_id='api_available',
        http_conn_id='base_api',
        endpoint=api_endpoint
    )
    get_api = SimpleHttpOperator(
        task_id='get_api',
        http_conn_id='base_api',
        endpoint=api_endpoint,
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
    processing_json = PythonOperator(
        task_id='processing_json',
        python_callable=processing_api_json
        # op_kwargs={
        #   'path': path,
        #   'filename': filename,
        #   'compression': compression}
    )

    triggerDagToSilver = TriggerDagRunOperator(
        task_id='triggerDagToSilver',
        trigger_dag_id='TransformToSilver'
    )

    api_available >> get_api >> processing_json >> triggerDagToSilver
