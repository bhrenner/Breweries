from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable
from utils import AdditionalFunctions
import pandas as pd
import sys
import os


def agg_by_location(ti):
    # file_path = ti.xcom_pull(task_id='transformJson')
    file_path = Variable.get("path_silver")

    print(f"Path recebido: {file_path}")

    if file_path and os.path.exists(file_path):
        # Get actual date
        dt = datetime.now()

        # Format date
        format_dt = dt.strftime('%Y%m%d')
        df = pd.read_parquet(f'{file_path}/process_date={format_dt}/')

        LocAgg_df = df.groupby(['country', 'state', 'city'])[
            'id'].count().reset_index(name='number_of_breweries')

        LocAgg_df = AdditionalFunctions.process_date(LocAgg_df)

        path = "datalake/gold/byLocation/"
        file_name = "view_breweries_by_location.parquet"

        file_path = path + file_name

        AdditionalFunctions.ensure_directory_exists(path)

        LocAgg_df.to_parquet(
            file_path, partition_cols=['process_date'])

    else:
        raise FileNotFoundError(f"Silver File Not Found: {file_path}")


def agg_by_type(ti):
    # file_path = ti.xcom_pull(task_id='transformJson')

    file_path = Variable.get("path_silver")

    print(f"Path recebido: {file_path}")
    if file_path and os.path.exists(file_path):
        df = pd.read_parquet(file_path)

        TypeAgg_df = df.groupby(['brewery_type'])['id'].count(
        ).reset_index(name='number_of_breweries')

        TypeAgg_df = AdditionalFunctions.process_date(TypeAgg_df)

        path = "datalake/gold/byType/"
        file_name = "view_breweries_by_type.parquet"
        file_path = path + file_name

        AdditionalFunctions.ensure_directory_exists(path)

        TypeAgg_df.to_parquet(
            file_path, partition_cols=['process_date'])

        Variable.set("path_gold", file_path)

    else:
        raise FileNotFoundError(f"Silver File Not Found: {file_path}")


with DAG(
    'AggBreweriesToGold',
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime.now()
) as dag:

    aggByLoc = PythonOperator(
        task_id='aggByLoc',
        python_callable=agg_by_location
    )

    aggByType = PythonOperator(
        task_id='aggByType',
        python_callable=agg_by_type
    )

    statusDag = PythonOperator(
        task_id='statusDag',
        python_callable=AdditionalFunctions.status_dags,
    )

    sendEmail = PythonOperator(
        task_id='sendEmail',
        python_callable=AdditionalFunctions.email_status
    )

[aggByLoc, aggByType] >> statusDag >> sendEmail
