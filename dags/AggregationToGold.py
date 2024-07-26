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

scripts_dir = os.path.join(os.path.dirname(__file__), '..', 'dags')
sys.path.append(scripts_dir)


def agg_by_location(ti):
    # Retrieve the file path from the Airflow variable
    file_path = Variable.get("path_silver")

    # Print the received path for debugging
    print(f"Received Path: {file_path}")

    # Check if the file path exists
    if file_path and os.path.exists(file_path):
        # Get the current date
        dt = datetime.now()

        # Format the date as 'YYYYMMDD'
        format_dt = dt.strftime('%Y%m%d')

        # Read the parquet file for the current date
        df = pd.read_parquet(f'{file_path}/process_date={format_dt}/')

        # Aggregate data by country, state, and city, counting the number of breweries
        LocAgg_df = df.groupby(['country', 'state', 'city'])[
            'id'].count().reset_index(name='number_of_breweries')

        # Process the date in the aggregated DataFrame
        LocAgg_df = AdditionalFunctions.process_date(LocAgg_df)

        # Define the path and filename for the aggregated data
        path = "datalake/gold/byLocation"
        file_name = "view_breweries_by_location.parquet"

        # Combine the path and filename
        file_path = path + file_name

        # Ensure the directory exists
        AdditionalFunctions.ensure_directory_exists(path)

        # Save the aggregated DataFrame as a parquet file with partitioning
        LocAgg_df.to_parquet(path, partition_cols=[
                             'process_date'], compression='gzip')
        # Variable.set("path_gold", file_path)

    else:
        # Raise an error if the file is not found
        raise FileNotFoundError(f"Silver File Not Found: {file_path}")


def agg_by_type(ti):
    # Retrieve the file path from the Airflow variable
    file_path = Variable.get("path_silver")

    # Print the received path for debugging
    print(f"Received Path: {file_path}")

    # Check if the file path exists
    if file_path and os.path.exists(file_path):
        # Get the current date
        dt = datetime.now()

        # Format the date as 'YYYYMMDD'
        format_dt = dt.strftime('%Y%m%d')

        # Read the parquet file for the current date
        df = pd.read_parquet(f'{file_path}/process_date={format_dt}/')

        # Aggregate data by brewery type, counting the number of breweries
        TypeAgg_df = df.groupby(['brewery_type'])['id'].count(
        ).reset_index(name='number_of_breweries')

        # Process the date in the aggregated DataFrame
        TypeAgg_df = AdditionalFunctions.process_date(TypeAgg_df)

        # Define the path and filename for the aggregated data
        path = "datalake/gold/byType"
        file_name = "view_breweries_by_type.parquet"
        file_path = path + file_name

        # Ensure the directory exists
        AdditionalFunctions.ensure_directory_exists(file_path)

        # Save the aggregated DataFrame as a parquet file with partitioning
        TypeAgg_df.to_parquet(path, partition_cols=[
                              'process_date'], compression='gzip')

        # Set the Airflow variable with the path of the parquet file
        # Variable.set("path_gold", file_path)

    else:
        # Raise an error if the file is not found
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
    # Task to get the status of the DAG
    statusDag = PythonOperator(
        task_id='statusDag',
        # Function to call to get the status of the DAG
        python_callable=AdditionalFunctions.status_dags,
    )

    # Task to send an email with the status
    sendEmail = PythonOperator(
        task_id='sendEmail',
        python_callable=AdditionalFunctions.email_status
    )

[aggByLoc, aggByType] >> statusDag >> sendEmail
