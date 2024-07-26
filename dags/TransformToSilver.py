from airflow import DAG
from datetime import datetime
from utils import AdditionalFunctions
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable

import json
import pandas as pd
from pandas import json_normalize
import os


def json_to_dataframe(ti):
    # Retrieve the file path from the Airflow variable
    file_path = Variable.get("path_bronze")

    # Print the received path for debugging
    print(f"Received Path: {file_path}")

    # Check if the file path exists
    if file_path and os.path.exists(file_path):
        # Open and load the JSON data from the file
        with open(file_path, 'r') as f:
            data = json.load(f)

        # Print the JSON data for debugging
        print(f"JSON Data: {data}")

        # Extract the list of dictionaries from the JSON data
        lista_dicionarios = [v['0'] for k, v in data.items()]
        print(f"Dictionary List: {lista_dicionarios}")

        # Convert the list of dictionaries into a pandas DataFrame
        df_list = pd.DataFrame(lista_dicionarios)
        print(f"DataFrame List: {df_list}")

        # Convert the DataFrame columns to the appropriate data types
        df = df_list.astype({
            'id': 'string',
            'name': 'string',
            'brewery_type': 'string',
            'address_1': 'string',
            'address_2': 'string',
            'address_3': 'string',
            'city': 'string',
            'state_province': 'string',
            'postal_code': 'string',
            'country': 'string',
            'longitude': 'string',
            'latitude': 'string',
            'phone': 'string',
            'website_url': 'string',
            'state': 'string',
            'street': 'string'
        })

        # Process the DataFrame date
        df = AdditionalFunctions.process_date(df)

        # Clean the DataFrame columns
        df_final = AdditionalFunctions.clean_columns(df)

        # Print the final DataFrame for debugging
        print("\nFinal DataFrame:")
        print(df_final)

        # Define the path and filename for the parquet file
        path = "datalake/silver/"
        file_name = "breweries.parquet"

        # Combine the path and filename
        file_path = path + file_name

        # Ensure the directory exists
        AdditionalFunctions.ensure_directory_exists(file_path)

        # Save the DataFrame as a parquet file with partitions and compression
        df.to_parquet(
            file_path, partition_cols=['process_date', 'country', 'state', 'city'], compression='gzip'
        )

        # Set the Airflow variable with the path of the parquet file
        Variable.set("path_silver", file_path)

        # Return the path of the parquet file
        return file_path

    else:
        # Raise an error if the file is not found
        raise FileNotFoundError(f"Bronze File Not Found: {file_path}")


with DAG(
    'TransformToSilver',
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime.now()
) as dag:

    transformJson = PythonOperator(
        task_id='transformJson',
        python_callable=json_to_dataframe
    )

    triggerDagToGold = TriggerDagRunOperator(
        task_id='triggerDagToGold',
        trigger_dag_id='AggBreweriesToGold'
    )

transformJson >> triggerDagToGold
