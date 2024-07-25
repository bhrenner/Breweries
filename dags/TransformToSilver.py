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
    # file_path = ti.xcom_pull(dag_id=['extractAPItoBronze'],task_ids=['processing_json'])

    file_path = Variable.get("path_bronze")

    print(f"Path recebido: {file_path}")

    if file_path and os.path.exists(file_path):

        with open(file_path, 'r') as f:
            data = json.load(f)

        print(f"Dados json: {data}")
        lista_dicionarios = [v['0']
                             for k, v in data.items()]  # list(data.values())
        print(f"Lista Dicionario: {lista_dicionarios}")

        df_list = pd.DataFrame(lista_dicionarios)
        print(f"Lista DF {df_list}")

        # df = json_df['0']

        """
                print("\nDataFrame normalizado:")
                print(df)
                print("Tipos de dados das colunas:")
                print(df.dtypes)
                print("\nColunas do DataFrame:")
                print(df.columns)

                # Explodir a coluna com listas de dicionários
                df_exploded = df.explode(0)
                print("\nExplodido:")
                print(df_exploded)
                print("Tipos de dados das colunas:")
                print(df_exploded.dtypes)

                # Normalizar a coluna com listas de dicionários
                df_normalized = pd.json_normalize(df_exploded[0])
                print("\nDataFrame normalizado:")
                print(df_normalized)

                # Adicionar outras colunas ao DataFrame normalizado, se necessário
                df_final = pd.concat(
                    [df_exploded.drop(columns=[0]), df_normalized], axis=1)
                print("\nDataFrame final:")
                print(df_final)

            # df_nomalized = json_normalize(data=df[0])

                df_row = df_exploded.iloc[:].to_dict()
                print("\nDataFrame ROW:")
                print(df_row)
        """

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

        df = AdditionalFunctions.process_date(df)
        df_final = AdditionalFunctions.clean_columns(df)

        print("\nDataFrame Final:")
        print(df_final)
        path = "datalake/silver/"
        file_name = "breweries.parquet"

        file_path = path + file_name

        AdditionalFunctions.ensure_directory_exists(path+file_name)

        df.to_parquet(
            file_path, partition_cols=['process_date', 'country', 'state', 'city'], compression='gzip')

        Variable.set("path_silver", file_path)
        return file_path

    else:
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
