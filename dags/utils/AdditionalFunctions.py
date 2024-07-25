import airflow
from airflow import DAG
from datetime import datetime
from airflow import settings
from airflow.models import DagRun
from airflow.utils.state import State

import pandas as pd
from pandas import json_normalize
import os

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# funçao para salvar nas partiçoes


# funçao para padronizar nome de colunas
def clean_columns(df):
    df.columns = (
        df.columns
        .str.lower()
        .str.replace(' ', '_')
        .str.strip()
    )
    return df

# função para criar diretorio


def ensure_directory_exists(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        print("Criando repositorio")
        os.makedirs(directory)


def process_date(df):
    format_dt = datetime.now().strftime('%Y%m%d')
    df['process_date'] = format_dt

    return df


def nomalize_json(df):
    if '0' in df.columns:
        # Extrair o conteúdo da coluna '0'
        contents = df['0'].apply(lambda x: x if isinstance(x, dict) else {})

        # Criar um novo DataFrame a partir da coluna '0'
        df_normalized = pd.json_normalize(contents)

        # Concatenar com outras colunas, se houver
        df_final = pd.concat([df.drop(columns=['0']), df_normalized], axis=1)

        print("DataFrame normalizado:")
        return df_final
    else:
        print("A coluna '0' não está presente no DataFrame.")
        df_final = df
        return df_final


def nomalize_json2(df):
    if isinstance(df, pd.DataFrame) and len(df.columns) == 1:
        df_normalized = pd.json_normalize(df.iloc[:, 0])
        print("\nDataFrame normalizado:")
        print(df_normalized)

    # Converter DataFrame normalizado para JSON
        json_str = df_normalized.to_json(orient='records', index=False)
        print("\nJSON resultante:")
        print(json_str)

        # Salvar DataFrame final como JSON
        # df_normalized.to_json(file_path, orient='records', index=False)

        print(f"\nJSON salvo em {file_path}")

        return df_normalized
    else:
        print("Estrutura do DataFrame não corresponde ao esperado.")


def status_dags():
    session = settings.Session()

    dags_list = ['extractAPItoBronze',
                 'TransformToSilver', 'AggregationToGold']

    fail_dags = []

    for dg in dags_list:
        last_run = session.query(DagRun).filter(DagRun.dag_id == dg).order_by(
            DagRun.execution_date.desc()).first()

        if last_run and last_run.state != State.SUCCESS:
            fail_dags.append(dg)
    if fail_dags:
        return f"Dags que não rodaram corretamente ou não foram concluídas com sucesso: {', '.join(fail_dags)}"
    else:
        return "O processo ocorreu corretamente, Dags concluidas com sucesso!"


def email_status(ti):

    subject = "Relatório de Status das DAGs"
    body = ti.xcom_pull(task_ids='statusDag')
    recipient = 'bhrenner_wilson@hotmail.com'

    # msg = MIMEText(body)
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = 'breweriescase@hotmail.com'
    msg['To'] = recipient

    msg.attach(MIMEText(body, 'plain'))
    try:
        with smtplib.SMTP('smtp-mail.outlook.com', 587) as server:
            server.ehlo()  # Identificar o servidor
            server.starttls()  # Iniciar TLS
            server.ehlo()  # Reidentificar o servidor após iniciar TLS
            server.login('breweriescase@hotmail.com', 'breCase#072024')
            server.sendmail(msg['From'], [msg['To']], msg.as_string())

            print("E-mail enviado com sucesso!")

    except Exception as e:
        print(f"Falha ao enviar o e-mail: {e}")
        airflow.utils.email.send_email(
            'breweriescase@hotmail.com', 'Airflow TEST HERE', 'This is airflow status success')
