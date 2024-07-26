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


def clean_columns(df):
    df.columns = (
        df.columns
        .str.lower()         # Convert column names to lowercase
        .str.replace(' ', '_')  # Replace spaces with underscores
        .str.strip()         # Remove leading and trailing whitespace
    )
    return df

# Function to create directory if it doesn't exist


def ensure_directory_exists(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        print("Creating directory")
        os.makedirs(directory)

# Function to add a processing date column to a DataFrame


def process_date(df):
    # Get the current date in 'YYYYMMDD' format
    format_dt = datetime.now().strftime('%Y%m%d')
    # Add the processing date column
    df['process_date'] = format_dt
    return df


def status_dags():
    session = settings.Session()

    # List of DAGs to check
    dags_list = ['extractAPItoBronze',
                 'TransformToSilver', 'AggregationToGold']

    fail_dags = []

    for dg in dags_list:
        # Get the last run of the DAG
        last_run = session.query(DagRun).filter(DagRun.dag_id == dg).order_by(
            DagRun.execution_date.desc()).first()

        # Check if the last run was not successful
        if last_run and last_run.state != State.SUCCESS:
            fail_dags.append(dg)
    # Return a message based on the status of the DAGs
    if fail_dags:
        return f"Dags que não rodaram corretamente ou não foram concluídas com sucesso: {', '.join(fail_dags)}"
    else:
        return "O processo ocorreu corretamente, Dags concluidas com sucesso!"


def email_status(ti):

    subject = "Relatório de Status das DAGs"
    # Get the status message from the previous task
    body = ti.xcom_pull(task_ids='statusDag')
    recipient = 'breweriescase@hotmail.com'

    # msg = MIMEText(body)
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = 'breweriescase@hotmail.com'
    msg['To'] = recipient

    msg.attach(MIMEText(body, 'plain'))
    try:
        with smtplib.SMTP('smtp-mail.outlook.com', 587) as server:
            server.ehlo()  # Identify the server
            server.starttls()  # Start TLS
            server.ehlo()  # Re-identify the server after starting TLS
            server.login('breweriescase@hotmail.com', 'breCase#072024')
            server.sendmail(msg['From'], [msg['To']], msg.as_string())

            print("E-mail enviado com sucesso!")

    except Exception as e:
        print(f"Falha ao enviar o e-mail: {e}")
        # Send an email indicating the failure to send the report
        airflow.utils.email.send_email(
            'breweriescase@hotmail.com', 'Airflow TEST HERE', 'This is airflow status success')
