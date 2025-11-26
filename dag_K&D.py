from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import pandas as pd

SPREADSHEET_ID = "1JVqKCiPHg0HM7-5R4N9aHmROJD-YDomA5tCbBvRT2xw"  # Die ID aus der Google Sheets URL
DESTINATION_PATH = "/Users/diren/Documents/DWH/sleep_health_data.csv"
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=csv"
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

#https://docs.google.com/spreadsheets/d/1JVqKCiPHg0HM7-5R4N9aHmROJD-YDomA5tCbBvRT2xw/export?format=csv

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

def download_sheet_to_local(sheet_url, output_path):
    try:
        df = pd.read_csv(sheet_url)
        df.to_csv(output_path, index=False)
        print(f"Successfully saved data to {output_path}")
    except Exception as e:
        print(f"Error downloading sheet: {e}")
        raise

with DAG(
    dag_id = 'health_insurance_lakehouse_pipeline',
    default_args = default_args,
    description = 'ELT pipeline for health insurance data from GSheets to Local CSV',
    schedule = '@daily', # Runs the pipeline once per day
    catchup = False,
) as elt_dag:

    download_sheet_local = PythonOperator(
        task_id="download_sheet_local",
        python_callable=download_sheet_to_local,
        op_kwargs={
            "sheet_url": SHEET_URL,
            "output_path": DESTINATION_PATH,
        },
    )