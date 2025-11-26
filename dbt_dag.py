from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime

# Define paths
DBT_PROJECT_DIR = "/Users/diren/Documents/DWH/test"
DBT_EXECUTABLE = "/Users/diren/Documents/DWH/venv/bin/dbt"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime.datetime(2023, 1, 1),
}

with DAG(
    dag_id='dbt_pipeline',
    default_args=default_args,
    description='A DAG to run dbt models',
    schedule='@daily',
    catchup=False,
) as dbt_dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'{DBT_EXECUTABLE} run',
        cwd=DBT_PROJECT_DIR,
    )
