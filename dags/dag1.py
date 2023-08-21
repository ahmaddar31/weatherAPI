import sys
import os

# Get the path to the parent directory of the current file (dags directory)
current_dir = os.path.dirname(os.path.abspath(__file__))

# Add the parent directory to the Python path
sys.path.append(current_dir)



from datetime import datetime
from modules import fetch
from modules import ingest
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow'
} 

dag = DAG(
    'weather_data_dag',
    default_args= default_args,
    schedule='0 0 * * *',  # Run once daily at midnight
    start_date= datetime(2023, 8, 6),
)



# task1 to fetch the data
fetch_weather_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch.fetch_data,
    dag=dag,
)

#task2 to to load the data
load_data_task = PythonOperator(
    task_id='load_data_to_mssql',
    python_callable=ingest.load_data,
    dag=dag,
)

dbt_run_task = BashOperator(
    task_id="dbt_run_task",
    bash_command='cd /home/ubuntu/weather/dbtWeatherTrans && source /home/ubuntu/airflow/.airflowenv/bin/activate && dbt run',
    dag=dag,
)


#the task of fetching data will run before the task of loading into database
fetch_weather_task >> load_data_task >> dbt_run_task 