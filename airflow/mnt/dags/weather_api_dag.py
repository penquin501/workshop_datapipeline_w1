from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.models import Variable #เรียกใช้ variable ใน airflow
import json
# import os

def _get_weather_data(**context):
    import requests
    # API_KEY = os.environ.get("WEATHER_API_KEY") #env
    API_KEY = Variable.get("WEATHER_API_KEY") #airflow

    # name = Variable.get("name")
    # print("hello, {name}")

    # print(context)
    # print(context['execution_date'])

    payload = { 
        "q": "bangkok",
        "appid": API_KEY,
        "units": "metric"
    }
    url = "https://api.openweathermap.org/data/2.5/weather"
    response = requests.get(url, params=payload)
    print(response.url)

    data = response.json()
    print(data)

    timestamp = context['execution_date']

    with open(f"/opt/airflow/dags/weather_data_{timestamp}.json", "w") as f:
        json.dump(data, f)

    return f"/opt/airflow/dags/weather_data_{timestamp}.json"

# Load to postgres
def _load_data_to_postgres(**context):
    ti = context['ti']
    file_name = ti.xcom_pull(task_ids="get_weather_data", key="return_value")
    print(file_name)

with DAG(
    "wheather_api_dag",
    schedule="@hourly",
    start_date=timezone.datetime(2024, 2, 3),
    catchup=False,
):
    start = EmptyOperator(task_id="start")
    
    get_weather_data = PythonOperator(
        task_id="get_weather_data",
        python_callable=_get_weather_data,
    )
    
    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
    )

    end = EmptyOperator(task_id="end")

    start >> end