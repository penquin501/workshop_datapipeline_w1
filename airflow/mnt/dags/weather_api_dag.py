from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.models import Variable #เรียกใช้ variable ใน airflow
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
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

def _create_weather_table(**context):
    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        CREATE TABLE IF NOT EXISTS weathers(
            temp FLOAT NOT NULL
        )
    """
    cursor.execute(sql)
    connection.commit()

# Load to postgres
def _load_data_to_postgres(**context):
    ti = context['ti']
    file_name = ti.xcom_pull(task_ids="get_weather_data", key="return_value")
    print(file_name)

    pg_hook = PostgresHook(
        postgres_conn_id="my_postgres_conn",
        schema="postgres"
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
        INSERT INTO weathers (temp) VALUES (31.39)
    """
    cursor.execute(sql)
    connection.commit()

default_args = {
    "email":["penquin501@gmail.com"],
    # "retries":1,
}

with DAG(
    "weather_api_dag",
    default_args=default_args,
    schedule="@hourly",
    start_date=timezone.datetime(2024, 2, 3),
    catchup=False,
):
    start = EmptyOperator(task_id="start")
    
    get_weather_data = PythonOperator(
        task_id="get_weather_data",
        python_callable=_get_weather_data,
    )

    create_weather_table = PythonOperator(
        task_id="create_weather_table",
        python_callable=_create_weather_table,
    )
    
    load_data_to_postgres = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=_load_data_to_postgres,
    )

    end = EmptyOperator(task_id="end")

    start >> get_weather_data >> create_weather_table >> load_data_to_postgres >> end