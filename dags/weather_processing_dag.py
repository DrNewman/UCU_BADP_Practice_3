import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum

def _transform_from_file(**context):
    dag_run_conf = context['dag_run'].conf
    file_path = dag_run_conf.get('file_path')
    city = dag_run_conf.get('city')

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"Файл не знайдено: {file_path}")

    with open(file_path, 'r') as f:
        data = json.load(f)

    weather_list = data.get("data", [data.get("current", {})])
    weather_data = weather_list[0] if weather_list else {}

    # DATA QUALITY CHECK
    temp = weather_data.get("temp")
    if temp is not None and (temp < -60 or temp > 60):
        raise ValueError(f"Data Quality Check Failed: Temperature {temp} is out of realistic range!")

    return {
        "city": city,
        "timestamp": weather_data.get("dt"),
        "temp": weather_data.get("temp"),
        "humidity": weather_data.get("humidity"),
        "wind_speed": weather_data.get("wind_speed")
    }

with DAG(
        dag_id="weather_processing_dag",
        schedule=None,
        start_date=pendulum.datetime(2026, 4, 24, tz="UTC"),
        catchup=False
) as dag:

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=_transform_from_file,
    )

    load = SQLExecuteQueryOperator(
        task_id="load_to_db",
        conn_id="weather_conn",
        sql="""
            INSERT INTO measures (city, timestamp, temp, humidity, wind_speed)
            VALUES ('{{ ti.xcom_pull(task_ids='transform_data')['city'] }}',
                    {{ ti.xcom_pull(task_ids='transform_data')['timestamp'] }},
                    {{ ti.xcom_pull(task_ids='transform_data')['temp'] }},
                    {{ ti.xcom_pull(task_ids='transform_data')['humidity'] }},
                    {{ ti.xcom_pull(task_ids='transform_data')['wind_speed'] }});
        """,
    )

    transform >> load