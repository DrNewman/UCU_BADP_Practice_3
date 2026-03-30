from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
import json

CITIES = {
    "Lviv": {"lat": "49.83", "lon": "24.02"},
    "Kyiv": {"lat": "50.45", "lon": "30.52"},
    "Kharkiv": {"lat": "49.99", "lon": "36.23"},
    "Odesa": {"lat": "46.48", "lon": "30.72"},
    "Zhmerynka": {"lat": "49.03", "lon": "28.10"},
}

def _process_weather(ti, city_name):
    info = ti.xcom_pull(task_ids=f"extract_data_{city_name}")
    weather_data = info.get("data", [info.get("current", {})])[0]
    return (
        city_name,
        weather_data.get("dt"),
        weather_data.get("temp"),
        weather_data.get("humidity"),
        weather_data.get("clouds"),
        weather_data.get("wind_speed")
    )

with DAG(
        dag_id="weather_pipeline",
        schedule="@daily",
        start_date=pendulum.datetime(2026, 3, 25, tz="UTC"), # Зробимо відступ на пару днів назад
        catchup=True,
        max_active_runs=1 # Щоб не перевантажити API одночасно
) as dag:

    # db_drop_table = SQLExecuteQueryOperator( # Можна використовувати, коли треба видалити попередню версію таблиці
    #     task_id="create_table_sqlite",
    #     conn_id="weather_conn",
    #     sql="""DROP TABLE IF EXISTS measures;""",
    # )

    db_create = SQLExecuteQueryOperator(
        task_id="create_table_sqlite",
        conn_id="weather_conn",
        sql="""CREATE TABLE IF NOT EXISTS measures
               (city VARCHAR(50),
                timestamp TIMESTAMP,
                temp FLOAT,
                humidity INTEGER,
                cloudiness INTEGER,
                wind_speed FLOAT);""",
    )

    previous_inject = db_create  # Точка старту для ланцюжка

    for city, coords in CITIES.items():
        extract_data = HttpOperator(
            task_id=f"extract_data_{city}",
            http_conn_id="weather_import",
            endpoint="data/3.0/onecall/timemachine",
            data={
                "appid": Variable.get("WEATHER_API_KEY"),
                "lat": coords["lat"],
                "lon": coords["lon"],
                "dt": "{{ dag_run.logical_date.timestamp() | int }}",
                "units": "metric"
            },
            method="GET",
            response_filter=lambda x: json.loads(x.text),
        )

        process_data = PythonOperator(
            task_id=f"process_data_{city}",
            python_callable=_process_weather,
            op_kwargs={"city_name": city}
        )

        inject_data = SQLExecuteQueryOperator(
            task_id=f"inject_data_{city}",
            conn_id="weather_conn",
            sql=f"""
            INSERT INTO measures (city, timestamp, temp, humidity, cloudiness, wind_speed)
            VALUES (
                '{{{{ ti.xcom_pull(task_ids='process_data_{city}')[0] }}}}',
                {{{{ ti.xcom_pull(task_ids='process_data_{city}')[1] }}}},
                {{{{ ti.xcom_pull(task_ids='process_data_{city}')[2] }}}},
                {{{{ ti.xcom_pull(task_ids='process_data_{city}')[3] }}}},
                {{{{ ti.xcom_pull(task_ids='process_data_{city}')[4] }}}},
                {{{{ ti.xcom_pull(task_ids='process_data_{city}')[5] }}}}
            );
            """,
        )

        db_create >> extract_data >> process_data >> inject_data

        previous_inject >> inject_data
        previous_inject = inject_data