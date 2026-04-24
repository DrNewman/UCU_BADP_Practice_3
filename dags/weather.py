from airflow import DAG
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import pendulum
import json
import logging

CITIES = {
    "Lviv": {"lat": "49.83", "lon": "24.02"},
    "Kyiv": {"lat": "50.45", "lon": "30.52"},
    "Kharkiv": {"lat": "49.99", "lon": "36.23"},
    "Odesa": {"lat": "46.48", "lon": "30.72"},
    "Zhmerynka": {"lat": "49.03", "lon": "28.10"},
}

def _process_weather(ti, city_name):
    info = ti.xcom_pull(task_ids=f"{city_name}.extract_data")
    weather_data = info.get("data", [info.get("current", {})])[0]
    return {
        "city": city_name,
        "timestamp": weather_data.get("dt"),
        "temp": weather_data.get("temp"),
        "humidity": weather_data.get("humidity"),
        "clouds": weather_data.get("clouds"),
        "wind_speed": weather_data.get("wind_speed")
    }

def _check_wind_speed(ti, city_name, **context):
    processed_data = ti.xcom_pull(task_ids=f"{city_name}.process_data")
    wind_speed = processed_data["wind_speed"]

    threshold = context['params']['wind_threshold']
    if wind_speed > threshold:
        return f"{city_name}.alert_load"
    return f"{city_name}.normal_load"

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': pendulum.duration(minutes=1),
}

with DAG(
        dag_id="weather_pipeline_v3",
        default_args=default_args,
        schedule="@daily",
        start_date=pendulum.datetime(2026, 4, 24, tz="UTC"),
        catchup=True,
        max_active_runs=1,
        params={
            "wind_threshold": Param(5.0, type="number", description="Wind speed threshold for alert"),
            "units": Param("metric", type="string", enum=["metric", "imperial"])
        },
        render_template_as_native_obj=True # Дозволяє передавати типи (int/float) через Jinja
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
               (city VARCHAR(50), timestamp TIMESTAMP, temp FLOAT,
                humidity INTEGER, cloudiness INTEGER, wind_speed FLOAT);""",
    )

    previous_group_end = db_create

    for city, coords in CITIES.items():
        with TaskGroup(group_id=city) as city_group:

            extract = HttpOperator(
                task_id="extract_data",
                http_conn_id="weather_import",
                endpoint="data/3.0/onecall/timemachine",
                data={
                    "appid": Variable.get("WEATHER_API_KEY"),
                    "lat": coords["lat"],
                    "lon": coords["lon"],
                    "dt": "{{ dag_run.logical_date.timestamp() | int }}",
                    "units": "{{ params.units }}"
                },
                method="GET",
                response_filter=lambda x: json.loads(x.text),
            )

            process = PythonOperator(
                task_id="process_data",
                python_callable=_process_weather,
                op_kwargs={"city_name": city}
            )

            branch = BranchPythonOperator(
                task_id="branch_on_wind",
                python_callable=_check_wind_speed,
                op_kwargs={"city_name": city}
            )

            alert_load = PythonOperator(
                task_id="alert_load",
                python_callable=lambda city_name, threshold: logging.info(f"!!! ALERT IN {city_name}: Wind > {threshold} !!!"),
                op_kwargs={"city_name": city, "threshold": "{{ params.wind_threshold }}"}
            )

            normal_load = SQLExecuteQueryOperator(
                task_id="normal_load",
                conn_id="weather_conn",
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                sql=f"""
                INSERT INTO measures (city, timestamp, temp, humidity, cloudiness, wind_speed)
                VALUES ('{{{{ ti.xcom_pull(task_ids='{city}.process_data')['city'] }}}}',
                        {{{{ ti.xcom_pull(task_ids='{city}.process_data')['timestamp'] }}}},
                        {{{{ ti.xcom_pull(task_ids='{city}.process_data')['temp'] }}}},
                        {{{{ ti.xcom_pull(task_ids='{city}.process_data')['humidity'] }}}},
                        {{{{ ti.xcom_pull(task_ids='{city}.process_data')['clouds'] }}}},
                        {{{{ ti.xcom_pull(task_ids='{city}.process_data')['wind_speed'] }}}});
                """
            )

            extract >> process >> branch
            branch >> [normal_load, alert_load]
            alert_load >> normal_load

        previous_group_end >> extract
        previous_group_end = normal_load