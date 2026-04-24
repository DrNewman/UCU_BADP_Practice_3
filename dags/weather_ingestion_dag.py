import json
import os
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
import pendulum

CITIES = {
    "Lviv": {"lat": "49.83", "lon": "24.02"},
    "Kyiv": {"lat": "50.45", "lon": "30.52"},
    "Kharkiv": {"lat": "49.99", "lon": "36.23"},
    "Odesa": {"lat": "46.48", "lon": "30.72"},
    "Zhmerynka": {"lat": "49.03", "lon": "28.10"},
}

DATA_DIR = Variable.get("WEATHER_DATA_PATH", default_var="/opt/airflow/data")

def _save_raw_data(ti, city_name, **context):
    raw_data = ti.xcom_pull(task_ids='extract_data')

    # Data Quality Check
    if not raw_data or 'data' not in raw_data and 'current' not in raw_data:
        raise ValueError(f"Data Quality Check Failed: Raw data for {city_name} is empty or malformed")

    ds_nodash = context['ds_nodash']
    file_path = os.path.join(DATA_DIR, f"weather_{city_name}_{ds_nodash}.json")

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(file_path, 'w') as f:
        json.dump(raw_data, f)
    print(f"Successfully saved {city_name} data to {file_path}")

for city, coords in CITIES.items():
    dag_id = f"weather_ingestion_{city.lower()}"

    with DAG(
            dag_id=dag_id,
            schedule="@daily",
            start_date=pendulum.datetime(2026, 4, 24, tz="UTC"),
            catchup=False,
            tags=['factory', 'ingestion']
    ) as dag:

        extract = HttpOperator(
            task_id="extract_data",
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

        save = PythonOperator(
            task_id="save_raw",
            python_callable=_save_raw_data,
            op_kwargs={"city_name": city}
        )

        trigger = TriggerDagRunOperator(
            task_id="trigger_processing",
            trigger_dag_id="weather_processing_dag",
            conf={
                "city": city,
                "file_path": f"{DATA_DIR}/weather_{city}_{{{{ ds_nodash }}}}.json"
            },
        )

        extract >> save >> trigger

        globals()[dag_id] = dag