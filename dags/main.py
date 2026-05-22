from airflow import DAG
from datetime import datetime, timedelta

import pendulum

from api.video_stats import extract_video_data, get_playlist_id, get_video_ids, save_to_json
from dataquality.soda import yt_elt_data_quality
from datawarehouse.dwh import core_table, staging_table
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

local_tz = pendulum.timezone("Asia/Kolkata")

# default args

default_args = {
    "owner" : "dataengineers",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # "retries": 1,
    # "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    # "end_date": datetime(2030, 12, 31, tzinfo=local_tz),
}

staging_schema = "staging"
core_schema = "core"

with DAG(
    dag_id='produce_json',
    default_args=default_args,
    description='DAG to produce JSON file with raw data',
    schedule='0 14 * * *',
    catchup=False
) as dag_produce:
    
    #define task
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_json = save_to_json(extract_data)

    trigger_update_db = TriggerDagRunOperator(
        task_id = "trigger_update_db",
        trigger_dag_id = "update_db",
    )

    #define dependencies
    playlist_id >> video_ids >> extract_data >> save_json >> trigger_update_db


with DAG(
    dag_id='update_db',
    default_args=default_args,
    description='DAG to produce JSON file and insert data into both staging and core schemas',
    schedule=None,
    catchup=False
) as dag_update:
    
    #define taks
    update_staging = staging_table()
    update_core = core_table()

    trigger_data_quality = TriggerDagRunOperator(
        task_id = "trigger_data_quality",
        trigger_dag_id = "data_quality",
    )
    
    #define dependencies
    update_staging >> update_core >> trigger_data_quality


with DAG(
    dag_id='data_quality',
    default_args=default_args,
    description='DAG to check the data quality on both layers in thee db',
    schedule=None,
    catchup=False
) as dag_quality:
    
    #define taks
    soda_validate_staging = yt_elt_data_quality(staging_schema)
    sdoa_validate_core = yt_elt_data_quality(core_schema)

    #define dependencies
    soda_validate_staging >> sdoa_validate_core