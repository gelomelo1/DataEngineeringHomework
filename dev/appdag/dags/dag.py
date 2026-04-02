import sys
from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

sys.path.append("/opt/airflow/appsrc")

# Dag configuration
default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="game_data_pipeline",
    default_args=default_args,
    description="ETL pipeline for Steam, PlayStation, Xbox game data",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_tasks=5,
    tags=["homework", "etl", "game-data"],
)
def game_pipeline_dag():

    # Task definitions

    @task()
    def zero_simulate_task() -> None:
        from zero_simulate import zero_simulate
        zero_simulate()

    @task()
    def one_extract_task() -> None:
        from one_extract import one_extract
        one_extract()

    @task()
    def two_staging_task() -> None:
        from two_staging import two_staging
        two_staging()

    @task()
    def three_transformation_task() -> dict:
        from three_transformation import three_transformation
        three_transformation()

    @task()
    def four_load_task() -> None:
        from four_load import four_load
        four_load()
    
    @task()
    def final_success_task() -> None:
        print("Pipeline cycle was successfull")

    # Define task dependencies

    simulated = zero_simulate_task()
    extracted = one_extract_task()
    staged = two_staging_task()
    transformed = three_transformation_task()
    loaded = four_load_task()
    final_success = final_success_task()

    # Define dependencies
    simulated >> extracted >> staged >> transformed >> loaded >> final_success

# Instantiate DAG
dag = game_pipeline_dag()