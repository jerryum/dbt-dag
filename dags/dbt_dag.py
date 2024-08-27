# Databricks notebook source
import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator


AIRBYTE_CONNECTION_ID = "airbyte_conn" # configure at Airflow Connection Config
SNOWFLAKE_CONNECTION_ID = "snowflake_conn" # configure at Airflow Connection Config

DBT_EXECUTABLE_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"
DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/dbt/data_pipeline"

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=SNOWFLAKE_CONNECTION_ID, 
        profile_args={"database": "dbt_db", "schema": "dbt_schema"}, # Snowflake database & schema
    )
)


@dag(
    start_date=datetime(2023, 8, 1),
    schedule=None,
    catchup=False,
)
def airbyte_dbt_pipeline():
    # Step 1: Define the Airbyte sync task using Cosmos
    airbyte_sync = AirbyteTriggerSyncOperator(
        task_id="airbyte_sync_task",
        
        connection_id="0b59aee3-dc20-4790-8ff4-067ade455c2b",  # Your Airbyte connection ID
        airbyte_conn_id=AIRBYTE_CONNECTION_ID,    # Your Airflow connection ID
        asynchronous=True
    )

    # Step 2: Define the dbt transformation task group using Cosmos
    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        operator_args={"install_deps": True}
    )

    # Set the task dependencies: Run Airbyte sync first, then dbt transformations
    airbyte_sync >> transform_data


# Instantiate the DAG
airbyte_dbt_pipeline()

