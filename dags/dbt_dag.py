# Databricks notebook source
import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow import DAG
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
import snowflake.connector
import pandas as pd
import great_expectations as ge



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

def get_snowflake_conn_details(conn_id):
    connection = BaseHook.get_connection(conn_id)
    conn_params = {
        "user": connection.login,
        "password": connection.password,
        "account": connection.extra_dejson.get('account'),
        "warehouse": connection.extra_dejson.get('warehouse'),
        "database": connection.schema,  # In Airflow, "schema" often holds the database name for Snowflake
        "role": connection.extra_dejson.get('role'),
    }
    return conn_params

def validate_data():
    # Get Snowflake connection details from Airflow
    conn_params = get_snowflake_conn_details(SNOWFLAKE_CONNECTION_ID)
    print (conn_params)
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=conn_params['user'],
        password=conn_params['password'],
        account=conn_params['account'],
        warehouse=conn_params['warehouse'],
        database=conn_params['database'],
        role=conn_params['role']
    )

    # Read the rules from the Snowflake table
    query = "SELECT * FROM dbt_db.dbt_schema.data_quality_rules"
    rules_df = pd.read_sql(query, conn)
    print(rules_df)
    
    for _, row in rules_df.iterrows():
        table_name = row['TABLE_NAME']
        column_name = row['COLUMN_NAME']
        rule_name = row['RULE_NAME']
        threshold = row['THRESHOLD']
        error_table = row['ERROR_TABLE']

        # Fetch data from the table
        data_query = f"SELECT * FROM dbt_db.dbt_schema.{table_name}"
        df = pd.read_sql(data_query, conn)
        ge_df = ge.from_pandas(df)

        # Apply the validation rule
        if rule_name == 'not_null':
            expectation = ge_df.expect_column_values_to_not_be_null(column_name)
        elif rule_name == 'unique':
            expectation = ge_df.expect_column_values_to_be_unique(column_name)
        elif rule_name == 'within_range':
            expectation = ge_df.expect_column_values_to_be_between(column_name, min_value=threshold)

        # If the validation fails, log invalid rows to the error table
        if not expectation['success']:
            # Filter out the invalid rows
            failing_rows = df.loc[~expectation['success']]
            
            # Prepare the data to match the error table schema
            failing_rows = failing_rows[[column_name]].copy()
            failing_rows['TABLE_NAME'] = table_name
            failing_rows['COLUMN_NAME'] = column_name
            failing_rows['FAILING_VALUE'] = failing_rows[column_name].astype(str)
            failing_rows['ERROR_REASON'] = f'{rule_name} validation failed'
            failing_rows['FAILED_AT'] = pd.Timestamp.now()

            # Insert the failing rows into the error table
            failing_rows[['TABLE_NAME', 'COLUMN_NAME', 'FAILING_VALUE', 'ERROR_REASON', 'FAILED_AT']].to_sql(
                error_table,
                conn,
                if_exists='append',
                index=False
            )
    conn.close()


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

    # Step 3: Define the data quality validation task
    validate_data_task = PythonOperator(
        task_id="validate_data_task",
        python_callable=validate_data
    )
    
    # Set the task dependencies: Run Airbyte sync first, then dbt transformations
    airbyte_sync >> transform_data >> validate_data_task


# Instantiate the DAG
airbyte_dbt_pipeline()

