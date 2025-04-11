

import pendulum
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.state import State # To check task success

# --- Configuration Variables (Paths INSIDE the Container) ---
PROJECT_ROOT = Path("/opt/airflow") # Base directory inside the container
PYTHON_SCRIPT_DIR = PROJECT_ROOT / "scripts"
# SQL_SCRIPT_DIR = PROJECT_ROOT / "sql"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"

# --- Airflow/Tool Specific Config ---
PYTHON_EXE = "python" # Use the default python in the container's PATH
SNOWFLAKE_CONN_ID = "snowflake_default" # The Connection ID to create in Airflow UI for SnowflakeOperator

# --- Environment Variables for the Python Load Script ---
# These should match the environment variables your hospital_data_script.py expects
# We will set these in docker-compose.yaml
SNOWFLAKE_ENV_VARS = {
    "SNOWFLAKE_USER": "{{ var.value.get('snowflake_user', 'DEFAULT_USER') }}", # Example using Airflow Variables (Best Practice)
    "SNOWFLAKE_PASSWORD": "{{ var.value.get('snowflake_password', 'DEFAULT_PASSWORD') }}",
    "SNOWFLAKE_ACCOUNT": "{{ var.value.get('snowflake_account', 'DEFAULT_ACCOUNT') }}",
    "SNOWFLAKE_DATABASE": "{{ var.value.get('snowflake_database', 'HOSPITAL_RAW') }}",
    "SNOWFLAKE_SCHEMA": "{{ var.value.get('snowflake_schema', 'RAW_DATA') }}",
    "SNOWFLAKE_WAREHOUSE": "{{ var.value.get('snowflake_warehouse', 'COMPUTE_WH') }}",
    "SNOWFLAKE_ROLE": "{{ var.value.get('snowflake_role', 'ACCOUNTADMIN') }}"
    # --- OR ---
    # If getting directly from .env via docker-compose (Simpler but less secure for passwords)
    # "SNOWFLAKE_USER": "{{ macros.os.environ.get('SNOWFLAKE_USER_FROM_ENV') }}",
    # "SNOWFLAKE_PASSWORD": "{{ macros.os.environ.get('SNOWFLAKE_PASSWORD_FROM_ENV') }}",
    # ... etc ... Requires setting these in docker-compose environment section
}


# --- DAG Definition ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=2),
    'snowflake_conn_id': SNOWFLAKE_CONN_ID # Default for SnowflakeOperator
}

with DAG(
    dag_id='hospital_elt_docker_direct_load', # New unique DAG ID
    default_args=default_args,
    description='Orchestrate Hospital ELT (Docker - Python Direct Load): Generate/Load, DBT',
    schedule_interval=None, # Manual trigger
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"), # Adjust as needed
    catchup=False,
    tags=['hospital', 'elt', 'snowflake', 'dbt', 'docker', 'direct-load'],
) as dag:

    # --- Task Definitions ---

    # Task 1: Generate AND Load data using the Python script
    # This task now handles both steps 1 and 3 from your original plan
    task_generate_and_load_staging = BashOperator(
        task_id='generate_and_load_staging_data',
        # Execute the python script using its path INSIDE the container
        bash_command=f"{PYTHON_EXE} {PYTHON_SCRIPT_DIR / 'hospital_data_script.py'}",
        env=SNOWFLAKE_ENV_VARS, # Pass Snowflake credentials as environment variables
        doc_md="Runs Python script to generate data and load directly into Snowflake staging tables via SQLAlchemy."
    )

   

    # Task 2: Run dbt transformations )
    task_run_dbt = BashOperator(
        task_id='run_dbt_transformations',
        # Change directory to the dbt project path INSIDE the container
        # Uses dbt installed in the container & the mounted ~/.dbt/profiles.yml
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
        doc_md="Executes 'dbt run' in the container's dbt project directory."
    )

    # --- Define Task Dependencies (Revised Workflow Order) ---

    # Generate/Load Staging -> Run DBT
    task_generate_and_load_staging >> task_run_dbt

    # --- Alternative Workflow (If truncation is not desired right after load): ---
    # task_generate_and_load_staging >> task_process_staging >> task_run_dbt
    # (Comment out task_truncate_tables and adjust the chain above if removing it)