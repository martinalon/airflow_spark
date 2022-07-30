from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr_add_steps import (
    EmrAddStepsOperator,
)
from airflow.providers.amazon.aws.operators.emr_create_job_flow import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import (
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

with DAG("db_ingestion", start_date=days_ago(1)) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")
    validate = DummyOperator(task_id="validate")
    prepare = DummyOperator(task_id="prepare")
    load = DummyOperator(task_id="load")
    end_workflow = DummyOperator(task_id="end_workflow")

start_workflow >> validate >> prepare >> load >> end_workflow
