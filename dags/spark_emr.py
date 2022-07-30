import datetime

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# with DAG("db_ingestion", start_date=datetime.date(2022, 1, 1)) as dag:
#    start_workflow = DummyOperator(task_id="start_workflow")
#    validate = DummyOperator(task_id="validate")
#    prepare = DummyOperator(task_id="prepare")
#    load = DummyOperator(task_id="load")
#    end_workflow = DummyOperator(task_id="end_workflow")

#    start_workflow >> validate >> prepare >> load >> end_workflow


def ingest_data():
    s3_hook = S3Hook(
        aws_conn_id="aws_default",  # esta es la conexion que se utilizará para los s3
    )
    psql_hook = PostgresHook(
        postgres_conn_id="rds_conn"
    )  # Aqui se pone el nombre de la conexxion para el postgres
    file = s3_hook.download_file(
        key="user_purchase.csv",  # este es el pad del archivo que previamente tiene que estar en un s3
        bucket_name="my-tf-test-bucket-el-martin",  # Este es el nombre del buquet que contiene a mis archivos csv
    )
    psql_hook.bulk_load(
        table="user_purchase", tmp_file="file"
    )  # aqui se especifica el nombre de la base de datos en postgres


with DAG(
    "db_ingestion", start_date=days_ago(1), schedule_interval="@once"
) as dag:
    start_workflow = DummyOperator(task_id="start_workflow")  # inicio
    validate = S3KeySensor(
        task_id="validate",
        aws_conn_id="aws_default",  # esta es la coneccion utilizada para el s3
        bucket_name="my-tf-test-bucket-el-martin",  # Nombre del bucket que contiene el archivo csv que convertire en una base de datos postgres
        bucket_key="user_purchase.csv",  # este es el path del archivo csv dentro del bucket s3
    )
    prepare = PostgresOperator(
        task_id="prepare",
        postgres_conn_id="rds_conn",  # esta es la conexion utilizada para el rds. lo siguiente es el query para definir el postgres db
        sql="""                                                
            CREATE TABLE IF NOT EXISTS user_purchase (
                invoice_number varchar(10),
                stock_code varchar(20),
                detail varchar(1000),
                quantity int,
                invoice_date timestamp,
                unit_price numeric(8,3),
                customer_id int,
                country varchar(20)
            )
        """,
    )
    clear = PostgresOperator(
        task_id="clear",
        postgres_conn_id="rds_conn",
        sql="""DELETE FROM user_purchase""",  # este query es para descartar datos repetidos
    )
    continue_workflow = DummyOperator(
        task_id="continue_workflow"
    )  # esto es para continuar en caso de que los datos no esten repetidos
    branch = BranchSQLOperator(
        task_id="is_empty",
        conn_id="rds_conn",  # conexion del rds
        sql="SELECT COUNT(*) AS rows FROM user_purchase",  # si el resultado del conteo es mayor a uno, entonces no se menten los datos
        # si mayor o igual a uno se limpian los datos
        follow_task_ids_if_true=[clear.task_id],
        # si es cero entonces continua con el proceso
        follow_task_ids_if_false=[continue_workflow.task_id],
    )
    load = PythonOperator(  # aqui se meten los datos gracias a la funcion ingest_data
        task_id="load",
        python_callable=ingest_data,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    end_workflow = DummyOperator(
        task_id="end_workflow"
    )  # termina el proceso aqui

    start_workflow >> validate >> prepare >> branch
    branch >> [clear, continue_workflow] >> load >> end_workflow
