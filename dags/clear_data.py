from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
import datetime

with DAG(
    dag_id="stage_cleaner",
    start_date=datetime.datetime(2021,11,25),
    schedule_interval="@once",
    catchup=False
) as dag:

    clear_stage_spells = PostgresOperator(
        postgres_conn_id = "stage",
        task_id = "truncate_spells",
        sql = """TRUNCATE spells"""
    )

    clear_stage_spellers = PostgresOperator(
        postgres_conn_id = "stage",
        task_id = "truncate_spellers",
        sql = """TRUNCATE spellers"""
    )

    clear_loader_data = PostgresOperator(
        postgres_conn_id = "stage",
        task_id = "truncate_loads",
        sql = """TRUNCATE loader"""
    )

    clear_stage_spells >> clear_stage_spellers >> clear_loader_data

