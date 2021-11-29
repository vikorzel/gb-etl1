from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
import tempfile

import datetime

def dump_data(**kwargs):
    ph = PostgresHook(postgres_conn_id = "prod")
    prod_connector = ph.get_conn()
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    with prod_connector.cursor() as cursor:
            cursor.copy_expert('''COPY (SELECT * FROM spells) TO STDOUT WITH (FORMAT CSV)''', tmp_file)
    return tmp_file.name

def load_data(**kwargs):
    ph = PostgresHook(postgres_conn_id = "stage")
    stage_connector = ph.get_conn()
    fname=str(kwargs['ti'].xcom_pull(task_ids='dump_data'))
    with stage_connector.cursor() as cursor:
        with open(fname) as ofile:
            cursor.copy_expert('''COPY spells FROM STDIN WITH (FORMAT CSV)''', ofile)
    stage_connector.commit()
    return fname

with DAG(
    dag_id="data_dumper",
    start_date=datetime.datetime(2021,11,25),
    schedule_interval="@once",
    catchup=False
) as dag:

    clear_data_stage = PostgresOperator(
        postgres_conn_id = "stage",
        task_id = "truncate_spells",
        sql = """TRUNCATE spells"""
    )

    dump_from_prod = PythonOperator(
        task_id = "dump_data",
        python_callable = dump_data,
        xcom_push = True,
        provide_context = True
    )

    load_to_stage = PythonOperator(
        task_id = "load_data",
        python_callable = load_data,
        xcom_push = True,
        provide_context = True
    )

    delete_temp_file = BashOperator (
        task_id = 'delete_temp_file',
        bash_command = 'rm {{ti.xcom_pull("load_data")}}'
    )

    clear_data_stage >> dump_from_prod >> load_to_stage >> delete_temp_file