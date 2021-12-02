from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import datetime

def get_range(**kwargs):
    prod_connector = PostgresHook(postgres_conn_id = "prod").get_conn()
    stage_connector = PostgresHook(postgres_conn_id= "stage").get_conn()
    max_id = 0
    min_id = 0
    with prod_connector.cursor() as prod_cursor:
        prod_cursor.execute("""select speller_id from spellers order by speller_id desc limit 1""")
        res = prod_cursor.fetchone()
        if res is not None:
            max_id = res[0]
    
    with stage_connector.cursor() as stage_cursor:
        stage_cursor.execute("""select MAX(id_greatest) FROM loader where tabname='spellers' and operation='uploaded'""")
        res = stage_cursor.fetchone()
        if res[0] is not None:
            min_id = res[0]
    return {'max_id': max_id, 'min_id': min_id}

def init_upload(**kwargs):
    id_range = kwargs['ti'].xcom_pull(task_ids='get_id_range') 
    stage_connector = PostgresHook(postgres_conn_id= "stage").get_conn()
    prod_connector = PostgresHook(postgres_conn_id= "prod").get_conn()
    fname = "/dumps/spellers.{min_id}.{max_id}.csv".format(min_id = id_range['min_id'], max_id = id_range['max_id'])    
    with stage_connector.cursor() as stage_cursor:
        stage_cursor.execute("""INSERT INTO loader(operation, tabname, id_least, id_greatest) VALUES (%s, %s, %s, %s)""", ("init", "spellers", id_range['min_id'], id_range['max_id']))
    stage_connector.commit()
    with prod_connector.cursor() as cursor:
        with open(fname, "w") as dmp_file:
            cursor.copy_expert('''COPY (SELECT * FROM spellers WHERE speller_id > {min_id} AND speller_id <= {max_id}) TO STDOUT WITH (FORMAT CSV)'''.format(min_id = id_range['min_id'], max_id = id_range['max_id']), dmp_file)

def finish_upload(**kwargs):
    id_range = kwargs['ti'].xcom_pull(task_ids='get_id_range') 
    stage_connector = PostgresHook(postgres_conn_id= "stage").get_conn()
    with stage_connector.cursor() as stage_cursor:
        stage_cursor.execute("""INSERT INTO loader(operation, tabname, id_least, id_greatest) VALUES (%s, %s, %s, %s)""", ("uploaded", "spellers", id_range['min_id'], id_range['max_id']))
    stage_connector.commit()

with DAG(
    dag_id="download_spellers",
    start_date=datetime.datetime(2021,11,25),
    schedule_interval=datetime.timedelta(seconds=50, hours=10),
    catchup=False,
    max_active_runs = 1
) as dag:


    gen = PythonOperator(
        python_callable = get_range,
        xcom_push = True,
        task_id = "get_id_range"
    )

    download = PythonOperator(
        python_callable = init_upload,
        provide_context = True,
        task_id = "dump_to_csv"
    )
    finish = PythonOperator(
        python_callable = finish_upload,
        provide_context = True,
        task_id = "finalize_upload"
    )

    gen >> download >> finish