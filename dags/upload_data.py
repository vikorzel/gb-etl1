from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import datetime
import os

def upload_data(**kwargs):
    stage_connector = PostgresHook(postgres_conn_id = "stage").get_conn()
    processed_files = []
    for fname in os.listdir('/dumps'):
        (tabname, min_id, max_id) = fname.split('.')[:-1]
        with open(os.path.join('/dumps', fname)) as ofile, stage_connector.cursor() as cursor:
              cursor.copy_expert('''COPY {} FROM STDIN WITH (FORMAT CSV)'''.format(tabname), ofile)
        processed_files.append(
            {
                'fpath': os.path.join('/dumps', fname),
                'min_id': min_id,
                'max_id': max_id,
                'tname': tabname
            }
        )
        stage_connector.commit()
    return processed_files


def update_status(**kwargs):
    processed_files = kwargs['ti'].xcom_pull(task_ids='load_from_csv') 
    stage_connector = PostgresHook(postgres_conn_id= "stage").get_conn()
    with stage_connector.cursor() as stage_cursor:
        for pfile in processed_files:
            stage_cursor.execute("""INSERT INTO loader(operation, tabname, id_least, id_greatest) VALUES (%s, %s, %s, %s)""", ("finished", pfile['tab_name'], pfile['min_id'], pfile['max_id']))
    stage_connector.commit()

def delete_files(**kwargs):
    processed_files = kwargs['ti'].xcom_pull(task_ids='load_from_csv') 
    for pfile in processed_files:
        os.remove(pfile['fpath'])



with DAG(
    dag_id="load_on_stage",
    start_date=datetime.datetime(2021,11,25),
    schedule_interval=datetime.timedelta(seconds=30), #'@daily'
    catchup=False,
    max_active_runs = 1
) as dag:
    load = PythonOperator(
        python_callable = upload_data,
        provide_context = True,
        xcom_push = True,
        task_id = 'load_from_csv'
    )

    update_status = PythonOperator(
        python_callable = upload_data,
        provide_context = True,
        xcom_push = True,
        task_id = 'update_status'
    )

    delete = PythonOperator(
        python_callable = delete_files,
        provide_context = True,
        xcom_push = True,
        task_id = 'cleanup'
    )

    load >> update_status >> delete