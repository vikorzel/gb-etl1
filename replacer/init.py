import time
import psycopg2
import os

def init_stage_tables():
    stage_connector = psycopg2.connect(
        user='pguser',
        password='pgpassword',
        database='magic',
        host='stage_db'
    )
    with stage_connector.cursor() as cursor:
        for schema in os.listdir('schemas'):
            with open(os.path.join('schemas', schema)) as f:
                cursor.execute(f.read())
        stage_connector.commit()
    print("schema inited")


if __name__ == "__main__":
    while True:
        try:
            init_stage_tables()
            break
        except:
            time.sleep(5)
    while True:
        time.sleep(20)
