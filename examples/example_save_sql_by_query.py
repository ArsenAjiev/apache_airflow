from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os


import psycopg2

dag_path = os.getcwd()

conn_source = psycopg2.connect(
    dbname="amjggcki",
    user="amjggcki",
    password="VvW9ZKw8SGRZuv47VTqXumYGXpDCYs5e",
    host="mel.db.elephantsql.com")

conn_dwh = psycopg2.connect(
    dbname="rrodgahb",
    user="rrodgahb",
    password="yD-fDiMNn7Z3P2KK55RjSJEj63-4pXU-",
    host="mel.db.elephantsql.com")


def write_csv_from_table(connection, source_table):
    # Open a cursor to perform database operations
    cur = connection.cursor()
    # read rows from db
    cur.execute(f"select count(*) as cnt from {source_table}")
    cnt = cur.fetchone()
    # export to csv
    fid = open(f'{dag_path}/processed_data/source_{source_table}_data.csv', 'w', encoding="utf-8")
    print(type(fid))
    print(fid)
    # save db by query
    sql = f"COPY (SELECT * FROM {source_table} where public.booking_record.client_id > 5 ) TO STDOUT WITH CSV HEADER"
    cur.copy_expert(sql, fid)
    fid.close()
    # Make the changes to the database persistent
    conn_source.commit()
    # Close communication with the database
    cur.close()
    return cnt[0]


# def truncate_table(connection, table):
#     # Open a cursor to perform database operations
#     cur = connection.cursor()
#     # truncate db
#     cur.execute(f'TRUNCATE TABLE {table}')
#     # Make the changes to the database persistent
#     conn_source.commit()
#     # read rows from db
#     cur.execute(f'select count(1) as cnt from {table}')
#     cnt = cur.fetchone()
#     # Close communication with the database
#     cur.close()
#     if cnt[0] == 0:
#         return 'Is OK NO Data'
#     return cnt[0]
#
#
#
# def write_from_csv_table(connection, table, path_to_csv):
#     cur = connection.cursor()
#     sqlstr = f"COPY {table} FROM STDIN DELIMITER ',' CSV HEADER"
#     with open(path_to_csv) as f:
#         cur.copy_expert(sqlstr, f)
#     connection.commit()
#     cur.execute(f'select count(1) as cnt from {table}')
#     cnt = cur.fetchone()
#     return 'Загружено в landing_dwh:', cnt[0], table




with DAG(
        "copy_data_from_source_to_csv",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),

        },
        description="copy_data_from_source_to_csv",
        # schedule=timedelta(days=1),
        schedule=None,
        start_date=days_ago(2),
        catchup=False,
) as dag:

    source_categories_to_csv = PythonOperator(
        task_id='source_data_to_csv',
        provide_context=True,
        python_callable=write_csv_from_table,
        op_kwargs={'connection': conn_source, 'source_table': 'public.booking_record'},
        dag=dag,
    )
