from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator

import os
import psycopg2

# необходимо для формирования пути к csx файлу
dag_path = os.getcwd()

# подключение к БД
conn_source = psycopg2.connect(
    dbname="dwh",
    user="postgres",
    password="tr134sdfWE",
    port=5433,
    host="host.docker.internal")


# Запись данных из CSV файла в таблицу БД DWH
def write_from_csv_to_table(connection, database, tbl):
    """
    :param connection: подключение к БД DWH
    :param database: аргумент необходим для формирования пути к csv файлу
    :param src_tbl: аргумент необходим для формирования пути к csv файлу
        path_to_csv = f'{dag_path}/raw_data/{database}.{src_tbl}.csv'
    :param tg_tbl: целевая таблица в БД DWH куда будут записываться данные из csv файла
    :return:
    """
    cur = connection.cursor()
    truncate_sql = f'truncate table landing.{tbl}'
    cur.execute(truncate_sql)
    connection.commit()
    cur.execute(f'select count(1) as cnt from landing.{tbl}')
    cnt_1 = cur.fetchone()
    sqlstr = f"COPY landing.{tbl} FROM STDIN DELIMITER  ',' CSV HEADER"
    path_to_csv = f'{dag_path}/raw_data/{database}.public.{tbl}.csv'
    with open(path_to_csv) as f:
        cur.copy_expert(sqlstr, f)
    connection.commit()
    cur.execute(f'select count(1) as cnt from landing.{tbl}')
    cnt_2 = cur.fetchone()
    return 'Загружено в dwh:', cnt_1[0], cnt_2[0]


with DAG(
        "4_upload_data_to_dwh_landing_db_from_scv",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),

        },
        description="4_upload_data_to_dwh_landing_db_from_scv",
        # schedule=timedelta(days=1),
        schedule=None,
        start_date=days_ago(2),
        catchup=False,
        tags=["dwh", "landing"],
) as dag:
    upload_data_to_dwh_landing_actor_from_scv = PythonOperator(
        task_id='upload_data_to_dwh_landing_actor_from_scv',
        provide_context=True,
        python_callable=write_from_csv_to_table,
        op_kwargs={
            'connection': conn_source,
            'database': 'dvdrental',
            'tbl': 'actor',

        },
        dag=dag,
    )

    upload_data_to_dwh_landing_payment_from_scv = PythonOperator(
        task_id='upload_data_to_dwh_landing_payment_from_scv',
        provide_context=True,
        python_callable=write_from_csv_to_table,
        op_kwargs={
            'connection': conn_source,
            'database': 'dvdrental',
            'tbl': 'payment',


        },
        dag=dag,
    )

    upload_data_to_dwh_landing_actor_from_scv >> upload_data_to_dwh_landing_payment_from_scv
