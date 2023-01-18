from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python import PythonOperator
from psycopg2 import OperationalError


import os
import psycopg2

# путь до корневой папки airflow в докере
# нужен для загрузки данных в файл
dag_path = os.getcwd()

try:
    conn = psycopg2.connect(
        dbname="dvdrental",
        user="postgres",
        password="tr134sdfWE",
        port=5433,
        host="host.docker.internal")
except OperationalError as err:
    conn = None




# в функцию в качестве параметров передаем соединение с целевой БД (conn)
# название БД например - ('dvdrental')
# и название таблицы в БД например - ('public.actor')
def write_csv_from_table(connection, db, table):
    cur = connection.cursor()
    file = open(f'{dag_path}/raw_data/{db}.{table}.csv', 'w', encoding="utf-8")
    sql = f"COPY (SELECT * FROM {table}) TO STDOUT WITH CSV HEADER"
    cur.copy_expert(sql, file)
    file.close()
    connection.commit()
    cur.close()
    return None


# в функцию в качестве параметров передаем соединение с целевой БД (conn)
# название БД например - ('dvdrental')
# и название таблицы в БД например ('public.actor')
# указываем условие, например данные только за май 2007 года (payment_date BETWEEN '2014-02-01' AND '2014-03-01')
def write_csv_from_table_with_condition(connection, db,  table, condition):
    cur = connection.cursor()
    file = open(f'{dag_path}/raw_data/{db}.{table}.csv', 'w', encoding="utf-8")
    sql = f"COPY (SELECT * FROM {table} WHERE {condition}) TO STDOUT WITH CSV HEADER"
    cur.copy_expert(sql, file)
    file.close()
    connection.commit()
    cur.close()
    return None


with DAG(
        "3_save_dvdrental_db_to_scv",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),

        },
        description="3_save_dvdrental_db_to_scv",
        # schedule=timedelta(days=1),
        schedule=None,
        start_date=days_ago(2),
        catchup=False,
        tags=["dvdrental", "public"],
) as dag:
    dvdrental_actor_to_csv = PythonOperator(
        task_id='dvdrental_actor_to_csv',
        provide_context=True,
        python_callable=write_csv_from_table,
        op_kwargs={
            'connection': conn,
            'db': 'dvdrental',
            'table': 'public.actor'},
        dag=dag,
    )

    dvdrental_payment_to_csv_with_condition = PythonOperator(
        task_id='dvdrental_actor_to_csv_with_condition',
        provide_context=True,
        python_callable=write_csv_from_table_with_condition,
        op_kwargs={
            'connection': conn,
            'db': 'dvdrental',
            'table': 'public.payment',
            # payment_date BETWEEN '2007-02-01' AND '2007-03-01' - данные за февраль
            'condition': "payment_date BETWEEN '2007-03-01' AND '2007-04-01'"
        },
        dag=dag,
    )

    dvdrental_actor_to_csv >> dvdrental_payment_to_csv_with_condition
