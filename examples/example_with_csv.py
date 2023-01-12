from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os


def write_csv_from_table():
    dag_path = os.getcwd()
    # /opt/apache_airflow
    print(dag_path)
    with open(f"{dag_path}/processed_data/foo.csv", "w") as my_empty_csv:
        # in this case
        # with open("/opt/apache_airflow/processed_data/foo.csv", "w")
        pass
    return None


with DAG(
        "3_copy_data_from_source_to_csv",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),

        },
        description="3_copy_data_from_source_to_csv",
        # schedule=timedelta(days=1),
        schedule=None,
        start_date=days_ago(2),
        catchup=False,
) as dag:
    source_categories_to_csv = PythonOperator(
        task_id='source_categories_to_csv',
        provide_context=True,
        python_callable=write_csv_from_table,
        dag=dag,
    )