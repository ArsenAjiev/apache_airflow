from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

with DAG(
        "7_insert_from_stg_tmp_to_stage_fact_table",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="fact",
        # schedule=timedelta(days=1),
        schedule=None,
        start_date=days_ago(2),
        catchup=False,
        tags=["dwh", "stage", "target_tbl"],
) as dag:
    start_fact = BashOperator(
        task_id="start_insert",
        bash_command="echo start_insert",
    )


procedure_ins_from_stg_payment_tmp_to_stg_payment_tbl = PostgresOperator(
    task_id="procedure_ins_from_stg_payment_tmp_to_stg_payment_tbl",
    postgres_conn_id="dwh",
    sql="""
        CREATE OR REPLACE PROCEDURE stage.ins_from_stg_payment_tmp_to_stg_payment_tbl()
         LANGUAGE plpgsql
        AS $procedure$
        BEGIN

        MERGE INTO stage.payment t
        USING stage.payment_tmp s
            ON t.payment_id = s.payment_id
        WHEN NOT MATCHED THEN
                INSERT (payment_id, customer_id, staff_id, rental_id, amount, payment_date)
                VALUES (s.payment_id, s.customer_id, s.staff_id, s.rental_id, s.amount, s.payment_date)

        WHEN MATCHED AND t.payment_id IS DISTINCT FROM s.payment_id THEN
            UPDATE SET payment_id = s.payment_id	
        WHEN MATCHED AND t.customer_id IS DISTINCT FROM s.customer_id THEN
            UPDATE SET customer_id = s.customer_id
        WHEN MATCHED AND t.staff_id IS DISTINCT FROM s.staff_id THEN
            UPDATE SET staff_id = s.staff_id	
        WHEN MATCHED AND t.rental_id IS DISTINCT FROM s.rental_id THEN
            UPDATE SET rental_id = s.rental_id
        WHEN MATCHED AND t.amount IS DISTINCT FROM s.amount THEN
            UPDATE SET amount = s.amount
        WHEN MATCHED AND t.payment_date IS DISTINCT FROM s.payment_date THEN
            UPDATE SET payment_date = s.payment_date;


            COMMIT;
        END;$procedure$
        ;    
    """,
)

run_procedure_ins_from_stg_payment_tmp_to_stg_payment_tbl = PostgresOperator(
    task_id="run_procedure_ins_from_stg_payment_tmp_to_stg_payment_tbl",
    postgres_conn_id="dwh",
    sql=['call stage.ins_from_stg_payment_tmp_to_stg_payment_tbl()'],
    autocommit=True
)

finish_procedure_procedure_ins_from_land_to_stg_payment_tmp_tbl = BashOperator(
    task_id="finish_ins",
    bash_command="echo finish_ins",
)

start_fact >> procedure_ins_from_stg_payment_tmp_to_stg_payment_tbl >> \
    run_procedure_ins_from_stg_payment_tmp_to_stg_payment_tbl >> \
    finish_procedure_procedure_ins_from_land_to_stg_payment_tmp_tbl
