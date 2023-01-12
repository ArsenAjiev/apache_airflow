from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

with DAG(
        "5_insert_from_landing_to_stage_dim_table",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="5_insert_from_landing_to_stage_dim_table",
        # schedule=timedelta(days=1),
        schedule=None,
        start_date=days_ago(2),
        catchup=False,
        tags=["dwh", "stage"],
) as dag:

    start_insert = BashOperator(
        task_id="start_insert",
        bash_command="echo start_insert",
    )

procedure_ins_from_land_to_stg_actor_tbl = PostgresOperator(
    task_id="procedure_ins_from_land_to_stg_actor_tbl",
    postgres_conn_id="dwh",
    sql="""
        CREATE OR REPLACE PROCEDURE stage.ins_from_land_to_stg_actor_tbl()
         LANGUAGE plpgsql
        AS $procedure$
        BEGIN
        
        TRUNCATE TABLE stage.actor;
    
        INSERT INTO stage.actor
        (
        actor_id ,
        first_name ,
        last_name ,
        last_update
        )
        SELECT 
        actor_id ,
        first_name ,
        last_name ,
        last_update
        
        FROM landing.actor
            COMMIT;
        END;$procedure$
        ;    
    """,
)

run_procedure_ins_from_land_to_stg_actor_tbl = PostgresOperator(
    task_id="run_procedure_ins",
    postgres_conn_id="dwh",
    sql=['call stage.ins_from_land_to_stg_actor_tbl()'],
    autocommit=True
)


finish_procedure_ins_from_land_to_stg_actor_tbl = BashOperator(
    task_id="finish_ins",
    bash_command="echo finish_ins",
)

start_insert >> procedure_ins_from_land_to_stg_actor_tbl >> run_procedure_ins_from_land_to_stg_actor_tbl >>\
    finish_procedure_ins_from_land_to_stg_actor_tbl
