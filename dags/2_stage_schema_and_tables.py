from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

with DAG(
        "2_stage_schema_and_tables",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="Create stage schema and 15 tables ",
        # schedule=timedelta(days=1),
        schedule=None,
        start_date=days_ago(2),
        catchup=False,
        tags=["dwh", "stage"],
) as dag:
    create_schema_stage = PostgresOperator(
        task_id="create_schema_stage",
        postgres_conn_id="dwh",
        sql=
        """CREATE SCHEMA IF NOT EXISTS stage""",
    )

finish_create_schema_stage = BashOperator(
    task_id="finish_create_schema_stage",
    bash_command="echo finish_create_schema_stage",
)

procedure_create_stage_actor_tbl = PostgresOperator(
    task_id="procedure_create_stage_actor_tbl",
    postgres_conn_id="dwh",
    sql="""
    CREATE OR REPLACE PROCEDURE stage.create_actor_tbl()
     LANGUAGE plpgsql
    AS $procedure$
    BEGIN

    CREATE TABLE IF NOT EXISTS stage.actor
(
    actor_id integer NOT NULL,
    first_name character varying(45) NOT NULL,
    last_name character varying(45)  NOT NULL,
    last_update timestamp without time zone ,
    PRIMARY KEY (actor_id)
    );

        COMMIT;
    END;$procedure$
    ;    
    """,
)

procedure_create_stage_payment_tbl = PostgresOperator(
    task_id="procedure_create_stage_payment_tbl",
    postgres_conn_id="dwh",
    sql="""
    CREATE OR REPLACE PROCEDURE stage.create_payment_tbl()
     LANGUAGE plpgsql
    AS $procedure$
    BEGIN

    CREATE TABLE IF NOT EXISTS stage.payment
    (
    payment_id integer NOT NULL,
    customer_id smallint NOT NULL,
    staff_id smallint NOT NULL,
    rental_id integer NOT NULL,
    amount numeric(5,2) NOT NULL,
    payment_date timestamp without time zone NOT NULL,
    PRIMARY KEY (payment_id)
    );

        COMMIT;
    END;$procedure$
    ;    
    """,
)

finish_create_procedure_stage = BashOperator(
    task_id="finish_create_procedure_stage",
    bash_command="echo finish_create_procedure_stage",
)

run_procedure_create_stage_actor_tbl = PostgresOperator(
    task_id="run_procedure_create_stage_actor_tbl",
    postgres_conn_id="dwh",
    sql=['call stage.create_actor_tbl()'],
    autocommit=True
)

run_procedure_create_stage_payment_tbl = PostgresOperator(
    task_id="run_procedure_create_stage_payment_tbl",
    postgres_conn_id="dwh",
    sql=['call stage.create_payment_tbl()'],
    autocommit=True
)

finish_run_procedure_stage = BashOperator(
    task_id="finish_run_procedure_stage",
    bash_command="echo finish_run_procedure_stage",
)

create_schema_stage >> \
finish_create_schema_stage >> \
[procedure_create_stage_actor_tbl, procedure_create_stage_payment_tbl] >> \
finish_create_procedure_stage >> \
[run_procedure_create_stage_actor_tbl, run_procedure_create_stage_payment_tbl] >> \
finish_run_procedure_stage
