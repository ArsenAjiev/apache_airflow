from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

with DAG(
        "1_landing_schema_and_tables",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
        },
        description="Create landing schema and 15 tables ",
        # schedule=timedelta(days=1),
        schedule=None,
        start_date=days_ago(2),
        catchup=False,
        tags=["dwh", "landing"],
) as dag:
    create_schema_landing = PostgresOperator(
        task_id="create_schema_landing",
        postgres_conn_id="dwh",
        sql=
        """CREATE SCHEMA IF NOT EXISTS landing""",
    )

finish_create_schema_landing = BashOperator(
    task_id="finish_create_schema_landing",
    bash_command="echo finish_create_schema_landing",
)


procedure_create_landing_actor_tbl = PostgresOperator(
    task_id="procedure_create_landing_actor_tbl",
    postgres_conn_id="dwh",
    sql="""
    CREATE OR REPLACE PROCEDURE landing.create_actor_tbl()
     LANGUAGE plpgsql
    AS $procedure$
    BEGIN
    
    CREATE TABLE IF NOT EXISTS landing.actor
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

procedure_create_landing_payment_tbl = PostgresOperator(
    task_id="procedure_create_landing_payment_tbl",
    postgres_conn_id="dwh",
    sql="""
    CREATE OR REPLACE PROCEDURE landing.create_payment_tbl()
     LANGUAGE plpgsql
    AS $procedure$
    BEGIN

    CREATE TABLE IF NOT EXISTS landing.payment
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

finish_create_procedure_landing = BashOperator(
    task_id="finish_create_procedure_landing",
    bash_command="echo finish_create_procedure_landing",
)

run_procedure_create_landing_actor_tbl = PostgresOperator(
    task_id="run_procedure_create_landing_actor_tbl",
    postgres_conn_id="dwh",
    sql=['call landing.create_actor_tbl()'],
    autocommit=True
)

run_procedure_create_landing_payment_tbl = PostgresOperator(
    task_id="run_procedure_create_landing_payment_tbl",
    postgres_conn_id="dwh",
    sql=['call landing.create_payment_tbl()'],
    autocommit=True
)


finish_run_procedure_landing = BashOperator(
    task_id="finish_run_procedure_landing",
    bash_command="echo finish_run_procedure_landing",
)

create_schema_landing >> \
    finish_create_schema_landing >> \
    [procedure_create_landing_actor_tbl, procedure_create_landing_payment_tbl] >> \
    finish_create_procedure_landing >> \
    [run_procedure_create_landing_actor_tbl, run_procedure_create_landing_payment_tbl] >> \
    finish_run_procedure_landing
