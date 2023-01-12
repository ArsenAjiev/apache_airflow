# We'll start by importing the DAG object
from datetime import timedelta

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
import pandas as pd
import os
import psycopg2

conn_source = psycopg2.connect(
    dbname="amjggcki",
    user="amjggcki",
    password="VvW9ZKw8SGRZuv47VTqXumYGXpDCYs5e",
    host="mel.db.elephantsql.com")

engine = create_engine('postgresql+psycopg2://amjggcki:VvW9ZKw8SGRZuv47VTqXumYGXpDCYs5e@mel.db.elephantsql.com/amjggcki')

# get dag directory path
dag_path = os.getcwd()


def transform_data():
    booking = pd.read_csv(f"{dag_path}/raw_data/booking.csv", low_memory=False)
    client = pd.read_csv(f"{dag_path}/raw_data/client.csv", low_memory=False)
    hotel = pd.read_csv(f"{dag_path}/raw_data/hotel.csv", low_memory=False)

    # merge booking with client
    data = pd.merge(booking, client, on='client_id')
    data.rename(columns={'name': 'client_name', 'type': 'client_type'}, inplace=True)

    # merge booking, client & hotel
    data = pd.merge(data, hotel, on='hotel_id')
    data.rename(columns={'name': 'hotel_name'}, inplace=True)

    # make date format consistent
    data.booking_date = pd.to_datetime(data.booking_date, infer_datetime_format=True)

    # make all cost in GBP currency
    data.loc[data.currency == 'EUR', ['booking_cost']] = data.booking_cost * 0.8
    data.currency.replace("EUR", "GBP", inplace=True)

    # remove unnecessary columns
    data = data.drop('address', 1)

    # load processed data
    data.to_csv(f"{dag_path}/processed_data/processed_data.csv", index=False)


def load_data():
    # Open a cursor to perform database operations
    cur = conn_source.cursor()

    cur.execute('''
                   CREATE TABLE IF NOT EXISTS booking_record (
                       client_id INTEGER NOT NULL,
                       booking_date TEXT NOT NULL,
                       room_type TEXT NOT NULL,
                       hotel_id INTEGER NOT NULL,
                       booking_cost NUMERIC,
                       currency TEXT,
                       age INTEGER,
                       client_name TEXT,
                       client_type TEXT,
                       hotel_name TEXT
                   );
                ''')
    # Make the changes to the database persistent
    conn_source.commit()
    # read rows from db

    records = pd.read_csv(f"{dag_path}/processed_data/processed_data.csv")
    print(records)
    records.to_sql('booking_record', engine, if_exists='replace', index=False)


# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

ingestion_dag = DAG(
    'booking_ingestion',
    default_args=default_args,
    description='Aggregates booking records for data analysis',
    schedule_interval=timedelta(days=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=ingestion_dag,
)

task_1 >> task_2
