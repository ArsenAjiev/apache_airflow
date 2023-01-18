import pandas.io.sql as pd_sql
import pandas as pd
import sqlalchemy
import os

connection = sqlalchemy.create_engine("postgresql://postgres:tr134sdfWE@localhost:5433/dvdrental")
dag_path = os.getcwd()


db = 'dvdrental'
table = 'payment_date'


def write_from_table_to_csv():
    df = pd_sql.read_sql("SELECT * FROM payment WHERE payment_date BETWEEN '2007-05-01' AND '2007-06-01'", connection)
    df.to_csv(f'{dag_path}/{db}.{table}.csv', header=True, index=False, encoding='utf-8')
    return None


def write_from_csv_to_table():
    df = pd.read_csv(f'{dag_path}/{db}.{table}.csv')
    df.to_sql('payment2', con=connection, if_exists='replace', index=False)
    return None


write_from_table_to_csv()
write_from_csv_to_table()

