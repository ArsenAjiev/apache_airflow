from dotenv import load_dotenv
import psycopg2
import os
import sys
import pandas as pd

# take environment variables from .env.
load_dotenv()


def connect():
    """Connect to database"""
    conn = None
    try:
        print('Connecting…')
        conn = psycopg2.connect(
            host=os.environ['DB_PATH'],
            database=os.environ['DB_NAME'],
            user=os.environ['DB_USERNAME'],
            password=os.environ['DB_PASSWORD'],
            port=os.environ['DB_PORT'])

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print('All good, Connection successful!')
    return conn

def postgresql_to_dataframe(conn, select_query, column_names):
    """
    Tranform a SELECT query into a pandas dataframe
    """
    cursor = conn.cursor()
    try:
        cursor.execute(select_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        cursor.close()
        return 1

    # Naturally we get a list of tupples
    tupples = cursor.fetchall()
    cursor.close()

    # We just need to turn it into a pandas dataframe
    df = pd.DataFrame(tupples, columns=column_names)
    return df


# Connect to the database
conn = connect()
column_names = ["payment_id", "customer_id"]
# Execute the "SELECT *" query
df = postgresql_to_dataframe(conn, "select payment_id, customer_id  from payment", column_names)

conn.close()

print(df.head())






