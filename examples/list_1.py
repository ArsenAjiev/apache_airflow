import psycopg2
import pandas as pd
import sys

# Connection parameters, yours will be different
param_dic = {
    "host": "host.docker.internal",
    "database": "dvdrental",
    "user": "postgres",
    "password": "tr134sdfWE",
    "port":5433
}


def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print("Connection successful")
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
conn = connect(param_dic)
column_names = ["payment_id", "customer_id"]
# Execute the "SELECT *" query
df = postgresql_to_dataframe(conn, "select payment_id, customer_id  from payment", column_names)
print(df.head())

conn.close()