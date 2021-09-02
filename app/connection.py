import psycopg2
from psycopg2 import Error

try:
    connection = psycopg2.connect(
        user="root",
        password="root",
        host="localhost",
        port="5432",
        database="test_db"
    )

    cursor = connection.cursor()

    # create table
    create_tabele_query = ''' CREATE TABLE mobile (
        ID INT PRIMARY KEY NOT NULL,
        MODEL TEXT NOT NULL,
        PRICE REAL
    ); '''

    cursor.execute(create_tabele_query)
    connection.commit()
    print("Table created successfully in PostgreSQL ")
    print("Informasi PostgreSQL Server")
    print(connection.get_dsn_parameters(),"\n")

    cursor.execute("SELECT version();")

    record = cursor.fetchone()
    print("You connected to  -", record, "\n")
except(Exception, Error) as error:
    print("Erro to connection databae", error)
finally:
    if(connection):
        cursor.close()
        connection.close()
        print("Close Connection")

