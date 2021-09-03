import psycopg2
from psycopg2 import Error

def create_tabel():
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
        create_tabele_query = '''CREATE TABLE DBO_FIN_FACILITY(
            APPLID VARCHAR(2),
            PRODID CHAR(8),
            CIFID CHAR(10),
            BRANCHID VARCHAR(5),
            IDFACILITY VARCHAR(13) NOT NULL PRIMARY KEY,
            AMOUNT DECIMAL(18,2),
            FLEXAMT DECIMAL(18,2),
            OPENDT_DATE TIMESTAMP,
            OPENDT_TIME TIMESTAMP,
            PERIOD SMALLINT,
            STRDT_DATE TIMESTAMP,
            STRDT_TIME TIMESTAMP,
            DUEDT_DATE TIMESTAMP,
            DUEDT_TIME TIMESTAMP,
            STSID SMALLINT,
            STSREVOL SMALLINT,
            STSCOMMIT SMALLINT,
            NOTE VARCHAR(50),
            MAINACCBRANCH CHAR(5),
            MAINACC VARCHAR(16),
            ACCESLVL SMALLINT,
            CRTUSER VARCHAR(8),
            CRTDATE_DATE TIMESTAMP,
            CRTDATE_TIME TIMESTAMP,
            UPDUSER VARCHAR(8),
            UPDDATE_DATE TIMESTAMP,
            UPDDATE_TIME TIMESTAMP,
            AUTHRZUSER VARCHAR(8),
            AUTHDT_DATE TIMESTAMP,
            AUTHDT_TIME TIMESTAMP,
            FLEXACCAMT DECIMAL(18,2),
            AMTCKPN DECIMAL(18,2),
            AMTCKPNOLD DECIMAL(18,2),
            CONTRACTNO VARCHAR(13)
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

