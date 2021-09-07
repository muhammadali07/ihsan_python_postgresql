import psycopg2
from psycopg2 import Error
from sqlalchemy.engine import create_engine

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
            applid VARCHAR(2),
                prodid CHAR(8),
                cifid CHAR(10),
                branchid VARCHAR(5),
                idfacility  VARCHAR(13) NOT NULL PRIMARY KEY,
                amount DECIMAL(18,2),
                flexamt DECIMAL(18,2),
                opendt_date TIMESTAMP,
                opendt_time TIMESTAMP,
                period SMALLINT,
                strdt_date TIMESTAMP,
                strdt_time TIMESTAMP,
                duedt_date TIMESTAMP,
                duedt_time TIMESTAMP,
                stsid SMALLINT,
                stsrevol SMALLINT,
                stscommit SMALLINT,
                note VARCHAR(50),
                mainaccbranch CHAR(5),
                mainacc VARCHAR(16),
                acceslvl SMALLINT,
                crtuser VARCHAR(8),
                crtdate_date TIMESTAMP,
                crtdate_time TIMESTAMP,
                upduser VARCHAR(8),
                upddate_date TIMESTAMP,
                upddate_time TIMESTAMP,
                authrzuser VARCHAR(8),
                authdt_date TIMESTAMP,
                authdt_time TIMESTAMP,
                flexaccamt DECIMAL(18,2),
                amtckpn DECIMAL(18,2),
                amtckpnold DECIMAL(18,2),
                contractno VARCHAR(13)
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

create_tabel()