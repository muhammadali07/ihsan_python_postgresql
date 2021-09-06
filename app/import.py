import os

from pandas.io.sql import pandasSQL_builder
import numpy as np
import pandas as pd
import psycopg2

def import_to_db():
    try:
        df = pd.read_csv('file/DBO_FIN_FACILITY.csv')

        df.columns =[x.lower() for x in df.columns]
        print(df.columns)

        connection = psycopg2.connect(
                user="root",
                password="root",
                host="localhost",
                port="5432",
                database="test_db"
            )

        conn = psycopg2.connect(connection)
        cursor = conn.cursor()
        df.to_csv('file/DBO_FIN_FACILITY.csv', header=df.columns, index=False)
        my_file = open('file/DBO_FIN_FACILITY.csv')
        SQL_STATEMENT =''' COPY dbo_fin_facility FROM STDIN WITH CSV HEADER DELIMITER AS ',' '''
        cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
        cursor.execute(" grant select on table dbo_fin_facility to public ")
        cursor.close()
    except Exception:
        print("just still failed update to db")
    finally:
        cursor.close()
        conn.close()