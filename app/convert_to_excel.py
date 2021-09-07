from os import error
import pandas as pd
import psycopg2 as ps
from psycopg2 import Error

def convert_to_excel():
    try:
        conn = ps.connect(
            user="root",
            password="root",
            host="localhost",
            port=5432,
            database="test_db")

        cursor = conn.cursor()

        df = pd.read_sql_query('select * from dbo_fin_facility', conn)
        with pd.ExcelWriter('downloads/xls/dbo_fin_facility.xlsx') as writer:
            df.to_excel(writer, sheet_name="data_dbo_fin_facility")
            print("Successfully export data to Excell")
    except (Exception, Error) as error:
        print("Error ", error)
    finally:
        if(conn):
            cursor.close()
            conn.close()

convert_to_excel()
