import pandas as pd
import psycopg2 as ps


def import_to_sql():
    connection = ps.connect(
            user="root",
            password="root",
            host="localhost",
            port="5432",
            database="test_db"
        )

    cur = connection.cursor()
    # data = 'file/DBO_FIN_FACILITY.csv'
    sql = " \copy dbo_fin_facility from 'file\DBO_FIN_FACILITY.csv'"
    
    with open(sql, 'r') as f:
        next(f)
        cur.copy_from(f, 'dbo_fin_facility', sep=',')
        connection.commit()
import_to_sql()