import os
from pandas.io.sql import pandasSQL_builder
import numpy as np
import pandas as pd
from psycopg2 import Error
import psycopg2

def import_to_db():
    try:
        df = pd.read_csv("file/DBO_FIN_FACILITY.csv")
        file = "DBO_FIN_FACILITY"
        clean_tbl_name = file.lower().replace(" ", "").replace("?","") \
                            .replace("-","_").replace(r"/","_").replace("\\","_").replace("%","") \
                            .replace(")","").replace(r"(","")
        print(clean_tbl_name)
        df.columns = [x.lower().replace(" ", "").replace("?","") \
                            .replace("-","_").replace(r"/","_").replace("\\","_").replace("%","") \
                            .replace(")","").replace(r"(","") for x in df.columns]

        lst = list(df.columns)
        # covert to txt file to get name coloumn from data csv file
        textfile = open('downloads/txt/field_dbo_fin_facility.txt', 'w')
        for i in lst:
            textfile.write(i + "\n")
        textfile.close()

        connection = psycopg2.connect(
                user="root",
                password="root",
                host="localhost",
                port="5432",
                database="test_db")
        cursor = connection.cursor()

        cursor.execute(" drop table if exists dbo_fin_facility ")

        create_tabel_query = ''' CREATE TABLE dbo_fin_facility (
                applid VARCHAR(2),
                prodid CHAR(8),
                cifid CHAR(10),
                branchid VARCHAR(5),
                idfacility  VARCHAR(13) NOT NULL PRIMARY KEY,
                amount DECIMAL(18,2),
                flexamt DECIMAL(18,2),
                opendt_date TIMESTAMP,
                opendt_time TIME,
                period SMALLINT,
                strdt_date TIMESTAMP,
                strdt_time TIME,
                duedt_date TIMESTAMP,
                duedt_time TIME,
                stsid SMALLINT,
                stsrevol SMALLINT,
                stscommit SMALLINT,
                note VARCHAR(50),
                mainaccbranch CHAR(5),
                mainacc VARCHAR(16),
                acceslvl SMALLINT,
                crtuser VARCHAR(8),
                crtdate_date INTERVAL,
                crtdate_time TIME,
                upduser VARCHAR(8),
                upddate_date INTERVAL,
                upddate_time TIME,
                authrzuser VARCHAR(8),
                authdt_date INTERVAL,
                authdt_time TIME,
                flexaccamt DECIMAL(18,2),
                amtckpn DECIMAL(18,2),
                amtckpnold DECIMAL(18,2),
                contractno VARCHAR(65)
        ); '''
        cursor.execute(create_tabel_query)
        connection.commit()
        print("Table created successfully in PostgreSQL ")
        cursor.execute("SELECT version();")

        df.to_csv('file/convert/dbo_fin_facility.csv', header=df.columns, index=False, encoding='utf-8')
        my_file = open('file/convert/dbo_fin_facility.csv')
        SQL_STATEMENT =""" COPY dbo_fin_facility FROM STDIN WITH CSV HEADER DELIMITER AS ',' """
        cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
        print('file copy to db')
        cursor.execute(" grant select on table dbo_fin_facility to public ")
        connection.commit()
        cursor.close()
        print('Data successfuly upload to db')
    except (Exception, Error) as error:
        print("Erro to connection databae", error)
    finally:
        if(connection):
            cursor.close()
            connection.close()
            print('Close Connection')

import_to_db()