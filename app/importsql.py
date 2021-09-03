import pandas as pd
from sqlalchemy import create_engine
import psycopg2 as ps
from sqlalchemy.sql.expression import false

def import_sql():
    df = pd.read_csv('file/DBO_FIN_FACILITY.csv')
    engine = create_engine("postgresql+psycopg2://root:root@localhost/test_db")
    df.to_sql('dbo_fin_facility', con=engine, if_exists='replace', index=false)