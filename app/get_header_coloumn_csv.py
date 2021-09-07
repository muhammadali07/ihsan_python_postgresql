from pandas.core.reshape.melt import wide_to_long
import psycopg2 as ps
import pandas as pd

def get_name_coloumn():
    df = pd.read_csv("file/DBO_FIN_FACILITY.csv")
    df.head()
    df.columns = [x.lower().replace(" ", "").replace("?","") \
                            .replace("-","_").replace(r"/","_").replace("\\","_").replace("%","") \
                            .replace(")","").replace(r"(","") for x in df.columns]
    lst = list(df.columns)

    # covert to txt file to get name coloumn from data csv file
    textfile = open('downloads/txt/field.txt', 'w')
    for i in lst:
        textfile.write(i + "\n")
    textfile.close()


get_name_coloumn()