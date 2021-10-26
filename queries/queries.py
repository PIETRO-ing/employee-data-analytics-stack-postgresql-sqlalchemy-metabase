import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import time

time.sleep(250)
print('Hello!\nConnecting!')
# make the connection
pg = create_engine('postgresql://postgres:pwd@postgresdb/company', echo=True)

query_00 = """select * 
              from regions;"""
df_00 = pd.read_sql(query_00, pg)
print(df_00)

query_01 = """select * from employees
               where salary > 100000
               order by salary desc
               limit 5;"""
df_01 = pd.read_sql(query_01, pg)
print(df_01)

