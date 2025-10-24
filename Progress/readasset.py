import pyodbc
import pandas as pd

conn_str = (
    "Driver={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=Cryptolab;"
    "Trusted_connection=yes;"
    "Encrypt=no;"
)

cnxn = pyodbc.connect(conn_str)

df = pd.read_sql("SELECT * FROM dbo.Assets", cnxn)
print("Veriler : ")
print(df)

cnxn.close()