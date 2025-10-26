import pandas as pd
import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='password',
    database='bankdb'
)

query = "SELECT * FROM transactions"
df = pd.read_sql(query, conn)
