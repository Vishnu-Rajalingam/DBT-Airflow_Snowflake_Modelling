import pandas as pd

# Snowflake connector libraries
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

# https://fq92599.canada-central.azure.snowflakecomputing.com
#Module to create the snowflake connection and return the connection objects
def create_connection():
   conn = snow.connect(
      user="vishnurajalingam",
      password="Sanguine@93",
      account="hmrtgyw-pj51038",
      warehouse="COMPUTE_WH",
      database="NETFLIX",
      schema="NETFLIX_RAW"
    )
   cursor = conn.cursor()
   print('SQL Connection Created')
   return cursor,conn

# Module to truncate the table if exists. This will ensure duplicate load doesn't happen
def truncate_table():
   cur,conn=create_connection()
   sql_titles = "TRUNCATE TABLE IF EXISTS TITLES"
   sql_credits = "TRUNCATE TABLE IF EXISTS CREDITS"
   cur.execute(sql_titles)
   cur.execute(sql_credits)
   cur.execute('USE DATABASE NETFLIX')
   cur.execute('USE SCHEMA NETFLIX_RAW')
   print('Tables truncated')

#Module to read csv file and load data in Snowflake. Table is created dynamically
def load_data():
   cur,conn=create_connection()
   cur.execute('USE DATABASE NETFLIX')
   cur.execute('USE SCHEMA NETFLIX_RAW')
   credits_df = pd.read_csv("credits.csv", sep = ',')
   credits_df.columns = map(lambda x: str(x).upper(), credits_df.columns)
   print("Credits file read")
   titles_df = pd.read_csv("netflix_titles.csv", sep = ',')
   print("Titles file read")

   write_pandas(conn, titles_df, "TITLES",auto_create_table=True)
   print('Titles file loaded')
   write_pandas(conn, credits_df, "CREDITS", quote_identifiers=True)
   print('Credits file loaded')

   cur = conn.cursor()
   # Close your cursor and your connection.
   cur.close()
   conn.close()

print("Starting Script")
truncate_table()
load_data()
# cur, conn = create_connection()
# cur.execute('CREATE DATABASE DEMO_DB')
# cur.close()
# conn.close()