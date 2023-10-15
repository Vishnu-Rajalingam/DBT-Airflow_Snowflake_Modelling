import pandas as pd

# Snowflake connector libraries
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas


#Module to create the snowflake connection and return the connection objects
def create_connection():
   conn = snow.connect(
      user="vishnurajalingam3",
   password="Sanguine@93",
   account="hsqvdpz-ug08744",
   warehouse="COMPUTE_WH",
   database="NETFLIX",
   schema="NETFLIX_RAW"
   )
   cursor = conn.cursor()
   cursor.execute('CREATE DATABASE IF NOT EXISTS NETFLIX')
   cursor.execute('USE DATABASE NETFLIX')
   cursor.execute('CREATE SCHEMA IF NOT EXISTS NETFLIX_RAW')
   cursor.execute('USE SCHEMA NETFLIX_RAW')
   print('SQL Connection Created')
   return cursor,conn

# This is the module to truncate the table if exists. We don't want to duplicate data
def truncate_table():
   cur,conn=create_connection()
   sql_titles = "TRUNCATE TABLE IF EXISTS TITLES"
   sql_credits = "TRUNCATE TABLE IF EXISTS CREDITS"
   cur.execute(sql_titles)
   cur.execute(sql_credits)
   print('Tables truncated')

#Module to read csv file and load data in Snowflake. Table is created dynamically
def load_data():
   cur,conn=create_connection()
   titles_file = r"./data/netflix_titles.csv" 
   titles_delimiter = "," 
   credits_file=r"./data/credits.csv"
   credits_delimiter=","

   titles_df = pd.read_csv(titles_file, sep = titles_delimiter)
   print("Titles file read")
   credits_df = pd.read_csv(credits_file, sep = titles_delimiter)
   print("Credits file read")

   write_pandas(conn, titles_df, "TITLES",auto_create_table=True)
   print('Titles file loaded')
   write_pandas(conn, credits_df, "CREDITS",auto_create_table=True)
   print('Credits file loaded')

   cur = conn.cursor()

   # Close your cursor and your connection.
   cur.close()
   conn.close()

print("Starting Script")
truncate_table()
load_data()



