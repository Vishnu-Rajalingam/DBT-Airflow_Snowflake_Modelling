import pandas as pd

# Snowflake connector libraries
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas
from access_keys_with_service_principle import load_secret 


#Module to create the snowflake connection and return the connection objects
def create_connection():

   ##########-------------- Securely Access the snowflake Credentials from a cloud Key Store. This is could any Online Vault
   # hsqvdpz-ug08744
   snowflake_account = load_secret("snowflake-account")
   snowflake_username = load_secret("snowflake-username")
   snowflake_password = load_secret("snowflake-password")

   print(snowflake_account)

   conn = snow.connect(
      user = snowflake_username,
      password = snowflake_password,
      account = snowflake_account,
      warehouse="COMPUTE_WH",
      database="NETFLIX",
      schema="NETFLIX_RAW"
   )
   cursor = conn.cursor()
   ##########-------------- Create a Database
   cursor.execute('CREATE DATABASE IF NOT EXISTS NETFLIX')

   ##########-------------- Create a Database
   cursor.execute('USE DATABASE NETFLIX')

   ##########-------------- Create a Schema Within the Database
   cursor.execute('CREATE SCHEMA IF NOT EXISTS NETFLIX_RAW')

   ##########-------------- Change to the newly created Schema
   cursor.execute('USE SCHEMA NETFLIX_RAW')

   ##########-------------- Now this newly created Database, Schema need a user who can Do the transformations in DBT
   ##########-------------- We also need a dedicated role for this USER
   ##########-------------- So we will create a new user and grant him transforamtion  rights

   cursor.execute('USE ROLE ACCOUNTADMIN')
   cursor.execute('CREATE ROLE IF NOT EXISTS transform')
   cursor.execute('USE ROLE ACCOUNTADMIN')
   cursor.execute('GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN')
   cursor.execute('GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM')

   cursor.execute('''
      CREATE USER IF NOT EXISTS dbt
         PASSWORD='Sanguine@93'
         LOGIN_NAME='dbt'
         MUST_CHANGE_PASSWORD=FALSE
         DEFAULT_WAREHOUSE='COMPUTE_WH'
         DEFAULT_ROLE='transform'
         DEFAULT_NAMESPACE='NETFLIX.NETFLIX_RAW'
   ''')
   cursor.execute('GRANT ROLE TRANSFORM to USER dbt')

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



