import os
import pandas as pd
from pandas.io.sql import DatabaseError
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import unicodedata
import time
import sys
from openpyxl import load_workbook
import psycopg2
import unicodedata
import glob
import sys
import psycopg2.extras as extras
from io import StringIO
import csv
from pandas.io.sql import DatabaseError


# Save your database, username & password
params_dic = {
    "host"      : "abc123",
    "database"  : "abc123",
    "user"      : "abc123",
    "password"  : "abc123"
}


def connect(params_dic):
    conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1)
    print('Connection Successful')
    return(conn)


def query_DB_as_df(conn, table):
    #Retrieve all data from the given table
    query = f"""SELECT * FROM {table};"""
    try:
        df = pd.read_sql_query(query,conn)
        return(df)
    except DatabaseError as e:
        print(f'Query failed!\n\n{e}')
        return 1


def custom_query_DB_as_df(conn, query):
    #use customized query to retrieve data from DB
    try:
        df = pd.read_sql_query(query,conn)
        return(df)
    except DatabaseError as e:
        print(f'Query failed!\n\n{e}')
        return 1


def copy_from_stringio(conn, df, table):
    """
    Here we are going save to the dataframe on disk as 
    a csv file, load the csv file  
    and use copy_expert() to copy it to the table
    """
    # copy_from_file method only allows INSERT but not UPSERT
    # One of the faster methods for bulk inserting to Postgresql DB
    # For Tansactional data tables
    
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        sql = f"""COPY {table} ({','.join(list(df.columns))}) FROM STDIN WITH (FORMAT CSV)"""
        #The join(list(df.columns)) statement is used to create string of comma separated column headers without the SERIAL column
        #since SERIAL is auto incremented in postgres
        cursor.copy_expert(sql,buffer)
        #cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()


if __name__=='__main__':
    connect(params_dic)