#!/usr/bin/env python
import snowflake.connector
from collections import OrderedDict
from dotenv import load_dotenv
load_dotenv()
import os


#====================================================================
# Helper Functions
#====================================================================
#print env variables
def print_env(search_string):
    #list stored variables
    print(f'Environment variables containing {search_string}:')
    keylst =os.environ.keys()
    tokens_exist = 0
    for k, v in os.environ.items():
        if search_string in k.lower():
            print(f'{k}={v}')
        else:
            tokens_exist += 1
    if tokens_exist == 0:
        print('no env tokens set')

def sf_get_conn_ver():
    # Gets the version
    ctx = snowflake.connector.connect(\
        user=os.getenv('sf_username'),\
        password=os.getenv('sf_password'),\
        account=os.getenv('sf_account')\
        )
    cs = ctx.cursor()
    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(f"Current Version: {one_row[0]}")
    finally:
        cs.close()
    ctx.close()
    
def sf_conn_snowflake():   
    conn = snowflake.connector.connect(\
        user=os.getenv('sf_username'),\
        password=os.getenv('sf_password'),\
        account=os.getenv('sf_account')\
        )
    return conn

def sf_unset_user_pub_key(conn):
    user=os.getenv('sf_username')
    execute_comm = f"ALTER USER {user} UNSET RSA_PUBLIC_KEY;"
    print(f'executing removing public key from user: {execute_comm}')
    conn.cursor().execute(execute_comm)

def sf_set_user_pub_key(conn):
    sf_role = os.getenv('sf_role')
    execute_comm = f'use role {sf_role};'
    conn.cursor().execute(execute_comm)
    
    sf_unset_user_pub_key(conn)
    
    user_public_key = os.getenv('sf_pub_key')
    user=os.getenv('sf_username')
    execute_comm = f"ALTER USER {user} SET RSA_PUBLIC_KEY={user_public_key};"
    print(f'executing adding public key to user: {execute_comm}')
    conn.cursor().execute(execute_comm)


def sf_create_warehouse(conn, wh_name):
    execute_comm = f"CREATE WAREHOUSE IF NOT EXISTS {wh_name}"
    print(f'executing init warehouse: {execute_comm}')
    conn.cursor().execute(execute_comm)

def sf_use_warehouse(conn, wh_name):
    execute_comm = f"USE WAREHOUSE {wh_name}" 
    print(f'executing use warehouse: {execute_comm}')
    conn.cursor().execute(execute_comm)

def sf_create_database(conn, db_name):
    execute_comm = f"CREATE DATABASE IF NOT EXISTS {db_name}"    
    print(f'executing init db: {execute_comm}')
    conn.cursor().execute(execute_comm)

def sf_use_database(conn, db_name):
    execute_comm = f"USE DATABASE {db_name}" 
    print(f'executing use db: {execute_comm}')
    conn.cursor().execute(execute_comm)
    
def sf_create_schema(conn, schma_name):
    execute_comm = f"CREATE SCHEMA IF NOT EXISTS {schma_name}"    
    print(f'executing init schema: {execute_comm}')
    conn.cursor().execute(execute_comm)

def sf_use_schema(conn, db_name, schma_name):
    execute_comm = f"USE SCHEMA {db_name}.{schma_name}" 
    print(f'executing use schema: {execute_comm}')
    conn.cursor().execute(execute_comm)

def sf_create_table(conn, table_name, table_strg):
    # conn.cursor().execute("CREATE OR REPLACE TABLE "  "test_table(col1 integer, col2 string, col3 integer)")
    execute_comm_1 = f'table_name ({table_strg})'
    execute_comm = f'CREATE OR REPLACE TABLE {execute_comm_1}'   
    print(f'executing init table: {execute_comm}')
    conn.cursor().execute(execute_comm)

def sf_insert_in_table(tble_nme, col_lst, val_lst):
    #"INSERT INTO test_table(col1, col2) " "VALUES(123, 'test string1'),(456, 'test string2')")
    # col_lst = = [col1 col2, col3]
    # val_lst = [[val1a val1b val1c], [val2a val2b val2c]]
    if  len(col_lst) == len(val_lst):
        col_strg = get_col_strg(col_lst)
        val_strg = get_val_strg(val_lst)
        execute_comm = f'"INSERT INTO {tble_nme}({col_strg}) " " "VALUES{val_strg}"'

def get_col_strg(col_lst):
    #col_lst = [col1 col2, col3]
    #end result = (col1, col2, col3)
    fnal_strg = ''
    lst_len = len(col_lst)
    if lst_len == 1:
        fnal_strg = f'{col_lst[0]}'
    elif lst_len > 1:
        for n,col in col_lst:
            if n == 1:
                fnal_strg = f'{col_lst[0]}'
            else:
                fnal_strg = fnal_strg + f',{col}'
    if fnal_strg != '':
        fnal_strg = f'({fnal_strg})'
    else:
        print('invalid nos of item')      
    
    
def get_val_strg(val_lst):
    #val_lst = [[val1a val1b val1c], [val2a val2b val2c]]
    #end result = (val1a, val1b, val1c), (val2a, val2b, val2c)
    for val_arr in val_lst:
        val_strg = ''
        for val in val_arr:
            val_strg = val_strg + f',{val}'

def sf_create_stage(conn, sf_int_stage):
    execute_comm = f"CREATE OR REPLACE STAGE {sf_int_stage} COPY_OPTIONS = (ON_ERROR='skip_file');" 
    print(f'executing use schema: {execute_comm}')
    conn.cursor().execute(execute_comm)    


def sf_snowflake_for_spark_setup(tble_nme_lst, tble_map_lst):
    print_env('sf')
    
    sf_get_conn_ver()
    conn = sf_conn_snowflake()
    #sf_set_user_pub_key(conn) need to do this manually in snowflake
    
    sf_warehouse = os.getenv('sf_warehouse')
    sf_create_warehouse(conn, sf_warehouse)
    sf_use_warehouse(conn, sf_warehouse)
    
    sf_database = os.getenv('sf_database')
    sf_create_database(conn, sf_database)
    sf_use_database(conn, sf_database)
    
    sf_schema = os.getenv('sf_schema')
    sf_create_schema(conn, sf_schema)
    sf_use_schema(conn, sf_database, sf_schema)
    
    if len(tble_nme_lst) == len(tble_map_lst):
        for cnt, tablename in enumerate(tble_nme_lst):
            sf_create_table(conn, tablename, tble_map_lst[cnt])
    
    sf_int_stage = os.getenv('sf_int_stage')
    sf_create_stage(conn, sf_int_stage)
    
    return conn
    