
#!/usr/bin/env python
import snowflake.connector
from collections import OrderedDict
from dotenv import load_dotenv
load_dotenv()

#====================================================================
# Helper Functions
#====================================================================
#print env variables
def print_env(search_string):
    #list stored variables
    print('Environment:')
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
    ctx = snowflake.connector.connect(
        user=os.getenv('sf_user'),
        password=os.getenv('sf_password'),
        account=os.getenv('sf_username')
        )
    cs = ctx.cursor()
    try:
        cs.execute("SELECT current_version()")
        one_row = cs.fetchone()
        print(one_row[0])
    finally:
        cs.close()
    ctx.close()
    
def sf_conn_snowflake():   
    conn = snowflake.connector.connect(
        user=os.getenv('sf_user'),
        password=os.getenv('sf_password'),
        account=os.getenv('sf_username')
    )
    return conn

def sf_create_warehouse(conn, wh_name):
    execute_comm = f"CREATE WAREHOUSE IF NOT EXISTS {wh_name}"
    print(f'executing: {execute_comm}')
    conn.cursor().execute(execute_comm)

def sf_create_database(conn, db_name):
    execute_comm = f"CREATE DATABASE IF NOT EXISTS {db_name}"    
    print(f'executing: {execute_comm}')
    conn.cursor().execute(execute_comm)
    
def sf_create_schema(conn, schma_name):
    execute_comm = f"CREATE SCHEMA  IF NOT EXISTS {schma_name}"    
    print(f'executing: {execute_comm}')
    conn.cursor().execute(execute_comm)

def sf_create_table(conn, table_name, tble_lst):
    # conn.cursor().execute("CREATE OR REPLACE TABLE "  "test_table(col1 integer, col2 string, col3 integer)")
    execute_comm_1 = f'table_name({create_table_strg(tble_lst)})'
    execute_comm = f'"CREATE OR REPLACE TABLE" "{execute_comm_1}"'   
    print(f'executing: {execute_comm}')
    conn.cursor().execute(execute_comm)

def create_table_strg(tble_lst): 
    #input: structure of tble_lst is [[col1, type_integer], [col2, type_string], [col3, type_string]]
    #result col1 integer, col2 string, col3 integer
    fnal_strg = ''
    if len(tble_lst) >= 1:
        for n,item in enumerate(tble_lst):
            nwstring = get_table_col(item,2)
            if n == 0:
                fnal_strg = get_table_col(item,2)
            else:
                fnal_strg = fnal_strg + ',' + get_table_col(item,2)
        if fnal_strg != '':
            return fnal_strg
        else:
            print('invalid string')

def get_table_col(lst_items, nos = 2): 
    #input: [col1, type_integer]
    # result: "col1 integer"
    chk_cols_internal_n = len(lst_items) 
    if chk_cols_internal_n == nos:  
        if chk_cols_internal_n >= 2:
            if chk_col_type(lst_items[1]):    
                for n,item in lst_items:
                    if n == 1:
                        nwstring = f'{item}'
                    else:
                        nwstring = nwstring + f' {item}'
                    return nwstring 
            else:
                print('invalid column type') 
        else:
            print('invalid nos of items in list') 
    else:
        print(f'invalid nos of item: should be {nos}')      

def chk_col_type(col_type):
    chk_pass = False
    if col_type.lower() in ['integer', 'string']:
        chk_pass = True
    if chk_pass == False:
        print('invalid column type')
    return chk_pass

def sf_insert_in_table(tble_nme, col_lst, val_lst):
    #"INSERT INTO test_table(col1, col2) " "VALUES(123, 'test string1'),(456, 'test string2')")
    # col_lst = [col]
    # val_lst = [values]
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

def sf_snowflake_for_spark_setup():
    print_env('sf')
    
    sf_get_conn_ver()
    conn = sf_conn_snowflake()
    
    sf_warehouse = os.getenv('sf_warehouse')
    sf_create_warehouse(conn, sf_warehouse)
    
    sf_database = os.getenv('sf_database')
    sf_create_database(conn, sf_database)
    
    sf_schema = os.getenv('sf_schema')
    sf_create_schema(conn, sf_schema)
    
    
    #sf_create_table(conn, table_name, tble_lst)
    return conn
    