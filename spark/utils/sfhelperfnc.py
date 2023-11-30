
#!/usr/bin/env python
import snowflake.connector
from collections import OrderedDict
from dotenv import load_dotenv
load_dotenv()

def sf_get_conn_ver():
    # Gets the version
    ctx = snowflake.connector.connect(
        user=os.getenv('sf_user')',
        password=os.getenv('sf_password')',
        account=os.getenv('sf_username')'<your_account_name>'
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
        user='XXXX',
        password='XXXX',
        account='XXXX',
        session_parameters={
            'QUERY_TAG': 'EndOfMonthFinancials',
        }
    )
    return conn

def sf_create_warehouse(conn, wh_name):
    execute_comm = f"CREATE WAREHOUSE IF NOT EXISTS {wh_name}"
    conn.cursor().execute(execute_comm)

def sf_create_database(conn, db_name):
    execute_comm = f"CREATE DATABASE IF NOT EXISTS {db_name}"    
    conn.cursor().execute(execute_comm)
    
def sf_create_schema(conn, schma_name):
    execute_comm = f"CREATE SCHEMA  IF NOT EXISTS {schma_name}"    
    conn.cursor().execute(execute_comm)

def sf_create_table(conn, table_name, tble_lst):
    # conn.cursor().execute("CREATE OR REPLACE TABLE "  "test_table(col1 integer, col2 string)")
    execute_comm_1 = f'table_name({create_table_strg(tble_lst)})'
    execute_comm = f'"CREATE OR REPLACE TABLE" "{execute_comm_1}"   
    conn.cursor().execute(execute_comm)

def create_table_strg(tble_lst): 
    # structure of tble_lst is [[col1, type_integer], [col2, type_string], [col3, type_string]]
    fnal_strg = ''
    for n,item_l1 in enumerate(tble_lst):
        nwstring = get_table_col(item_l1)
        if n == 0:
            fnal_strg = get_table_col(item_l1)
        else:
            fnal_strg = fnal_strg + ',' + get_table_col(item_l1)
    if fnal_strg != '':
        return fnal_strg
    else:
        print('invalid string')

def get_table_col(lst_2items): 
    # final result "col1 integer"
    chk_cols_internal_n = len(lst_2items) 
    if chk_cols_internal_n == 2:  
        if chk_col_type(lst_2items[1]):    
            nwstring = f'{item_l2[0]} {item_l2[1]}'
            return nwstring 
        else:
            print('invalid column type')    
    else:
        print('invalid nos of item')      

def chk_col_type(col_type):
    chk_pass = False
    if col_type.lower() == 'integer':
        chk_pass = True
    if col_type.lower() == 'string':
        chk_pass = True
    if chk_pass == False
        print('invalid column type')
    return chk_pass

def sf_insert_in_table(tble_nme, col_lst, val_lst):
    #"INSERT INTO test_table(col1, col2) " "VALUES(123, 'test string1'),(456, 'test string2')")
    # col_lst = [col]
    # val_lst = [values] 
    col_strg = get_col_strg(col_lst)
    val_strg = get_val_strg(val_lst)
    execute_comm = f'"INSERT INTO {tble_nme}({col_strg}) " " "VALUES{val_strg}")'

def get_col_strg():
    pass
    
def get_val_strg():
    pass