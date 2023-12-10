from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, IntegerType
from pyspark.sql.types import StringType, FloatType, TimestampType, MapType
import uuid
import os
import time
import logging

import utils.sfhelperfnc as sfhelp
import utils.sfkeysfnc as sfkey

#====================================================================
# load env. var. from .env file
#====================================================================
from dotenv import load_dotenv
load_dotenv()

#====================================================================
# getting snowflake vars. from .env file
#====================================================================
#snowflakes tokens
sf_account              = os.getenv('sf_account')            
sf_url                  = os.getenv('sf_url')       
sf_database             = os.getenv('sf_database')  
sf_warehouse            = os.getenv('sf_warehouse')  
sf_schema               = os.getenv('sf_schema') 
sf_table                = os.getenv('sf_table_testDF')  
sf_role                 = os.getenv('sf_role')   
sf_int_stage            = os.getenv('sf_int_stage')    
sf_username             = os.getenv('sf_username')   
sf_password             = os.getenv('sf_password') 
#sf_pem_private_key      = os.getenv('sf_pem_private_key') 
#sf_pem_public_key       = os.getenv('sf_pem_public_key') 

if __name__ == "__main__":
#====================================================================
#Snowflake Env Variables print out
#====================================================================
    demarcator = '=' * 35
    print(demarcator)
    print("Snowflake Environmental Variables ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    print(f'Snowflake Environmental Variable listings:')
    print(demarcator)
    print(f'Snowflake account: {sf_account}')
    print(f'Snowflake Url: {sf_url}')
    print(f'Snowflake Database: {sf_database}')
    print(f'Snowflake Warehouse: {sf_warehouse}')
    print(f'Snowflake Schema: {sf_schema}')
    print(f'Snowflake Table: {sf_table}')
    print(f'Snowflake Role: {sf_role}')
    print(f'Snowflake Stage: {sf_int_stage}')
    print(f'Snowflake Username: {sf_username}')
    print(f'Snowflake Password: {sf_password}')
    print(demarcator)

#====================================================================
#Setup Spark Session
#====================================================================
    import os
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 pyspark-shell'
    ## Set up the Spark session with config for both kafka and snowflake
    spark = SparkSession \
        .builder \
        .appName("D2B Streaming App") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
        .config("spark.jar.packages",  "org.apache.spark:spark-avro_2.12:3.4.0") \
        .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.29") \
        .config("spark.jars.packages", "net.snowflake:snowflake_2.12:2.12.0-spark_3.4") \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.13.0-spark_3.4") \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("local[*]") \
        .getOrCreate()   

#====================================================================
#Spark Context setup
#====================================================================    
    # Spark Context created internlly
    print(spark.sparkContext)
    print("Spark App Name : "+ spark.sparkContext.appName)

    # Reduce logging
    #spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

#====================================================================
#Create Df
# ====================================================================
    # Define schema for DataFrame
    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
        StructField('age', IntegerType(), True),
        StructField('city', StringType(), True)
    ])

    # Load data into DataFrame
    data = [
        (1, 'John', 25, 'New York'),
        (2, 'Jane', 30, 'San Francisco'),
        (3, 'Bob', 40, 'Chicago')
    ]
    
    df = spark.createDataFrame(data, schema)

#====================================================================
#Establish Snowflake connection and prep up snowflake for spark input
#====================================================================
    tble_strg_1 = 'id integer, name varchar, age integer, city varchar'
    tble_map_lst = [tble_strg_1]
    tble_nme_lst = [df]
    conn = sfhelp.sf_snowflake_for_spark_setup(tble_nme_lst, tble_map_lst)

#====================================================================
#Provide Snowflake Connection Details/options
#====================================================================
    #sf_pem_private_key = sfkey.sf_get_private_key_uncrypted()
    sf_pem_private_key = '/opt/spark-app/rsa_key.p8'
    # Snowflake connection parameters
    # Replace the placeholders with your Snowflake connection details
    snowflake_options = {
        "sfURL":        sf_url,
        "sfDatabase":   sf_database,
        "sfWarehouse":  sf_warehouse,
        "sfSchema":     sf_schema,
        "sfRole":       sf_role,
        "sfUser":   sf_username,
        "pem_private_key": sf_pem_private_key,
        "tracing" : "all",
        "sfPassword":   sf_password
        }
    
#====================================================================
#Write to console: just testing to see visuals of stream
#====================================================================
#    console_df = df \
#    .writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .start()
#
#    console_df.awaitTermination()
#

#====================================================================
#send df to snowflake
#====================================================================
# Write DataFrame to Snowflake table
    #df.write \
    #    .format('snowflake') \
    #    .option('dbtable', 'table_name') \
    #    #.option('user', os.environ['SNOWFLAKE_USER']) \
    #    #.option('password', os.environ['SNOWFLAKE_PASSWORD']) \
    #    #.option('account', os.environ['SNOWFLAKE_ACCOUNT']) \
    #    #.option('database', os.environ['SNOWFLAKE_DATABASE']) \
    #    #.option('schema', os.environ['SNOWFLAKE_SCHEMA']) \
    #    #.option('warehouse', os.environ['SNOWFLAKE_WAREHOUSE']) \
    #    .mode('overwrite') \
    #    .save()

    df.write\
        .format("net.snowflake.spark.snowflake")\
        .options(**snowflake_options)\
        .option("dbtable", sf_table)\
        .mode('append')\
        .save()    

#====================================================================
#Read from Snowflake Table
#====================================================================
    # Read the data from the Snowflake table
    #df_from_snowflake = spark.read \
    #    .format('snowflake') \
    #    .option('dbtable', 'table_name') \
    #    .option('user', os.environ['SNOWFLAKE_USER']) \
    #    .option('password', os.environ['SNOWFLAKE_PASSWORD']) \
    #    .option('account', os.environ['SNOWFLAKE_ACCOUNT']) \
    #    .option('database', os.environ['SNOWFLAKE_DATABASE']) \
    #    .option('schema', os.environ['SNOWFLAKE_SCHEMA']) \
    #    .option('warehouse', os.environ['SNOWFLAKE_WAREHOUSE']) \
    #    .load()

    df_from_snowflake = spark.read \
        .format("net.snowflake.spark.snowflake")\
        .option('dbtable', 'sf_table') \
        .options(**snowflake_options)\
        .load()

    # Show the data from the Snowflake table
    df_from_snowflake.show()
 
#====================================================================
# Stop the Spark session
#====================================================================
    spark.stop()

    print("Test Data Processing Application Completed.")
    