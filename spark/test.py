from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
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

print(f'scorched earth alaysin hell:{sf_table}')

if __name__ == "__main__":
#====================================================================
#Setup Spark Session
#====================================================================
    sc = SparkContext("local", "Simple App")
    spark = SQLContext(sc)
    spark_conf = SparkConf().setMaster('local').setAppName('ShitApp')
    
    #spark = SparkSession \
    #    .builder \
    #    .appName("D2B Testing App") \
    #    .config("spark.jars.packages", "net.snowflake:snowflake-jdbc:3.13.29") \
    #    .config("spark.jars.packages", "net.snowflake:snowflake_2.12:2.12.0-spark_3.4") \
    #    .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.13.0-spark_3.4") \
    #    .config("spark.sql.shuffle.partitions", 4) \
    #    .master("local[*]") \
    #    .getOrCreate()   

#====================================================================
#Spark Context setup
#====================================================================    
    # Spark Context created internlly
    print(f'Spark Context: {spark.sparkContext}')
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
    tble_nme_lst = [sf_table]
    tble_strg_1 = 'id integer, name varchar, age integer, city varchar'
    tble_map_lst = [tble_strg_1]
    print(f'shithead shit: {sf_table}')
    sfhelp.print_strg('table name list', tble_nme_lst)
    sfhelp.print_strg('table mapping', tble_map_lst)
    conn = sfhelp.sf_snowflake_for_spark_setup(tble_nme_lst, tble_map_lst)

#====================================================================
#Provide Snowflake Connection Details/options
#====================================================================
    #sf_pem_private_key = sfkey.sf_get_private_key_uncrypted()
    sf_pem_private_key = '/opt/spark-app/rsa_key.p8'
    # Snowflake connection parameters
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

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"    
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
    