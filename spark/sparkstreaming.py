from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, IntegerType
from pyspark.sql.types import StringType, FloatType, TimestampType
import uuid
import os
import time

import utils.sfhelperfnc as sf
#from utils.sfhelperfnc import sf_get_conn_ver, sf_conn_snowflake, sf_create_warehouse, sf_create_database, sf_create_schema, sf_create_table 

#====================================================================
# load env. var. from .env file
#====================================================================
from dotenv import load_dotenv
load_dotenv()

#====================================================================
# getting kafka vars. from .env file
#====================================================================
#kafka tokens
kafka_topic_name = os.getenv('d2b_kafka_producer_topic')
kafka_server = os.getenv('d2b_kafka_server')
kafka_port = os.getenv('d2b_kafka_port')
kafka_bootstrap_servers = f'{kafka_server}:{kafka_port}'
  
#====================================================================
# getting snowflake vars. from .env file
#====================================================================
#snowflakes tokens
sf_account =    os.getenv('sf_account')            
sf_url  =       os.getenv('sf_url')       
sf_database =   os.getenv('sf_database')  
sf_warehouse =  os.getenv('sf_warehouse')  
sf_schema =     os.getenv('sf_schema') 
sf_table_1 =     os.getenv('sf_table_1')  
sf_table_2 =     os.getenv('sf_table_2')    
sf_role =       os.getenv('sf_role')       
sf_username =   os.getenv('sf_username')   
sf_password =   os.getenv('sf_password') 



if __name__ == "__main__":
#====================================================================
#Kafka Env Variables print out
#====================================================================
    demarcator = '=' * 35
    print(demarcator)
    print("Kafka Environmental Variables ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    print(f'Kafka Environmental Variable listings:')
    print(demarcator)
    print(f'kafka_topic_name: {kafka_topic_name}')
    print(f'kafka_server: {kafka_server}')
    print(f'kafka_port: {kafka_port}')
    print(f'kafka_bootstrap_servers: {kafka_bootstrap_servers}')
    print(demarcator)

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
    print(f'Snowflake Table 01: {sf_table_1}')
    print(f'Snowflake Table 02: {sf_table_2}')
    print(f'Snowflake Role: {sf_role}')
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
        .config("spark.sql.shuffle.partitions", 4) \
        .master("local[*]") \
        .getOrCreate()   
#change spark version to 3.4 because of snowflakes

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
#Read in Kafka Stream
#====================================================================
    # Construct a streaming DataFrame that reads from trades-topic
    df_finnhub = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .option("includeHeaders", "true") \
        .load()
    print("Printing Schema of df_finnhub: ")
    df_finnhub.printSchema()

    # is data streaming?
    print(f'data streaming right now?:{df_finnhub.isStreaming}')

#====================================================================
#Cleanup Dataframe from Kafka Stream
#====================================================================
    # Define a schema for the orders data
    # Trade Conditions, price, symbol, timestamp, Volume, type of message
    # explode the data from Avro
    trades_schema = open('/opt/spark-schemas/schema_trades.avsc', mode='r').read()
    df_expanded = df_finnhub\
        .withColumn("avroData",from_avro(F.col("value"),trades_schema))\
        .select("avroData.*")\
        .select(F.explode("data"),"type")\
        .select("col.*")
    print("Printing Schema of df_expanded: ")
    df_expanded.printSchema()  
 
    # rename columns and add proper timestamps
    df_final = df_expanded\
        .withColumn("uuid", F.expr("uuid()")) \
        .withColumnRenamed("c", "trade_conditions")\
        .withColumnRenamed("p", "price")\
        .withColumnRenamed("s", "symbol")\
        .withColumnRenamed("t","trade_timestamp")\
        .withColumnRenamed("v", "volume")\
        .withColumn("trade_timestamp", (F.col("trade_timestamp") / 1000).cast("timestamp")) \
        .withColumn("ingest_timestamp", F.current_timestamp().cast(StringType()))
    
    print("Printing Schema of df_final: ")
    df_final.printSchema()        

#====================================================================
#Transform Dataframe 
#====================================================================
    # Another DataFrame with aggregates - running averages from last 15 seconds
    summaryDF = df_final \
        .withColumn("price_volume_multiply", F.col("price") * F.col("volume")) \
        .withWatermark("trade_timestamp", "15 seconds") \
        .groupBy("symbol", F.window("trade_timestamp", "15 seconds")) \
        .agg(F.avg("price_volume_multiply").alias("avg_price_volume_multiply"))

    # Rename columns in the DataFrame and add UUIDs before inserting into Cassandra
    finalSummaryDF = summaryDF \
        .withColumn("uuid", F.expr("uuid()")) \
        .withColumn("ingest_timestamp", F.current_timestamp()) \
        .withColumnRenamed("avg_price_volume_multiply", "price_volume_multiply")


           
#====================================================================
#set checkpoint directory
#====================================================================
    #checkpointdir = '/opt/spark-chkpoint'
    #spark.sparkContext.setCheckpointDir(checkpointdir)
    
#====================================================================
#Establish Snowflake connection and prep up snowflake for spark input
#====================================================================
    conn = sf.sf_snowflake_for_spark_setup()
    
#====================================================================
#Provide Snowflake Connection Details/options
#====================================================================
# Snowflake connection parameters
    sfparams = {
      "sfURL" : sf_url,
      "sfUser" : sf_username,
      "sfPassword" : sf_password,
      "sfDatabase" : sf_database,
      "sfSchema" : sf_schema,
      "sfWarehouse" : sf_warehouse
    }

    # Replace the placeholders with your Snowflake connection details
    snowflake_options = {
        "sfURL":        sf_url,
        "sfDatabase":   sf_database,
        "sfWarehouse":  sf_warehouse,
        "sfSchema":     sf_schema,
        "sfRole":       sf_role,
        "sfUsername":   sf_username,
        "sfPassword":   sf_password
    }

#====================================================================
#Write to Snowflake Table
#====================================================================
    #UNFINISHED********************************************************************
    # Write df_final as a stream to Snowflake
    query_finalDF = df_final.writeStream \
        .outputMode("append") \
        .format("net.snowflake.spark.snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", sf_table_1) \
        .option("checkpointLocation", f'{checkpointdir}/finalDF') \
        .start()

    #UNFINISHED********************************************************************
    # Write finalSummaryDF as a stream to Snowflake
    query_finalSummaryDF = finalSummaryDF.writeStream \
        .outputMode("append") \
        .format("net.snowflake.spark.snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", sf_table_2) \
        .option("checkpointLocation", f'{checkpointdir}/finalSummaryDF') \
        .start()

#====================================================================
#terminate computations
#====================================================================
    # Await termination of the streaming query
    query_finalDF.awaitTermination()
    query_finalSummaryDF.awaitTermination()

    # Stop the Spark session
    spark.stop()

    print("Stream Data Processing Application Completed.")
    
#Replace the placeholders (<your_snowflake_url>, <your_database>, <your_warehouse>, <your_schema>, <your_role>, <your_username>, <your_password>, your_target_table, path/to/your/streaming/data, path/to/checkpoint/dir) with your Snowflake connection details, the specific target table you want to write to, the path to your streaming data source (e.g., a folder containing CSV files), and the checkpoint directory.
#Make sure to replace "net.snowflake:snowflake-jdbc:<version>" with the appropriate version of the Snowflake JDBC driver that you are using.
#Note: Structured Streaming requires a checkpoint location to be specified. It is used to store the metadata and state information for fault-tolerance. Ensure that the specified checkpoint directory is a durable storage location.