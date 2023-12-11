from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, IntegerType
from pyspark.sql.types import StringType, FloatType, TimestampType
#from pyspark.sql.streaming import trigger
from pyspark.conf import SparkConf
from datetime import datetime
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
sf_account                  = os.getenv('sf_account')            
sf_url                      = os.getenv('sf_url')       
sf_database                 = os.getenv('sf_database')  
sf_warehouse                = os.getenv('sf_warehouse')  
sf_schema                   = os.getenv('sf_schema') 
sf_table_detailedDFX        = os.getenv('sf_table_detailedDFX')  
sf_table_finalSummaryDFX    = os.getenv('sf_table_finalSummaryDFX')    
sf_role                     = os.getenv('sf_role')   
sf_int_stage                = os.getenv('sf_int_stage')    
sf_username                 = os.getenv('sf_username')   
sf_password                 = os.getenv('sf_password') 


if __name__ == "__main__":
#====================================================================
#Setup Spark Session
#====================================================================
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 pyspark-shell'
    ## Set up the Spark session with config for both kafka and snowflake
    conf = SparkConf()
    spark = SparkSession \
        .builder \
        .appName("D2B Streaming App") \
        .config(conf=conf)\
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
#Provide Snowflake Connection Details/options
#====================================================================
    pem_private_key_filepath = '/opt/spark-app/rsa_key.p8'
    sf_pem_private_key = sfkey.sf_get_private_key_uncrypted(pem_private_key_filepath)
    
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
#Establish Snowflake connection and prep up snowflake for spark input
#====================================================================
    tble_strg_1 = 'trade_conditions variant, price float, symbol varchar, trade_timestamp timestamp, volume float,uuid varchar,ingest_timestamp timestamp'
    tble_strg_2 = 'symbol varchar, trade_timestamp timestamp, price_volume_multiply float, uuid varchar,ingest_timestamp timestamp'

    tble_map_lst = [tble_strg_1, tble_strg_2]
    tble_nme_lst = [sf_table_detailedDFX, sf_table_finalSummaryDFX]
    conn = sfhelp.sf_snowflake_for_spark_setup(tble_nme_lst, tble_map_lst)
     
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
        .load()
    #print("Printing Schema of df_finnhub: ")
    #df_finnhub.printSchema()

    # is data streaming?
    print(f'kafka data streaming from finnhub right now?:{df_finnhub.isStreaming}')

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
        .select(F.col("col.*"))
    #print("Printing Schema of df_expanded: ")
    #df_expanded.printSchema()  
 
    # rename columns and add proper timestamps
    df_detailed = df_expanded\
        .withColumn("uuid", F.expr("uuid()")) \
        .withColumnRenamed("c", "trade_conditions") \
        .withColumnRenamed("p", "price")\
        .withColumnRenamed("s", "symbol")\
        .withColumnRenamed("t","trade_timestamp")\
        .withColumnRenamed("v", "volume")\
        .withColumn("trade_timestamp", (F.col("trade_timestamp") / 1000).cast(TimestampType())) \
        .withColumn("ingest_timestamp", F.current_timestamp().alias("ingest_timestamp"))
   
    print("Printing Schema of df_detailed: ")
    df_detailed.printSchema()        

    # Another dataframe with aggregates - running averages from the last 15 seconds
    summary_df_in_process = df_detailed \
        .withColumn("price_volume_multiply", F.col("price") * F.col("volume")) \
        .withWatermark("trade_timestamp", "15 seconds") \
        .groupBy("symbol", "trade_timestamp") \
        .agg(F.avg("price_volume_multiply").alias("price_volume_multiply"))

    # Rename columns in the dataframe and add UUIDs before inserting to Snowflake
    final_df = summary_df_in_process \
        .withColumn("uuid", F.expr("uuid()")) \
        .withColumn("ingest_timestamp", F.current_timestamp().alias("ingest_timestamp"))
    print("Printing Schema of final_df: ")
    final_df.printSchema() 
#====================================================================
#set checkpoint directory
#====================================================================
    checkpointdir = '/opt/spark-chkpoint'
    spark.conf.set("spark.sql.streaming.checkpointLocation", checkpointdir)
    #spark.sparkContext.setCheckpointDir(checkpointdir)
    
#====================================================================
#Write to Snowflake
#====================================================================
    def foreach_batch_stream(df, epoch_id):
           df.write\
                .format("net.snowflake.spark.snowflake")\
                .options(**snowflake_options)\
                .mode('append')\
                .save()    

    query = df_detailed.writeStream\
        .foreachBatch(foreach_batch_stream)\
        .option("dbtable", sf_table_detailedDFX) \
        .option("checkpointLocation",f"/opt/spark-chkpoint/detailedDF")\
        .outputMode("update")\
        .start()

    query2 = final_df.writeStream\
        .trigger(processingTime='10 seconds')\
        .foreachBatch(foreach_batch_stream)\
        .option("dbtable", sf_table_finalSummaryDFX) \
        .option("checkpointLocation",f"/opt/spark-chkpoint/finalDF")\
        .outputMode("update")\
        .start()

    
#    # Write query to Snowflake for trades
#    query = df_detailed \
#        .writeStream \
#        .foreachBatch(lambda batch_df, batch_id: batch_df.write \
#        .format("net.snowflake.spark.snowflake") \
#        .options(**snowflake_options) \
#        .option("dbtable", sf_table_detailedDFX) \
#        .mode("append") \
#        .save())
#
#    # Write second query to Snowflake for aggregates
#    query2 = final_df \
#        .writeStream \
#        .trigger(processingTime="5 seconds") \
#        .foreachBatch(lambda batch_df, batch_id: batch_df.write \
#        .format("net.snowflake.spark.snowflake") \
#        .options(**snowflake_options) \
#        .option("dbtable", sf_table_finalSummaryDFX) \
#        .mode("append") \
#        .save())

#====================================================================
#Write to console: just testing to see visuals of stream
#====================================================================
#    console_detailedDF = df_detailed \
#    .writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .start()
#    
#
#    console_summaryDF = final_df \
#    .writeStream \
#    .outputMode("append") \
#    .format("console") \
#    .start()



#====================================================================
#Terminate Streaming
#====================================================================
    # Let queries await termination
    spark.streams.awaitAnyTermination()
    


   
