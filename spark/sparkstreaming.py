from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
from pyspark.sql.types import StructField, StructType, IntegerType, \
    StringType, FloatType, TimestampType
import os
import time

# getting tokens from .env file
from dotenv import load_dotenv
load_dotenv()
kafka_topic_name = os.getenv('d2b_kafka_producer_topic')
kafka_server = os.getenv('d2b_kafka_server')
kafka_port = os.getenv('d2b_kafka_port')
kafka_bootstrap_servers = f'{kafka_server}:{kafka_port}'

if __name__ == "__main__":
    demarcator = '=' * 35
    print(demarcator)
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    print(f'Kafka Environmental Variable listings:')
    print(demarcator)
    print(f'kafka_topic_name: {kafka_topic_name}')
    print(f'kafka_server: {kafka_server}')
    print(f'kafka_port: {kafka_port}')
    print(f'kafka_bootstrap_servers: {kafka_bootstrap_servers}')
    print(demarcator)

    spark = SparkSession \
        .builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("local[*]") \
        .getOrCreate()        

    # Spark Context created internlly
    print(spark.sparkContext)
    print("Spark App Name : "+ spark.sparkContext.appName)

    # Reduce logging
    #spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

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

    # Define a schema for the orders data
    # Trade Conditions, price, symbol, timestamp, Volume, type of message
    # explode the data from Avro
    trades_schema = open('/opt/spark-schemas/schema_trades.avsc', mode='r').read()
    df_expanded = df_finnhub\
        .withColumn("avroData",from_avro(col("value"),trades_schema))\
        .select("avroData.*")\
        .select(explode("data"),"type")\
        .select("col.*")
    print("Printing Schema of df_expanded: ")
    df_expanded.printSchema()  
    
    #df_selected = df_expanded\
    #    .select("")
    
    # rename columns and add proper timestamps
#    df_final = df_expanded\
#        .withColumn("uuid", makeUUID())\
#        .withColumnRenamed("c", "trade_conditions")\
#        .withColumnRenamed("p", "price")\
#        .withColumnRenamed("s", "symbol")\
#        .withColumnRenamed("t","trade_timestamp")\
#        .withColumnRenamed("v", "volume")\
#        .withColumn("trade_timestamp",(col("trade_timestamp") / 1000).cast("timestamp"))\
#        .withColumn("ingest_timestamp",current_timestamp().as("ingest_timestamp"))
#    
#    print("Printing Schema of df_final: ")
#    df_final.printSchema()        
            
            
    
#    trades_df1 = trades_df.select("value", "timestamp")
#    print("Printing Schema of trades_df transform 01: ")
#    trades_df1.printSchema()
#
#
#
#    # {"c":null,"p":35112.76,"s":"BINANCE:BTCUSDT","t":1699311054111,"v":0.00126}],"type":"trade"}
#    trades_df2 = trades_df1\
#        .select(from_avro(col("value"), trades_schema_avro)\
#        .alias("Trades"), "timestamp")
#    print("Printing Schema of trades_df transform 02: ")
#    trades_df2.printSchema()
#    
#    
#    trades_df3 = trades_df2.select("Trades.data.c", "Trades.data.p", "Trades.data.s",\
#                    "Trades.data.t", "Trades.data.v","timestamp")
#    print("Printing Schema of trades_df transform 03: ")
#    trades_df3.printSchema()
#    
#    trades_df4 = trades_df3.withColumnRenamed("c","Conditions") \
#                    .withColumnRenamed("p","price")\
#                    .withColumnRenamed("s","symbol")\
#                    .withColumnRenamed("t","timestamp")\
#                    .withColumnRenamed("v","volume")\
#    print("Printing Schema of trades_df transform 04: ")
#    trades_df4.printSchema()
#
#    # Simple aggregate - find total_volume_amount by grouping symbol
#    trades_df5 = trades_df4.groupBy("symbol").\
#                    agg(sum('volume').alias('sum_volume'),
#                    max('volume').alias('max_volume'), \
#                    min('volume').alias('min_volume'), \
#                    mean('volume').alias('mean_volume'), \
#                    count('volume').alias('count_volume') )
#        
#
#    print("Printing Schema of trades_df5: ")
#    trades_df5.printSchema()
#
#    #define checkpoint directory 
#    checkpointDir = '/opt/spark-chkpoint'
#    
#    # Write final result into console for debugging purpose
#    trades_agg_write_stream = trades_df5 \
#        .writeStream \
#        .trigger(processingTime='5 seconds') \
#        .outputMode("update") \
#        .option("truncate", "false")\
#        .option("checkpointLocation", checkpointDir) \
#        .format("console") \
#        .start()
#
#    trades_agg_write_stream.awaitTermination()
#
#    print("Stream Data Processing Application Completed.")