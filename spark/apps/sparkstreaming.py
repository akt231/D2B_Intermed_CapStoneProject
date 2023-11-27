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
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("local[*]") \
        .getOrCreate()        
    # Set Config
    # spark.conf.set("spark.sql.shuffle.partitions", "30")

    # Reduce logging
    #spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from trades-topic
    trades_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .option("includeHeaders", "true")\
        .load()

    trades_df.load().selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    print("Printing Schema of trades_df: ")
    trades_df.printSchema()

    trades_df1 = trades_df.select("value", "timestamp")
#
#    # Define a schema for the orders data
#    # Trade Conditions, price, symbol, timestamp, Volume, type of message
#    trades_schema_avro = open('../finnhub_producer/schemas/schema_trades.avsc', mode='r').read()
#
#    # {"c":null,"p":35112.76,"s":"BINANCE:BTCUSDT","t":1699311054111,"v":0.00126}],"type":"trade"}
#    trades_df2 = trades_df1\
#        .select(from_avro(col("value"), trades_schema_avro)\
#        .alias("Trades"), "timestamp")
#
#    trades_df3 = trades_df2.select("Trades.*", "timestamp")
#    trades_df3.printSchema()
#
#    # Simple aggregate - find total_volume_amount by grouping symbol
#    trades_df4 = trades_df3.groupBy("s").\
#                    agg(sum('v').alias('sum_volume'),
#                    max('v').alias('max_volume'), \
#                    min('v').alias('min_volume'), \
#                    mean('v').alias('mean_volume'), \
#                    count('v').alias('count_volume') )
#        
#
#    print("Printing Schema of trades_df4: ")
#    trades_df4.printSchema()
#
#    #define checkpoint directory 
#    checkpointDir = 'C://Users//AKT//Downloads//projects.files//checkpoint'
#    
#    # Write final result into console for debugging purpose
#    trades_agg_write_stream = trades_df4 \
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