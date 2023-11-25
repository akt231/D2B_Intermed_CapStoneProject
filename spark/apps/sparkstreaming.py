from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col
import os
import time

kafka_topic_name = os.getenv('d2b_token_finnhubio')

kafka_server = os.getenv('d2b_kafka_server')
kafka_port = os.getenv('d2b_kafka_port')
kafka_bootstrap_servers = f'{kafka_server}:{kafka_port}'

if __name__ == "__main__":
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession\
        .builder\
        .appName("Structured_Streaming")\
        .master("spark://sparkm:7077")\
        .getOrCreate()
        
    # Set Config
    # spark.conf.set("spark.sql.shuffle.partitions", "30")

    # Reduce logging
    #spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    trades_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \

        #.option("kafka.sasl.mechanism", "PLAIN")\
        #.option("kafka.security.protocol", "SASL_SSL")\
        #.option("kafka.sasl.jaas.config", EH_SASL)\
        #.option("kafka.request.timeout.ms", "60000")\
        #.option("kafka.session.timeout.ms", "60000")\
        #.option("failOnDataLoss", "true")\
        #.option("partition", 1) \
        #.option("kafka.group.id", "grp1") \   #this i am not sure of
        .load()



    print("Printing Schema of trades_df: ")
    trades_df.printSchema()

    trades_df1 = trades_df.select("value", "timestamp")

    # Define a schema for the orders data
    # Trade Conditions, price, symbol, timestamp, Volume, type of message
    trades_schema_avro = open('../finnhub_producer/schemas/schema_trades.avsc', mode='r').read()

    # {"c":null,"p":35112.76,"s":"BINANCE:BTCUSDT","t":1699311054111,"v":0.00126}],"type":"trade"}
    trades_df2 = trades_df1\
        .select(from_avro(col("value"), trades_schema_avro)\
        .alias("Trades"), "timestamp")

    trades_df3 = trades_df2.select("Trades.*", "timestamp")
    trades_df3.printSchema()

    # Simple aggregate - find total_volume_amount by grouping symbol
    trades_df4 = trades_df3.groupBy("s").\
                    agg(sum('v').alias('sum_volume'),
                    max('v').alias('max_volume'), \
                    min('v').alias('min_volume'), \
                    mean('v').alias('mean_volume'), \
                    count('v').alias('count_volume') )
        

    print("Printing Schema of trades_df4: ")
    trades_df4.printSchema()

    #define checkpoint directory 
    checkpointDir = 'C://Users//AKT//Downloads//projects.files//checkpoint'
    
    # Write final result into console for debugging purpose
    trades_agg_write_stream = trades_df4 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .option("checkpointLocation", checkpointDir) \
        .format("console") \
        .start()

    trades_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")