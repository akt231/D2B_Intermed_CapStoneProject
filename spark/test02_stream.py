from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro

spark = SparkSession.builder.appName("KafkaStreamToRDD") \
    .getOrCreate()

spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "trades") \
    .option("startingOffsets", "earliest") \
    .load()