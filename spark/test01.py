from pyspark.sql import SparkSession

# Create a SparkSession with master and worker config
#spark = SparkSession.builder.appName("Test") \
#    .master("spark://spark-master:7077") \
#    .config("spark.executor.memory", "1g") \
#    .config("spark.executor.cores", "1") \
#    .config("spark.driver.memory", "1g") \
#    .config("spark.driver.host", "spark-jupyterlab") \
#    .getOrCreate()

spark = SparkSession \
        .builder \
        .appName("TestApp") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0') \
        .config("spark.sql.shuffle.partitions", 4) \
        .master("local[*]") \
        .getOrCreate()   



# Create a DataFrame with some example data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame
df.show()

# Group by age and count the number of names
result = df.groupBy("Age").count()

# Show the result
result.show()

# Stop the SparkSession
spark.stop()