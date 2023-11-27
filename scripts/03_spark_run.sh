/opt/bitnami/spark/bin/spark-submit \
  --master spark://172.18.0.5:7077 \
  --deploy-mode cluster \
    
    #--conf "spark.sql.shuffle.partitions=20000" \
    ##--conf "spark.executor.memoryOverhead=5244" \ #not supported in pyspark apps
    #--conf "spark.memory.fraction=0.8" \
    #--conf "spark.memory.storageFraction=0.2" \
    #--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    #--conf "spark.sql.files.maxPartitionBytes=168435456" \
    #--conf "spark.dynamicAllocation.minExecutors=1" \
    #--conf "spark.dynamicAllocation.maxExecutors=200" \
    #--conf "spark.dynamicAllocation.enabled=true" \
    #--conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \ 

#  --driver-memory 2g \
#  --executor-memory 2g \
#  --executor-cores 2  \
#  --jars  <comma separated dependencies>
#  --class <main-class> \
#    <application-jar> \
#  [application-arguments]
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 
  /opt/spark-apps/sparkstreaming.py

#run consumer
docker exec -it sparkm bash
/opt/bitnami/spark/bin/spark-submit \
  --master spark://172.18.0.5:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0, org.apache.spark:spark-avro_2.12:3.5.0
  /opt/spark-python-config/sparkstreaming.py


docker-compose exec sparkm spark-submit --master spark://172.18.0.5:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0,org.apache.spark:spark-avro_2.12:3.5.0 /opt/spark-python-config/sparkstreaming.py
