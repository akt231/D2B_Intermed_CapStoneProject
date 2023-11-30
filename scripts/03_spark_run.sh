#run consumer option1
docker exec -it sparkm bash
/opt/bitnami/spark/bin/spark-submit \
  --master spark://172.18.0.5:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0, org.apache.spark:spark-avro_2.12:3.5.0
  /opt/spark-app/sparkstreaming.py


sudo docker cp  /home/akt/projects.files/projects.git/git.personal/D2B_Intermed_CapStoneProject/spark/start-spark.sh sparkm:/opt/spark-app/start-spark.sh


#copy files between wsl-linux and container
sudo docker cp sparkm:/opt/bitnami/spark/python/lib/pyspark.zip ~/downloads/pyspark.zip
==============================================================================
sudo docker cp /home/akt/projects.files/projects.git/git.personal/D2B_Intermed_CapStoneProject/spark/test01.py sparkm:/opt/spark-app/test01.py

#run consumer|option2|test app
docker-compose exec sparkm spark-submit --master spark://172.18.0.5:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 /opt/spark-app/test01.py
==============================================================================
sudo docker cp /home/akt/projects.files/projects.git/git.personal/D2B_Intermed_CapStoneProject/spark/test02_stream.py sparkm:/opt/spark-app/test02_stream.py

#run consumer|option2|test app
docker-compose exec sparkm spark-submit --master spark://172.18.0.5:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 /opt/spark-app/test02_stream.py
==============================================================================
sudo docker cp /home/akt/projects.files/projects.git/git.personal/D2B_Intermed_CapStoneProject/spark/sparkstreaming.py sparkm:/opt/spark-app/sparkstreaming.py

#run consumer|option2|stream app
docker-compose exec sparkm spark-submit --master spark://172.18.0.5:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 /opt/spark-app/sparkstreaming.py

docker-compose exec sparkm spark-submit --master spark://172.18.0.5:7077 /opt/spark-app/sparkstreaming.py
