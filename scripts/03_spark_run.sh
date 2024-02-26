=========================================================================
#run consumer option1
docker exec -it sparkm bash
docker exec -it --user root sparkm bash

=========================================================================
#copy modified files to running spark container
cd /home/akt/projects.files/projects.git/git.personal/D2B_Intermed_CapStoneProject
#sudo docker cp ./spark/sparkstreaming.py sparkm:/opt/spark-app/sparkstreaming.py
sudo docker cp ./spark/sparkx.py sparkm:/opt/spark-app/sparkx.py
sudo docker cp ./spark/test.py sparkm:/opt/spark-app/test.py
sudo docker cp ./spark/utils/. sparkm:/opt/spark-app/utils/
sudo docker cp .env sparkm:/opt/spark-app/.env
sudo docker cp rsa_key.p8 sparkm:/opt/spark-app/rsa_key.p8
docker exec -it --user root sparkm chown -R 1001:1001 /opt/spark-app

=========================================================================
#run consumer|option2|stream app invalid will delete the py file
docker-compose exec sparkm spark-submit --master spark://172.18.0.5:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-avro_2.12:3.4.0,net.snowflake:spark-snowflake_2.12:2.13.0-spark_3.4 /opt/spark-app/sparkstreaming.py

=========================================================================
#run consumer|option2|stream app valid and works will delete the py file
docker-compose exec sparkm spark-submit --master spark://172.18.0.5:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.apache.spark:spark-avro_2.12:3.4.0,net.snowflake:spark-snowflake_2.12:2.13.0-spark_3.4 /opt/spark-app/sparkx.py

=========================================================================
#run consumer|option2|stream app not valid and works will delete the py file
docker-compose exec sparkm spark-submit --master spark://172.18.0.5:7077 --packages net.snowflake:snowflake-jdbc:3.13.29,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4 /opt/spark-app/test.py

=========================================================================
#change permissions of folder and ownership of symlinks:
#chown -R <username>:<groupname> <folder>
#chown -hR <username>:<groupname> <folder>
  
#deleting the checkpoint folder files
cd /home/akt/projects.files/projects.git/git.personalD2B_Intermed_CapStoneProject/spark/chkpt
sudo chattr -a ./spark/chkpt/*
sudo chown -hR akt:akt ./spark/chkpt/*
rm -rf ./spark/chkpt/*