# start zookeeper
docker exec -it zookeeper bash
/bin/zookeeper-server-start /etc/kafka/zookeeper.properties

# start kafka server
docker exec -it broker bash
/bin/kafka-server-start /etc/kafka/server.properties

# start producer for existing topic trades 
docker exec -it broker bash
/bin/kafka-console-producer --topic trades --bootstrap-server broker:29092

#run consumer
docker exec -it broker bash
/bin/kafka-console-consumer --topic trades --bootstrap-server broker:29092 --from-beginning