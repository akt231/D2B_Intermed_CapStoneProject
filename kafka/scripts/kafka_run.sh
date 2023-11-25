# start zookeeper
/bin/zookeeper-server-start /etc/kafka/zookeeper.properties
timeout 5

# start kafka server
/bin/kafka-server-start /etc/kafka/server.properties

# start producer for existing topic trades 
/bin/kafka-console-producer --topic trades --bootstrap-server broker:29092
timeout 5

#run consumer
/bin/kafka-console-consumer --topic trades --bootstrap-server broker:29092 --from-beginning