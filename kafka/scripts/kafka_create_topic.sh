# blocks until kafka is reachable
/bin/kafka-topics --bootstrap-server broker:9092 --list
echo -e 'Creating kafka topics'
/bin/kafka-topics --bootstrap-server broker:9092 --topic trades --create --partitions 1 --replication-factor 1
echo -e 'Successfully created the following topics:'
/bin/kafka-topics --bootstrap-server broker:9092 --list

#delete a topic
# /bin/kafka-topics --bootstrap-server broker:9092 --delete --topic first_topic