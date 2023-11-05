# blocks until kafka is reachable
echo -e 'List kafka topics'
kafka-topics.bat --bootstrap-server localhost:29092 --list

echo -e 'Creating kafka topics'
kafka-topics.bat --bootstrap-server localhost:29092 --create --if-not-exists --topic topic_finnhub --replication-factor 1 --partitions 1

echo -e 'Successfully created the following topics:'
kafka-topics.bat --bootstrap-server localhost:29092 --listP