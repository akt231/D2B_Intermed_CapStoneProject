# blocks until kafka is reachable
set kafka_dir = 'c:/personal.programs/kafka/bin'

echo -e 'List kafka topics'
%kafka_dir%/kafka-topics.bat --bootstrap-server localhost:29092 --list

echo -e 'Creating kafka topics'
%kafka_dir%/kafka-topics.bat --bootstrap-server localhost:29092 --create --if-not-exists --topic trades --replication-factor 1 --partitions 1

echo -e 'Successfully created the following topics:'
%kafka_dir%/kafka-topics.bat --bootstrap-server localhost:29092 --listP