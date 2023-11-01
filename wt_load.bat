# start zookeeper
wt -w 0 nt --startingDirectory c:/personal.programs/kafka --title ZooKeper --tabColor "#8AD8FF" --colorScheme "One Half Dark" powershell -noExit ".\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties"

# start kafka server
wt -w 0 nt --startingDirectory c:/personal.programs/kafka --title KafkaServer --tabColor "#F0F" --colorScheme "One Half Dark" powershell -noExit ".\bin\windows\kafka-server-start.bat .\config\server.properties"

# start producer for existing topic topic_finnhub 
wt -w 0 nt --startingDirectory c:/personal.programs/kafka --title Producer --tabColor "#00F" --colorScheme "One Half Dark" powershell -noExit ".\bin\windows\kafka-console-producer.bat --topic topic_finnhub --bootstrap-server localhost:9092"

# start Consumer for existing topic topic_finnhub 
wt -w 0 nt --startingDirectory c:/personal.programs/kafka --title Consumer --tabColor "#0F0" --colorScheme "One Half Dark" powershell -noExit ".\bin\windows\kafka-console-consumer.bat --topic topic_finnhub --bootstrap-server localhost:9092 --from-beginning"






