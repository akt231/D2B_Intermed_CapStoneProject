#run producer
docker exec -it producer sh

#run python script
python ./producer.py


#all run at once
docker exec -it producer python ./producer.py