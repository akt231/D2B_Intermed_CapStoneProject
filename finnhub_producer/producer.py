# importing required modules and functions
import os
import ast
import websocket
import json
from utils.helperfnc import print_env,  encode_avro, check_ticker, init_client, init_producer, load_avro_schema
from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaClient

# getting tokens from .env file
from dotenv import load_dotenv
load_dotenv()

#class for ingesting finnhub websocket data into Kafka
class ProducerFinnhub:
    def __init__(self):
        # list out all env variables containing 'd2b'string
        print_env('d2b')

        # initialise finnhub client and kafka producer 
        self.token_finnhubio = os.getenv('d2b_token_finnhubio')
        self.finnhub_client = init_client(os.getenv('d2b_token_finnhubio'))
        self.tickers = ast.literal_eval(os.environ['d2b_tickers_finnhubio'])
        
        self.kafka_server = f"{os.getenv('d2b_kafka_server')}:{os.getenv('d2b_kafka_port')}"
        self.topic = os.getenv('d2b_kafka_producer_topic')
        self.producer = init_producer(self.kafka_server)
        self.create_topic(self.topic)
        
        self.avro_schema = load_avro_schema('./finnhub_producer/schemas/schema_trades.avsc')
        
        websocket.enableTrace(True)
        self.socket_url = f"wss://ws.finnhub.io?token={self.token_finnhubio}"
        # initialise websocket connections
        self.ws = websocket.WebSocketApp(self.socket_url, \
                                  on_message = self.on_message, \
                                  on_error = self.on_error, \
                                  on_close = self.on_close) 
        self.ws.on_open = self.on_open
        self.ws.run_forever()

    #setting up new topic
    def create_topic(self, topic_name):
        client = KafkaClient(bootstrap_servers = self.kafka_server)
        for group in client.list_consumer_groups():
            topics_in_groups[group[0]] = []

        for group in topics_in_groups.keys():
            my_topics = []
            topic_dict = client.list_consumer_group_offsets(group)
            for topic in topic_dict:
                my_topics.append(topic.topic)
                topics_in_groups[group] = list(set(my_topics))

        for key , value in topics_in_groups.items():
            print(key, "\n\t", value)

        if topic_name not in server_topics:
            print(f'topic: {topic_name} does not exist.......will proceed with creating it now')
            admin_client = KafkaAdminClient(\
            bootstrap_servers=self.kafka_server,\
            client_id='admin_client_01'\
            )
    
            topic_list = []
            topic_list.append(NewTopic(name=topic_name, num_partitions=1, replication_factor=1))
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
     
    # websocket functions
    def on_message(self, ws, message):
        print(message)
        message = json.loads(message)
        avro_message = encode_avro(
            {
                'data': message['data'],
                'type': message['type']
            }, 
            self.avro_schema
        )
        self.producer.send(os.getenv('d2b_kafka_producer_topic'), avro_message)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print("=== socket closed ===")

    def on_open(self, ws):
        for ticker in self.tickers:
            print(f'test for ticker: {ticker}')
            print(f'ticker exist in xchange?: {check_ticker(self.finnhub_client,ticker)}')
            if(check_ticker(self.finnhub_client,ticker)):
                print(f'running ticker {ticker} right now')
                ws.send(f'{{"type":"subscribe","symbol":"{ticker}"}}')

                print(f'Subscription for {ticker} succeeded')
            else:
                print(f'Subscription for {ticker} failed - ticker not exist')

if __name__ == "__main__":
    ProducerFinnhub()