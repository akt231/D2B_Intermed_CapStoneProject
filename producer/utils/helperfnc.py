import json
import finnhub
import io
import avro.schema
import avro.io
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os


#setting up Finnhub client connection to test if tickers specified in config exist
def init_client(token_finnhubio):
    return finnhub.Client(api_key=token_finnhubio)

#look up ticker in Finnhub
def get_ticker(finnhub_client,ticker):
    return finnhub_client.symbol_lookup(ticker)

#validate if ticker exists
def check_ticker(finnhub_client,ticker):
    for stock in get_ticker(finnhub_client,ticker)['result']:
        if stock['symbol']==ticker:
            return True
    return False

#setting up a Kafka connection
def init_producer(kafka_server):
    return KafkaProducer(bootstrap_servers=kafka_server)

#parse Avro schema
def load_avro_schema(schema_path):
    return avro.schema.parse(open(schema_path).read())
    
#encode message into avro format
def encode_avro(data, schema):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(data, encoder)
    return bytes_writer.getvalue()


#print env variables
def print_env(search_string):
    #list stored variables
    print('Environment:')
    keylst =os.environ.keys()
    tokens_exist = 0
    for k, v in os.environ.items():
        if search_string in k.lower():
            print(f'{k}={v}')
        else:
            tokens_exist += 1
    if tokens_exist == 0:
        print('no env tokens set')