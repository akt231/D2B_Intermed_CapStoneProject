# imorting required modules and functions
import os
import ast
import websocket
import json
from utils.helperfnc import encode_avro, check_ticker, init_client, load_producer, load_avro_schema

# getting tokens from .env file
from dotenv import load_dotenv
load_dotenv()
# get finnhub data from .env file
d2b_token_finnhubio = os.getenv('d2b_token_finnhubio')
d2b_tickers_finnhubio = ast.literal_eval( os.getenv('d2b_tickers_finnhubio'))

# get kafka data from .env file
d2b_kafka_server = os.getenv('d2b_kafka_server')
d2b_kafka_port = os.getenv('d2b_kafka_port')
d2b_kafka_topic_name = os.getenv('d2b_kafka_topic_name')

# websocket functions
def on_message(ws, message):
    print(message)
    message = json.loads(message)
    avro_message = encode_avro(
        {
            'data': message['data'],
            'type': message['type']
        }, 
        load_avro_schema('schemas/schema_trades.avsc')
    )
    producer.send(d2b_kafka_topic_name, avro_message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("=== socket closed ===")

def on_open(ws):
    for ticker in d2b_tickers_finnhubio:
        if(check_ticker(finnhub_client,ticker)==True):
            ws.send(f'{"type":"subscribe","symbol":"{ticker}"}')
            print(f'Subscription for {ticker} succeeded')
        else:
            print(f'Subscription for {ticker} failed - ticker not found')

if __name__ == "__main__":
    #list stored variables
    print('Environment:')
    for k, v in os.environ.items():
        if k
        print(f'{k}={v}')

    finnhub_client = init_client(d2b_token_finnhubio)
    producer = load_producer(f"{d2b_kafka_server}:{d2b_kafka_port}")
    
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={d2b_token_finnhubio}",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
    