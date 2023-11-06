# importing required modules and functions
import os
import ast
import websocket
import json
from utils.helperfnc import print_env,  encode_avro, check_ticker, init_client, init_producer, load_avro_schema

# getting tokens from .env file
from dotenv import load_dotenv
load_dotenv()
# get finnhub data from .env file
d2b_token_finnhubio = os.getenv('d2b_token_finnhubio')
d2b_tickers_finnhubio = ast.literal_eval( os.getenv('d2b_tickers_finnhubio'))

# get kafka data from .env file
d2b_kafka_server = os.getenv('d2b_kafka_server')
d2b_kafka_port = os.getenv('d2b_kafka_port')
d2b_kafka_producer_topic = os.getenv('d2b_kafka_producer_topic')

# websocket functions
def on_message(ws, message, producer):
    print(message)
    message = json.loads(message)
    loaded_schema = load_avro_schema('./finnhub_producer/schemas/schema_trades.avsc')
    avro_message = encode_avro(
        {
            'data': message['data'],
            'type': message['type']
        }, 
        loaded_schema
    )
    producer.send(d2b_kafka_producer_topic, avro_message)

def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("=== socket closed ===")

def on_open(ws, finnhub_client):
    for ticker in d2b_tickers_finnhubio:
        print(f'test for ticker: {ticker}')
        print(f'ticker exist in xchange?: {check_ticker(finnhub_client,ticker)}')
        if(check_ticker(finnhub_client,ticker)):
            print(f'running ticker {ticker} right now')
            ws.send(f'{{"type":"subscribe","symbol":"{ticker}"}}')
            
            print(f'Subscription for {ticker} succeeded')
        else:
            print(f'Subscription for {ticker} failed - ticker not exist')

def main(d2b_token_finnhubio, d2b_kafka_server, d2b_kafka_port):
    # list out all env variables containing 'd2b'string
    print_env('d2b')

    # initialise finnhub client and kafka producer 
    finnhub_client = init_client(d2b_token_finnhubio)
    producer = init_producer(f"{d2b_kafka_server}:{d2b_kafka_port}")
    
    # enable websocket trace
    websocket.enableTrace(True)
    socket_url = f"wss://ws.finnhub.io?token={d2b_token_finnhubio}"
    
    # initialise websocket connections
    ws = websocket.WebSocketApp(socket_url, 
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    
    # open websocket connections and make persistent
    ws.on_open = on_open
    ws.run_forever()
    
    
if __name__ == "__main__":
    main(d2b_token_finnhubio, d2b_kafka_server, d2b_kafka_port)