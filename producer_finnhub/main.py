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
token_finnhubio = os.getenv('token_finnhubio')
tickers_finnhubio = ast.literal_eval( os.getenv('tickers_finnhubio'))

# get kafka data from .env file
kafka_server = os.getenv('kafka_server')
kafka_port = os.getenv('kafka_port')
kafka_topic_name = os.getenv('kafka_topic_name')

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
    producer.send(kafka_topic_name, avro_message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("=== socket closed ===")

def on_open(ws):
    for ticker in tickers_finnhubio:
        if(check_ticker(finnhub_client,ticker)==True):
            ws.send(f'{"type":"subscribe","symbol":"{ticker}"}')
            print(f'Subscription for {ticker} succeeded')
        else:
            print(f'Subscription for {ticker} failed - ticker not found')

if __name__ == "__main__":
    #list stored variables
    print('Environment:')
    for k, v in os.environ.items():
        print(f'{k}={v}')

    finnhub_client = init_client(token_finnhubio)
    producer = load_producer(f"{kafka_server}:{kafka_port}")
    
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={token_finnhubio}",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
    