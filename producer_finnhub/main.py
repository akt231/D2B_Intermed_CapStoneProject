import argparse
import os
from utils.helperfnc import init_client, get_ticker

#getting tokens from .env file
from dotenv import load_dotenv
load_dotenv()
token_finnhubio = os.getenv('token_finnhubio')

if __name__ == '__main__':
    #initialise finhub client
    finnhub_client = init_client(token_finnhubio)

    parser = argparse.ArgumentParser(prog="main.py",
                            usage = None,
                            description="List of Tickers from Finnhub search",
                            formatter_class= argparse.ArgumentDefaultsHelpFormatter,
                             conflict_handler = "error",
                            add_help = True
                            )
    
    parser.add_argument('--ticker', type=str, help="Please supply Ticker name: Enter the phrase to look up for a ticker")

    args = parser.parse_args()
    params = vars(args)

    try:
        print(get_ticker(finnhub_client,params['ticker']))
    except Exception as e:
        print(str(e))
