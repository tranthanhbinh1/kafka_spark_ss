import finnhub
import time
import multiprocessing as mp
import json


api_key = "chbqth1r01quf00vuei0chbqth1r01quf00vueig"
symbols = ['AAPL', 'AMZN', 'GOOG', 'MSFT', 'TSLA', 'NVDA', 'NFLX']
def get_stock_candles(symbol):
    # Calculate the start and end timestamps for a 1-minute interval
    end_timestamp = int(time.time())
    start_timestamp = end_timestamp - 60

    # Convert the timestamps to the required format for the function call
    start_timestamp = int(start_timestamp)
    end_timestamp = int(end_timestamp)

    # Setup client
    finnhub_client = finnhub.Client(api_key=api_key)

    # Retrieve stock candles
    res = finnhub_client.stock_candles(symbol, 'D', start_timestamp, end_timestamp)
    return res


pool = mp.Pool(len(symbols))
results = pool.map(get_stock_candles, symbols)
with open('stock_data.json', 'w') as f:
    for res in results:
        json.dump(res, f, indent=2)
