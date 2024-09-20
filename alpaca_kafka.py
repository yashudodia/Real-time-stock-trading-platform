import json
from kafka import KafkaProducer
from alpaca_trade_api.stream import Stream
import pandas as pd
import time
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed



API_KEY = 'PK75801NCM0T4NC316U4'
API_SECRET = 'ggQpe3BMbx0miE6mBHzUGDxsZknoyYFFhqQ5ajxV'
BASE_URL = 'https://paper-api.alpaca.markets'


async def on_time_sale_message(message):
        print(message)


symbols = ['GOOG', 'TSLA', 'NVDA', 'AAPL']
conn = StockDataStream(API_KEY, API_SECRET, BASE_URL, feed=DataFeed.IEX)
conn.subscribe_trades(on_time_sale_message, *symbols)
conn.run()




producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Callback for received messages
async def trade_callback(t):
    # Prepare the message for Kafka (you might want to include more fields)
    message = {'symbol': t.symbol, 'price': t.price, 'timestamp': t.timestamp}
    producer.send('stock-trades', value=message)
    producer.flush()

# Initialize Alpaca Stream
stream = Stream(API_KEY,
                API_SECRET,
                base_url=BASE_URL,
                data_feed='iex')  # or use 'sip' for premium data

# Subscribe to trade updates for a symbol
stream.subscribe_trades(trade_callback, 'AAPL')  # Example with Apple stock, add more as needed

# Start the stream
stream.run()
