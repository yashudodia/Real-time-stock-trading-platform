import json
import requests
from time import sleep
from confluent_kafka import Producer

# Initialize Kafka Producer
producer_conf = {
    'bootstrap.servers': "localhost:9092",  # Update with your Kafka broker's address
}
producer = Producer(**producer_conf)

def delivery_report(err, msg):
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def fetch_and_stream_stock_price(api_key, symbol, interval):
    while True:
        # Construct the API URL
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={api_key}&outputsize=full"
        response = requests.get(url)
        data = response.json()

        # Extract the time series data
        time_series = data.get(f"Time Series ({interval})", {})
        for time, values in time_series.items():
            price = values['4. close']
            message = {'symbol': symbol, 'time': time, 'price': float(price)}
            # Produce the message to Kafka
            producer.produce('stock-prices-alphavantage', key=symbol, value=json.dumps(message), callback=delivery_report)
            producer.poll(0)  # Serve delivery callback queue
            print(f"Sent price for {symbol} at {time}: {price}")
        
        sleep(60)  # Pause for 5 minutes between requests

# Example usage
api_key = '73D0V4NT052FRZJN'  # Replace with your Alpha Vantage API key
fetch_and_stream_stock_price(api_key, 'AAPL', '60min')
