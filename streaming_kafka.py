# from kafka import Consumer, KafkaError
import csv
import os
from datetime import datetime
import json
import requests
from confluent_kafka import Producer

# ticker_symbol = 'BTC-USD'
# headers = {
#  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
#  'Content-Type': 'application/json',
#  'Authorization': 'Bearer <token>'
# }



# def fetch_and_send_stock_price():
#  while True:
#    try:
#      url = 'https://query2.finance.yahoo.com/v8/finance/chart/btc-usd'
#      response = requests.get(url, headers=headers)
#      data = json.loads(response.text)
#      price = data["chart"]["result"][0]["meta"]["regularMarketPrice"]
#      print(f"Sent {ticker_symbol} price to Kafka: {price}")
#      return data 
#    except Exception as e:
#     print(f"Error fetching/sending stock price: {e}")

# data = fetch_and_send_stock_price()
# print(data)



import yfinance as yf
# from kafka import KafkaProducer
import json
import time

# # Initialize Kafka producer to connect to the Kafka service within Docker
# producer = Producer(bootstrap_servers=['broker:9092'], # Use the service name and port from docker-compose
#                          value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# def fetch_and_send_stock_price():
#     ticker_symbol = 'AAPL'  # Example with Apple stock, modify as needed
#     while True:
#         try:
#             # Fetch data
#             ticker = yf.Ticker(ticker_symbol)
#             hist = ticker.history(period="1d", interval="1m")
#             # Latest price
#             price = hist['Close'].iloc[-1]
#             # Construct message
#             message = {'symbol': ticker_symbol, 'price': float(price)}
#             # Send to Kafka
#             producer.send('stock-trades', value=message)
#             producer.flush()
#             print(f"Sent {ticker_symbol} price to Kafka: {price}")
#             time.sleep(60)  # Fetch every 60 seconds
#         except Exception as e:
#             print(f"Error fetching/sending stock price: {e}")

# fetch_and_send_stock_price()




# Corrected Kafka producer configuration
conf = {
    'bootstrap.servers': "localhost:9092",  # Use the service name and port from docker-compose
}

producer = Producer(**conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def fetch_and_send_stock_price():
    ticker_symbol = 'AAPL'  # Example ticker symbol
    while True:
        try:
            # Fetch the most recent day's data with a 5-minute interval
            ticker = yf.Ticker(ticker_symbol)
            hist = ticker.history(period="1d", interval="5m")
            if not hist.empty:
                # Get the opening price of the most recent interval
                open_price = hist['Open'].iloc[-1]
                # Get the closing price of the most recent interval
                latest_price = hist['Close'].iloc[-1]
                
                # Calculate price movement
                price_movement = latest_price - open_price
                
                # Construct the message with the real price and movement
                message = {
                    'symbol': ticker_symbol, 
                    'latest_price': float(latest_price),
                    'price_movement': float(price_movement)
                }
                
                # Send the message to Kafka
                producer.produce('stock-trades', key=str(ticker_symbol), value=json.dumps(message), callback=delivery_report)
                producer.poll(0)  # Serve delivery callback queue
                
                print(f"Sent {ticker_symbol} price and movement to Kafka: {latest_price}, {price_movement}")
            else:
                print(f"No data fetched for {ticker_symbol}.")
            
            time.sleep(20)  # Fetch every 5 minutes
        except Exception as e:
            print(f"Error fetching/sending stock price: {e}")

# fetch_and_send_stock_price()

def fetch_and_stream_historical_prices(ticker_symbol):
    """
    Fetches and streams historical stock prices for a given ticker symbol.

    Args:
    ticker_symbol (str): The stock ticker symbol for which to fetch historical prices.
    """
    try:
        # Fetch historical data for the last 2 years
        ticker = yf.Ticker(ticker_symbol)
        hist = ticker.history(period="2y", interval="1d")
        
        for date, row in hist.iterrows():
            # Get the closing price for the day
            closing_price = row['Close']
            
            # Construct the message with the price and date
            message = {
                'symbol': ticker_symbol,
                'date': str(date.date()),
                'closing_price': float(closing_price)
            }
            
            # Send the message to Kafka
            producer.produce(f'stock-trades-{ticker_symbol}', key=str(date.date()), value=json.dumps(message), callback=delivery_report)
            producer.poll(0)  # Serve delivery callback queue
            
            print(f"Sent {ticker_symbol} closing price for {date.date()} to Kafka: {closing_price}")
            
            # To mimic live streaming, introduce a delay between sending each day's price
            time.sleep(2)  # Delay of 2 seconds for demonstration; adjust as needed
            
    except Exception as e:
        print(f"Error fetching/sending historical stock prices: {e}")

fetch_and_stream_historical_prices("AAPL")
