import pandas as pd
import numpy as np
import plotly.graph_objs as go
import streamlit as st
import json
from confluent_kafka import Consumer, KafkaError

def consume_prices(consumer):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    }
    # consumer = Consumer(**conf)
    # consumer.subscribe([f'stock-trades-{ticker}'])
    data = pd.DataFrame(columns=['Date', 'Close'])
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Handle end of partition
                    continue
                else:
                    print(msg.error())
                    break
            stock_data = json.loads(msg.value().decode('utf-8'))
            new_row = {'Date': pd.to_datetime(stock_data['date']), 'Close': stock_data['closing_price']}
            data = pd.concat([data, pd.DataFrame([new_row])], ignore_index=True)
            if len(data) >= 15:  # Check if we have at least 30 data points
                yield data
    finally:
        consumer.close()



# Calculate indicators
def calculate_indicators(df):
    # Calculate Moving Averages, MACD, RSI
    df['EMA_12'] = df['Close'].ewm(span=12, adjust=False).mean()
    df['EMA_26'] = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = df['EMA_12'] - df['EMA_26']
    df['Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    df['SMA_5'] = df['Close'].rolling(window=5).mean()
    df['SMA_20'] = df['Close'].rolling(window=20).mean()

    return df
def plot_data(df, figs):
    # Update existing figures with new data
    figs['close_price'].data = []
    figs['sma'].data = []
    figs['macd'].data = []
    figs['rsi'].data = []
    for fig_key in figs:
        figs[fig_key].data = []
    # Close Price Chart
    figs['close_price'].add_trace(go.Scatter(x=df['Date'], y=df['Close'], mode='lines', name='Close Price'))

    # SMA Chart
    figs['sma'].add_trace(go.Scatter(x=df['Date'], y=df['SMA_5'], mode='lines', name='5-day SMA'))
    figs['sma'].add_trace(go.Scatter(x=df['Date'], y=df['SMA_20'], mode='lines', name='20-day SMA'))

    # MACD Chart
    figs['macd'].add_trace(go.Scatter(x=df['Date'], y=df['MACD'], mode='lines', name='MACD'))
    figs['macd'].add_trace(go.Scatter(x=df['Date'], y=df['Signal'], mode='lines', name='Signal Line'))

    # RSI Chart
    figs['rsi'].add_trace(go.Scatter(x=df['Date'], y=df['RSI'], mode='lines', name='RSI'))
def setup_consumer():
    """Sets up the Kafka consumer."""
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    }
    return Consumer(**conf)

def main():
    st.title('Real-Time Stock Indicator Visualization')
    data = pd.DataFrame(columns=['Date', 'Close'])
    available_tickers = ['AAPL', 'GOOG', 'MSFT', 'TSLA', 'GL', 'AMZN','NVDA']  # Add more tickers as needed
    ticker_symbol = st.selectbox("Select the stock ticker:", available_tickers)
    if st.button(" Stock Prices"):
        consumer = setup_consumer()
        consumer.subscribe([f'stock-trades-{ticker_symbol}'])
        # Create initial figures
        figs = {
            'close_price': go.Figure(),
            'sma': go.Figure(),
            'macd': go.Figure(),
            'rsi': go.Figure()
        }

        # Placeholders for each chart
        placeholder_close_price = st.empty()
        placeholder_sma = st.empty()
        placeholder_macd = st.empty()
        placeholder_rsi = st.empty()

        for data in consume_prices(consumer):
            df = calculate_indicators(data)  # Calculate indicators
            plot_data(df, figs)  # Update plots with new data

            # Display figures in placeholders
            placeholder_close_price.plotly_chart(figs['close_price'], use_container_width=True)
            placeholder_sma.plotly_chart(figs['sma'], use_container_width=True)
            placeholder_macd.plotly_chart(figs['macd'], use_container_width=True)
            placeholder_rsi.plotly_chart(figs['rsi'], use_container_width=True)

if __name__ == "__main__":
    main()
