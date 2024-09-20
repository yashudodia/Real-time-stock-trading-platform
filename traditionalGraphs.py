import streamlit as st
import plotly.graph_objs as go
import json
import pandas as pd

from confluent_kafka import Consumer, KafkaError

def consume_prices():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mygroup',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(**conf)
    consumer.subscribe(['stock-trades'])

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
            yield stock_data
    finally:
        consumer.close()

def main():
    st.title('Real-Time Stock Price Visualization')

    data_placeholder = st.empty()
    data = pd.DataFrame(columns=['Date', 'Price'])

    for stock_data in consume_prices():
        new_data = pd.DataFrame({'Date': [stock_data['date']], 'Price': [stock_data['closing_price']]})
        data = pd.concat([data, new_data], ignore_index=True).tail(50)  # Keep last 50 data points

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=data['Date'], y=data['Price'], mode='lines+markers', name='Price'))
        fig.update_layout(height=500, width=700, title_text="Real-Time Stock Prices", xaxis_title="Time", yaxis_title="Price")
        data_placeholder.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
