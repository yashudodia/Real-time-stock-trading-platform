
import threading
import pandas as pd
import time
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed
import json 
import websocket


API_KEY = 'PK75801NCM0T4NC316U4'
API_SECRET = 'ggQpe3BMbx0miE6mBHzUGDxsZknoyYFFhqQ5ajxV'
BASE_URL = 'https://api.alpaca.markets'


def on_open(ws):
    print("opened")
    auth_data = {
        "action": "authenticate",
        "data": {"key_id": API_KEY, "secret_key": API_SECRET}
    }

    ws.send(json.dumps(auth_data))

    listen_message = {"action": "listen", "data": {"streams": ["TSLA"]}}

    ws.send(json.dumps(listen_message))


def on_message(ws, message):
    print("received a message")
    print(message)

def on_close(ws):
    print("closed connection")

BASE_URL = 'https://paper-api.alpaca.markets/v2'
socket = "wss://stream.data.alpaca.markets/v2/iex"

ws = websocket.WebSocketApp(socket, on_open=on_open)
ws.run_forever()
# async def on_time_sale_message(message):
#         print(message)

{"action": "authenticate","data": {"key_id": API_KEY, "secret_key": API_SECRET}}
# # symbols = ['NVDA', 'AAPL']
# symbols = ['PHUN', 'DATS', 'DWAC', 'CNEY']
# conn = StockDataStream(API_KEY, API_SECRET, BASE_URL, feed=DataFeed.IEX)
# conn.subscribe_trades(on_time_sale_message, *symbols)
# conn.run()
