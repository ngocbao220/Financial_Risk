import asyncio
import websockets
import json
import time
from websocket_orderbook import orderbook_stream
from websocket_ticker import ticker_stream
from websocket_trade import trade_stream

if __name__ == "__main__":
    symbols = ["btcusdt", "bnbbtc"]  # Danh sách symbol cần subscribe
    interval = "1h"

    loop = asyncio.get_event_loop()
    tasks = []

    for s in symbols:
        tasks.append(trade_stream(s))
        tasks.append(ticker_stream(s, interval))
        tasks.append(orderbook_stream(s))

    loop.run_until_complete(asyncio.gather(*tasks))
