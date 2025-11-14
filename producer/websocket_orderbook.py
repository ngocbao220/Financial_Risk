import asyncio
import websockets
import json
from datetime import datetime
from helpers import convert_time
from kafka_producer import send_to_kafka

async def orderbook_stream(symbol: str, depth: int = 5):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@depth@100ms"

    async with websockets.connect(url) as ws:
        print(f"[ORDERBOOK] Connected to {symbol} orderbook")
        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)

                # Lấy thời gian
                event_time = convert_time(data.get('E'))

                # Lấy top N bids/asks
                bids = data.get('b', [])
                asks = data.get('a', [])

                # Log trên terminal
                print(f"\n===== {symbol.upper()} ORDERBOOK @ {event_time} =====")
                print("Bids:")
                for price, qty in bids:
                    print(f"  Price: {price} | Qty: {qty}")
                print("Asks:")
                for price, qty in asks:
                    print(f"  Price: {price} | Qty: {qty}")
                print("======================================\n")

                # Gửi Kafka
                send_to_kafka('orderbook_stream', data)

            except Exception as e:
                print(f"[ORDERBOOK] Error: {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    symbols = ["btcusdt", "bnbbtc"]
    loop = asyncio.get_event_loop()
    tasks = [orderbook_stream(s) for s in symbols]
    loop.run_until_complete(asyncio.gather(*tasks))
