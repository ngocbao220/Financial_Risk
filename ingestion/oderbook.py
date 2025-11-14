import asyncio
import websockets
import json
from datetime import datetime
# from producer_kafka import send_to_kafka

def convert_time(ms):
    """Chuyển timestamp Unix(ms) sang dạng readable"""
    if ms:
        return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return None

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
                bids = data.get('b', [])[:depth]
                asks = data.get('a', [])[:depth]

                # Log trên terminal
                print(f"\n===== {symbol.upper()} ORDERBOOK @ {event_time} =====")
                print("Top Bids:")
                for price, qty in bids:
                    print(f"  Price: {price} | Qty: {qty}")
                print("Top Asks:")
                for price, qty in asks:
                    print(f"  Price: {price} | Qty: {qty}")
                print("======================================\n")

                # Gửi Kafka
                # send_to_kafka('orderbook_stream', data)

            except Exception as e:
                print(f"[ORDERBOOK] Error: {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    symbols = ["btcusdt", "bnbbtc"]
    loop = asyncio.get_event_loop()
    tasks = [orderbook_stream(s) for s in symbols]
    loop.run_until_complete(asyncio.gather(*tasks))
