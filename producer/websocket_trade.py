import asyncio
import websockets
import json
from datetime import datetime
from kafka_producer import send_to_kafka
from helpers import convert_time

async def trade_stream(symbol: str):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"

    async with websockets.connect(url) as ws:
        print(f"[TRADE] Connected to {symbol} WebSocket")
        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)

                # Chuyển timestamp
                event_time = convert_time(data.get('E'))

                # Thông tin trade
                price = data.get('p')
                quantity = data.get('q')
                side = "SELL" if data.get('m') else "BUY"  # m = True là sell
                trade_id = data.get('t')

                # Log trên terminal
                print(f"[{symbol.upper()}] Trade ID: {trade_id} | {side} | Price: {price} | Qty: {quantity} | Event: {event_time}")

                # Gửi dữ liệu Kafka
                send_to_kafka('trade_stream', data)

            except Exception as e:
                print(f"[TRADE] Error: {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    symbols = ["btcusdt", "bnbbtc"]  # ví dụ
    loop = asyncio.get_event_loop()
    tasks = [trade_stream(s) for s in symbols]
    loop.run_until_complete(asyncio.gather(*tasks))
