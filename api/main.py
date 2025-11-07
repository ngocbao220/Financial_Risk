from fastapi import FastAPI, WebSocket
from cassandra.cluster import Cluster
from fastapi.middleware.cors import CORSMiddleware
import asyncio

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

cluster = Cluster(['cassandra'])
session = cluster.connect('op_risk')

@app.get("/risks/recent")
def get_recent(limit: int = 50):
    rows = session.execute(f"SELECT * FROM risk_signals LIMIT {limit}")
    return [dict(r._asdict()) for r in rows]

@app.websocket("/ws/risks")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    while True:
        rows = session.execute("SELECT * FROM risk_signals LIMIT 20")
        data = [dict(r._asdict()) for r in rows]
        await ws.send_json(data)
        await asyncio.sleep(2)
