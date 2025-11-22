import uuid
from db import redis_client

def create_user(username: str):
    user_id = redis_client.incr("user:id:counter")
    
    # Lưu info cơ bản
    redis_client.hset(f"user:{user_id}", mapping={
        "user_id": user_id,
        "username": username,
    })

    # Lưu balance riêng
    redis_client.hset(f"user:{user_id}:balance", mapping={
        "usd": 1000.0,
        "btc": 1.0,
        "reserved_usd": 0.0,
        "reserved_btc": 0.0
    })

    return {
        "user_id": user_id,
        "username": username,
        "usd": 1000.0,
        "btc": 1.0,
        "reserved_usd": 0.0,
        "reserved_btc": 0.0
    }

def get_user(user_id: str):
    info = redis_client.hgetall(f"user:{user_id}")
    balance = redis_client.hgetall(f"user:{user_id}:balance")

    if not info or not balance:
        return {"error": "User not found"}

    # Convert balance sang float
    def safe_float(v):
        try:
            return float(v)
        except:
            return 0.0

    return {
        "user_id": info.get("user_id"),
        "username": info.get("username"),
        "usd": safe_float(balance.get("usd")),
        "btc": safe_float(balance.get("btc")),
        "reserved_usd": safe_float(balance.get("reserved_usd")),
        "reserved_btc": safe_float(balance.get("reserved_btc")),
    }
