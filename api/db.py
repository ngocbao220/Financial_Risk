import redis

redis_client = redis.Redis(
    host="redis",      # tên container docker
    port=6379,
    decode_responses=True
)
