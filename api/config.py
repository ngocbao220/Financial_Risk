"""
Configuration cho ClickHouse và database settings
"""

import os

# ClickHouse connection settings
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "12345")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")

# Interval mapping cho time-based queries
INTERVAL_MAP = {
    "1m": "toStartOfMinute(TradeTime)",
    "5m": "toStartOfFiveMinutes(TradeTime)",
    "15m": "toStartOfFifteenMinutes(TradeTime)",
    "1h": "toStartOfHour(TradeTime)",
    "4h": "toDateTime(toStartOfHour(TradeTime) - toStartOfHour(TradeTime) % 14400)",
    "1d": "toStartOfDay(TradeTime)"
}