"""
Service layer cho trade data và price history
"""

from typing import Dict, Any, List
from services.clickhouse_service import get_clickhouse_client
from config import INTERVAL_MAP

def get_price_history(symbol: str, interval: str = "1m", limit: int = 100) -> Dict[str, Any]:
    """
    Lấy lịch sử giá theo khoảng thời gian (OHLCV - cho candlestick)
    
    Args:
        symbol: Trading pair
        interval: Khoảng thời gian (1m, 5m, 15m, 1h)
        limit: Số điểm dữ liệu
    
    Returns:
        Dict chứa OHLCV data
    """
    client = get_clickhouse_client()
    
    time_bucket = INTERVAL_MAP.get(interval, INTERVAL_MAP["1m"])
    
    # Tính OHLCV (Open, High, Low, Close, Volume) cho candlestick
    query = f"""
    SELECT 
        {time_bucket} as timestamp,
        argMin(Price, TradeTime) as open,
        MAX(Price) as high,
        MIN(Price) as low,
        argMax(Price, TradeTime) as close,
        SUM(TradeValue) as volume,
        COUNT(*) as trades
    FROM trades
    WHERE Symbol = '{symbol}'
      AND TradeTime >= now() - INTERVAL 24 HOUR
    GROUP BY timestamp
    ORDER BY timestamp DESC
    LIMIT {limit}
    """
    
    result = client.execute(query)
    
    # Tự động chọn độ chính xác dựa vào giá trị
    decimals = 8 if symbol.endswith('BTC') or symbol.endswith('ETH') else 2
    
    return {
        "symbol": symbol,
        "interval": interval,
        "data": [
            {
                "timestamp": row[0].isoformat(),
                "open": round(row[1], decimals),
                "high": round(row[2], decimals),
                "low": round(row[3], decimals),
                "close": round(row[4], decimals),
                "volume": round(row[5], decimals),
                "trades": row[6]
            }
            for row in result
        ]
    }

def get_realtime_trades(symbol: str, limit: int = 10) -> Dict[str, Any]:
    """
    Lấy trades gần nhất (realtime)
    Dùng để hiển thị live trades
    
    Args:
        symbol: Trading pair
        limit: Số lượng trades
    
    Returns:
        Dict chứa danh sách trades gần nhất
    """
    client = get_clickhouse_client()
    
    query = f"""
    SELECT 
        TradeTime,
        Price,
        Quantity,
        Side,
        TradeValue
    FROM trades
    WHERE Symbol = '{symbol}'
    ORDER BY TradeTime DESC
    LIMIT {limit}
    """
    
    result = client.execute(query)
    
    return {
        "symbol": symbol,
        "trades": [
            {
                "time": row[0].isoformat(),
                "price": row[1],
                "quantity": row[2],
                "side": row[3],
                "value": row[4]
            }
            for row in result
        ]
    }