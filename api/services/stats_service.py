"""
Service layer cho trade statistics
"""

from typing import Dict, Any
from services.clickhouse_service import get_clickhouse_client

def get_trade_statistics(symbol: str, hours: int = 24) -> Dict[str, Any]:
    """
    Lấy thống kê trading của 1 symbol
    
    Args:
        symbol: Trading pair (BTCUSDT, BNBBTC, ...)
        hours: Số giờ lịch sử (default 24h)
    
    Returns:
        Dict chứa thống kê trades
    """
    client = get_clickhouse_client()
    
    query = f"""
    SELECT 
        Symbol as symbol,
        COUNT(*) as total_trades,
        SUM(TradeValue) as total_volume,
        AVG(Price) as avg_price,
        MIN(Price) as min_price,
        MAX(Price) as max_price
    FROM trades
    WHERE Symbol = '{symbol}'
      AND TradeTime >= now() - INTERVAL {hours} HOUR
    GROUP BY Symbol
    """
    
    result = client.execute(query)
    
    if not result:
        return {
            "symbol": symbol,
            "total_trades": 0,
            "total_volume": 0,
            "message": "No data found"
        }
    
    row = result[0]
    return {
        "symbol": row[0],
        "total_trades": row[1],
        "total_volume": round(row[2], 2),
        "avg_price": round(row[3], 2),
        "min_price": round(row[4], 2),
        "max_price": round(row[5], 2),
        "period_hours": hours
    }