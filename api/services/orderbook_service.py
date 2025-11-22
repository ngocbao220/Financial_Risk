"""
Service layer cho orderbook data
"""

from typing import Dict, Any
from services.clickhouse_service import get_clickhouse_client

def get_orderbook_snapshot(symbol: str) -> Dict[str, Any]:
    """
    Lấy orderbook snapshot mới nhất
    Dùng để hiển thị bid/ask depth
    
    Args:
        symbol: Trading pair
    
    Returns:
        Dict chứa orderbook data với bids và asks
    """
    client = get_clickhouse_client()
    
    query = f"""
    SELECT 
        event_time,
        bid_prices,
        bid_quantities,
        ask_prices,
        ask_quantities
    FROM orderbook
    WHERE symbol = '{symbol}'
    ORDER BY event_time DESC
    LIMIT 1
    """
    
    result = client.execute(query)
    
    if not result:
        return {"symbol": symbol, "message": "No orderbook data"}
    
    row = result[0]
    
    # Combine bid prices and quantities
    bids = [
        {"price": p, "quantity": q} 
        for p, q in zip(row[1], row[2])
    ][:10]  # Top 10 bids
    
    asks = [
        {"price": p, "quantity": q} 
        for p, q in zip(row[3], row[4])
    ][:10]  # Top 10 asks
    
    return {
        "symbol": symbol,
        "timestamp": row[0].isoformat(),
        "bids": bids,
        "asks": asks,
        "spread": asks[0]["price"] - bids[0]["price"] if bids and asks else 0
    }