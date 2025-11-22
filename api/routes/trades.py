"""
Routes cho trades và price history endpoints
"""

from fastapi import APIRouter, HTTPException
from services import trades_service

router = APIRouter()

@router.get("/price-history/{symbol}")
def get_price_history(
    symbol: str, 
    interval: str = "1m",  # 1m, 5m, 15m, 1h
    limit: int = 100
):
    """
    Lấy lịch sử giá theo khoảng thời gian (OHLCV - cho candlestick)
    
    Args:
        symbol: Trading pair
        interval: Khoảng thời gian (1m, 5m, 15m, 1h)
        limit: Số điểm dữ liệu
    """
    try:
        return trades_service.get_price_history(symbol, interval, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/realtime/{symbol}")
def get_realtime_data(symbol: str, limit: int = 10):
    """
    Lấy trades gần nhất (realtime)
    Dùng để hiển thị live trades
    """
    try:
        return trades_service.get_realtime_trades(symbol, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))