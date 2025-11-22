"""
Routes cho trade statistics endpoints
"""

from fastapi import APIRouter, HTTPException
from services import stats_service

router = APIRouter()

@router.get("/stats/{symbol}")
def get_trade_stats(symbol: str, hours: int = 24):
    """
    Lấy thống kê trading của 1 symbol
    
    Args:
        symbol: Trading pair (BTCUSDT, BNBBTC, ...)
        hours: Số giờ lịch sử (default 24h)
    """
    try:
        return stats_service.get_trade_statistics(symbol, hours)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))