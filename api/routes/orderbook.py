"""
Routes cho orderbook endpoints
"""

from fastapi import APIRouter, HTTPException
from services import orderbook_service

router = APIRouter()

@router.get("/orderbook/{symbol}")
def get_orderbook_snapshot(symbol: str):
    """
    Lấy orderbook snapshot mới nhất
    Dùng để hiển thị bid/ask depth
    """
    try:
        return orderbook_service.get_orderbook_snapshot(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))