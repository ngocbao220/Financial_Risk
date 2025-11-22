"""
Routes cho symbols management endpoints
"""

from fastapi import APIRouter, HTTPException
from services import symbols_service

router = APIRouter()

@router.get("/symbols")
def get_available_symbols():
    """Lấy danh sách symbols có data"""
    try:
        symbols = symbols_service.get_available_symbols()
        return {"symbols": symbols}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))