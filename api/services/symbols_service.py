"""
Service layer cho symbols management
"""

from typing import List
from services.clickhouse_service import get_clickhouse_client

def get_available_symbols() -> List[str]:
    """
    Lấy danh sách symbols có data trong database
    
    Returns:
        List các symbol strings
    """
    client = get_clickhouse_client()
    
    query = """
    SELECT DISTINCT Symbol 
    FROM trades
    ORDER BY Symbol
    """
    
    result = client.execute(query)
    
    return [row[0] for row in result]