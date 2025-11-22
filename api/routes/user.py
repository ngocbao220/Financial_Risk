from fastapi import APIRouter
from services.user_service import create_user, get_user, get_all_users
router = APIRouter()

@router.post("/create")
def create_user_api(data: dict):
    return create_user(data["username"])

@router.get("/get/{user_id}")
def get_user_api(user_id: str):
    return get_user(user_id)

@router.get("/get_all")
def list_users_api():
    """API lấy danh sách toàn bộ user"""
    return get_all_users()
