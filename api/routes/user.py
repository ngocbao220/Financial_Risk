from fastapi import APIRouter
from services.user_service import create_user, get_user

router = APIRouter()

@router.post("/create")
def create_user_api(data: dict):
    return create_user(data["username"])

@router.get("/{user_id}")
def get_user_api(user_id: str):
    return get_user(user_id)
