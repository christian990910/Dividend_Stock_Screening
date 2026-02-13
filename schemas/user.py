from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime

class UserRegister(BaseModel):
    account: str
    password: str
    nickname: str
    email: EmailStr
    phone: Optional[str] = None
    avatar_url: Optional[str] = None

class UserLogin(BaseModel):
    account: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class UserOut(BaseModel):
    user_id: int
    account: str
    nickname: str
    email: str
    is_active: bool
    
    class Config:
        from_attributes = True