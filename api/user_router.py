from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from fastapi.security import OAuth2PasswordRequestForm
from datetime import timedelta
from core.database import get_db
from core.auth_dependency import get_current_user
from core.security import create_access_token
from schemas.user import UserCreate, UserLogin, UserUpdate, UserResponse, Token
from crud.user import (
    create_user, get_user_by_account, get_user_by_id, get_users, update_user, 
    delete_user, authenticate_user, update_last_login
)
from models.user import User

router = APIRouter(prefix="/api/users", tags=["用户管理"])

@router.post("/", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def register_user(user: UserCreate, db: Session = Depends(get_db)):
    """用户注册"""
    db_user = get_user_by_account(db, user.account)
    if db_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="账号已存在"
        )
    return create_user(db, user)


@router.post("/login", response_model=Token)
async def login(credentials: UserLogin, db: Session = Depends(get_db)):
    """用户登录"""
    user = authenticate_user(db, credentials.account, credentials.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="账号或密码错误"
        )
    update_last_login(db, user.user_id)
    from core.security import create_access_token
    access_token = create_access_token(data={"sub": user.account})
    return {"access_token": access_token, "token_type": "bearer"}


@router.get("/me", response_model=UserResponse)
def read_users_me(current_user: User = Depends(get_current_user)):
    """获取当前用户信息"""
    return current_user

@router.get("/{user_id}", response_model=UserResponse)
def read_user(user_id: int, db: Session = Depends(get_db)):
    """获取指定用户信息"""
    db_user = get_user_by_id(db, user_id)
    if db_user is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    return db_user

@router.put("/{user_id}", response_model=UserResponse)
def update_user_info(
    user_id: int, 
    user_update: UserUpdate, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """更新用户信息"""
    if current_user.user_id != user_id:
        raise HTTPException(status_code=403, detail="权限不足")
    
    db_user = update_user(db, user_id, user_update)
    if db_user is None:
        raise HTTPException(status_code=404, detail="用户不存在")
    return db_user

@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user_account(
    user_id: int, 
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """删除用户账户"""
    if current_user.user_id != user_id:
        raise HTTPException(status_code=403, detail="权限不足")
    
    if not delete_user(db, user_id):
        raise HTTPException(status_code=404, detail="用户不存在")

@router.post("/token", response_model=Token)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    """OAuth2 标准登录端点"""
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="账号或密码错误",
            headers={"WWW-Authenticate": "Bearer"},
        )
    update_last_login(db, user.user_id)
    access_token = create_access_token(data={"sub": user.account})
    return {"access_token": access_token, "token_type": "bearer"}
