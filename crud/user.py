from sqlalchemy.orm import Session
from models.user import User
from schemas.user import UserCreate, UserUpdate
from core.security import get_password_hash, verify_password
from typing import List, Optional

def create_user(db: Session, user: UserCreate) -> User:
    """创建用户"""
    db_user = User(
        account=user.account,
        nickname=user.nickname,
        password_hash=get_password_hash(user.password),
        email=user.email
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def get_user_by_account(db: Session, account: str) -> Optional[User]:
    """根据账号获取用户"""
    return db.query(User).filter(User.account == account).first()

def get_user_by_id(db: Session, user_id: int) -> Optional[User]:
    """根据ID获取用户"""
    return db.query(User).filter(User.user_id == user_id).first()

def get_users(db: Session, skip: int = 0, limit: int = 100) -> List[User]:
    """获取用户列表"""
    return db.query(User).offset(skip).limit(limit).all()

def update_user(db: Session, user_id: int, user_update: UserUpdate) -> Optional[User]:
    """更新用户信息"""
    db_user = db.query(User).filter(User.user_id == user_id).first()
    if db_user:
        update_data = user_update.dict(exclude_unset=True)
        for key, value in update_data.items():
            setattr(db_user, key, value)
        db.commit()
        db.refresh(db_user)
    return db_user

def delete_user(db: Session, user_id: int) -> bool:
    """删除用户"""
    db_user = db.query(User).filter(User.user_id == user_id).first()
    if db_user:
        db.delete(db_user)
        db.commit()
        return True
    return False

def authenticate_user(db: Session, account: str, password: str) -> Optional[User]:
    """用户认证"""
    user = get_user_by_account(db, account)
    if not user or not verify_password(password, user.password_hash):
        return None
    return user

def update_last_login(db: Session, user_id: int) -> None:
    """更新最后登录时间"""
    from datetime import datetime
    db_user = db.query(User).filter(User.user_id == user_id).first()
    if db_user:
        db_user.last_login_at = datetime.now()
        db.commit()