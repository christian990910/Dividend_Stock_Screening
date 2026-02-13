from sqlalchemy.orm import Session
from models.user import User
from schemas.user import UserRegister
from core.security import hash_password

def get_user_by_id(db: Session, user_id: int):
    return db.query(User).filter(User.user_id == user_id).first()

def get_user_by_account(db: Session, account: str):
    return db.query(User).filter(User.account == account).first()

def create_user(db: Session, user_in: UserRegister):
    db_user = User(
        account=user_in.account,
        nickname=user_in.nickname,
        email=user_in.email,
        password_hash=hash_password(user_in.password),
        phone=user_in.phone,
        avatar_url=user_in.avatar_url
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user

def update_last_login(db: Session, user_id: int):
    import datetime
    user = get_user_by_id(db, user_id)
    if user:
        user.last_login_at = datetime.datetime.now()
        db.commit()