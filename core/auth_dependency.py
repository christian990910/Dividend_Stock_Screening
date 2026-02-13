from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from sqlalchemy.orm import Session
from core.config import settings
from core.database import get_db
from models.user import User

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/users/login")

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        
        account: str = payload.get("sub")
        print(f"DEBUG: Decoded account from token: {account}")
        if account is None:
            raise credentials_exception
    except JWTError as e:
        print(f"DEBUG: JWT Decode Error: {str(e)}")
        raise credentials_exception
        
    user = db.query(User).filter(User.account == account).first()
    if user is None:
        print(f"DEBUG: User {account} not found in database")
        raise credentials_exception
    if not user.is_active:
        raise HTTPException(status_code=403, detail="Inactive user")
    return user