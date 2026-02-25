
from fastapi import APIRouter, Depends, HTTPException, status  # 添加status导入
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from core.database import get_db
from core.security import verify_password, create_access_token
from core.auth_dependency import get_current_user
from schemas.user import UserRegister, UserLogin, TokenResponse, UserOut
import crud.user as crud_user

router = APIRouter(prefix="/users", tags=["用户系统"])

@router.post("/register", response_model=UserOut)
def register(user_in: UserRegister, db: Session = Depends(get_db)):
    if crud_user.get_user_by_account(db, user_in.account):
        raise HTTPException(status_code=400, detail="Account already exists")
    return crud_user.create_user(db, user_in)

@router.post("/login", response_model=TokenResponse)
def login(
    # 使用 OAuth2PasswordRequestForm 替代原来的 UserLogin 模型
    # 这会让接口同时支持 Swagger 的表单提交
    form_data: OAuth2PasswordRequestForm = Depends(), 
    db: Session = Depends(get_db)
):
    # 注意：这里要用 form_data.username，它对应你数据库里的 account
    user = crud_user.get_user_by_account(db, form_data.username)
    
    if not user or not verify_password(form_data.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,  # 修复：添加status前缀
            detail="用户名或密码错误",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # 生成 Token
    token = create_access_token({"sub": user.account})
    crud_user.update_last_login(db, user.user_id)
    
    # 返回格式必须包含 access_token 和 token_type
    return {"access_token": token, "token_type": "bearer"}

@router.get("/me", response_model=UserOut)
def get_me(current_user: UserOut = Depends(get_current_user)):
    return current_user