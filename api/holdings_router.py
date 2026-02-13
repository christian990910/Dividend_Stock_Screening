from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import List
from core.database import get_db
from core.auth_dependency import get_current_user
from schemas.holdings import HoldingCreate, HoldingOut
import crud.holdings as crud_holdings

router = APIRouter(prefix="/holdings", tags=["持仓管理"])

@router.post("/buy", response_model=HoldingOut)
def buy_stock(h: HoldingCreate, db: Session = Depends(get_db), current_user = Depends(get_current_user)):
    return crud_holdings.create_holding_record(db, current_user.user_id, h)

@router.get("/my", response_model=List[HoldingOut])
def list_my_holdings(db: Session = Depends(get_db), current_user = Depends(get_current_user)):
    return crud_holdings.get_user_holdings(db, current_user.user_id)