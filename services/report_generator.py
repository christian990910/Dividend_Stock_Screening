import os
import pandas as pd
from sqlalchemy.orm import Session
from crud.stock import get_analysis_by_user

class ReportGenerator:
    @staticmethod
    def generate_user_csv(db: Session, user_id: int):
        """为特定用户生成分析报告文件"""
        results = get_analysis_by_user(db, user_id)
        if not results:
            return None
        
        data = []
        for r in results:
            data.append({
                "股票代码": r.stock_code,
                "股票名称": r.stock_name,
                "分析日期": str(r.analysis_date),
                "综合评分": r.total_score,
                "投资建议": r.suggestion,
                "最新价": r.latest_price,
                "股息率%": r.dividend_yield,
                "ROE%": r.roe
            })
        
        df = pd.DataFrame(data)
        out_dir = "outputs"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            
        file_path = os.path.join(out_dir, f"report_user_{user_id}.csv")
        df.to_csv(file_path, index=False, encoding="utf_8_sig")
        return file_path