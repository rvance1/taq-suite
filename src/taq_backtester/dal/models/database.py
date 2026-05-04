from pydantic import BaseModel
import datetime as dt
from .taq_month import TaqMonth, TaqType

class Database(BaseModel):
    root_path: str

    def get_taq_month(self, date: dt.date, type: TaqType) -> TaqMonth:
        return TaqMonth(root_path=self.root_path, date=date, type=type)