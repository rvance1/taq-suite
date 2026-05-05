from pydantic import BaseModel
import datetime as dt
from .taq_month import TaqMonth, TaqType


class Database(BaseModel):
    root_path: str | None = None

    def connect(self, path: str) -> None:
        self.root_path = path
    
    def is_connected(self) -> bool:
        return self.root_path is not None
    
    def get_taq_month(self, date: dt.date, type: TaqType) -> TaqMonth:
        if not self.is_connected():
            raise ValueError("Database is not connected")
        return TaqMonth(root_path=self.root_path, date=date, type=type)