from pydantic import BaseModel
import datetime as dt
from .taq_file import TaqFile, TaqType


class Database(BaseModel):
    root_path: str | None = None

    def connect(self, path: str) -> None:
        self.root_path = path
    
    def is_connected(self) -> bool:
        return self.root_path is not None

    def get_raw_taq_path(self) -> str:
        if not self.is_connected():
            raise ValueError("Database is not connected")
        return f"{self.root_path}/raw"
    
    def get_interim_path(self) -> str:
        if not self.is_connected():
            raise ValueError("Database is not connected")
        return f"{self.root_path}/interim"
    
    def get_taq_file(self, date: dt.date, type: TaqType) -> TaqFile:
        if not self.is_connected():
            raise ValueError("Database is not connected")
        return TaqFile(root_path=self.get_raw_taq_path() + "/taq", date=date, type=type)