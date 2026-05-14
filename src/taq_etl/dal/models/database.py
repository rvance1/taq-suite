from pydantic import BaseModel
import datetime as dt
from .taq_file import TaqFile, TaqType


class Database(BaseModel):
    raw_taq_path: str | None = None
    output_path: str | None = None

    def connect(self, raw_taq_path: str, output_path: str) -> None:
        self.raw_taq_path = raw_taq_path
        self.output_path = output_path

    def is_connected(self) -> bool:
        return self.raw_taq_path is not None and self.output_path is not None
    
    def get_taq_file(self, date: dt.date, type: TaqType) -> TaqFile:
        if not self.is_connected():
            raise ValueError("Database is not connected")
        return TaqFile(root_path=self.raw_taq_path + "/taq", date=date, type=type, letter="A")