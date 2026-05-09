from pydantic import BaseModel
import datetime as dt

from .taq_table import TaqTable

class Database(BaseModel):
    root_path: str | None = None

    def connect(self, path: str) -> None:
        self.root_path = path
    
    def is_connected(self) -> bool:
        return self.root_path is not None

    def get_taq_table(self) -> TaqTable:
        if not self.is_connected():
            raise ValueError("Database is not connected")
        return TaqTable(root_dir=self.root_path + "/interim/taq")