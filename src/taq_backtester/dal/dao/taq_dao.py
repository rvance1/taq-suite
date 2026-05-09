from pydantic import BaseModel
from pathlib import Path
import datetime as dt
import polars as pl

from taq_backtester.dal.models.database import Database
from taq_backtester.dal.models.taq_table import TaqTable, TaqType

class TaqDao(BaseModel):
    database: Database

    def get_table(self) -> TaqTable:
        return self.database.get_taq_table()
    
    def load_by_date(self, date: dt.date, type: TaqType) -> pl.DataFrame:
        table = self.get_table()
        return table.scan_date(date, type).collect()
    
    def load_by_range() -> pl.DataFrame:
        pass