import datetime as dt
from enum import StrEnum
from pathlib import Path
from pydantic import BaseModel, computed_field


class TaqType(StrEnum):
    QUOTE = "CQ"
    TRADE = "CT"
    MASTER = "M"


class TaqMonth(BaseModel):
    date: dt.date
    type: TaqType
    
    def __get_base_path(self) -> str:
        y = self.date.year
        m = self.date.month
        if self.date < dt.date(1999, 12, 1):
            return f"{y}/{self.type}{y % 100:02d}{m:02d}"
        else:
            return f"{y}/{self.type[-1]}{y}{m:02d}"
    
    @computed_field
    @property
    def idx_path(self) -> Path:
        return f"{self.__get_base_path()}.IDX.lz4"
    
    @computed_field
    @property
    def bin_path(self) -> Path:
        return f"{self.__get_base_path()}.BIN.lz4"