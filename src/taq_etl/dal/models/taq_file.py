import datetime as dt
from enum import StrEnum
from pathlib import Path
from pydantic import BaseModel, FilePath, computed_field


class TaqType(StrEnum):
    QUOTE = "CQ"
    TRADE = "CT"
    MASTER = "M"


class TaqFile(BaseModel):
    root_path: str
    date: dt.date
    type: TaqType
    letter: str | None = None
    
    def __create_prefix(self) -> str:
        y = self.date.year
        m = self.date.month
        if self.date < dt.date(1999, 12, 1):
            return f"{y}/{self.type}{y % 100:02d}{m:02d}"
        else:
            return f"{y}/{self.type[-1]}{y}{m:02d}"
    
    def __create_base_path(self) -> str:
        if self.date.year > 1995:
            if not self.letter:
                raise ValueError("Letter is required for dates after 1995")
            return f"{self.root_path}/{self.__create_prefix()}{self.letter}"
        else:
            return f"{self.root_path}/{self.__create_prefix()}"
    
    @computed_field
    @property
    def idx_path(self) -> FilePath:
        return Path(f"{self.__create_base_path()}.IDX.lz4")
    
    @computed_field
    @property
    def bin_path(self) -> FilePath:
        return Path(f"{self.__create_base_path()}.BIN.lz4")