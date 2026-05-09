from pydantic import BaseModel
import datetime as dt
import polars as pl
from pathlib import Path
from enum import StrEnum


class TaqType(StrEnum):
    QUOTE = "CQ"
    TRADE = "CT"
    MASTER = "M"


class TaqTable(BaseModel):
    root_dir: str

    def __get_file_path(self, date: dt.date) -> Path:
        base = f"{date.year}/{date.month:02d}"
        file_name = date.strftime("%Y-%m-%d.parquet")
        return Path(f"{self.root_dir}/{base}/{file_name}")

    def __get_file_paths(self, start: dt.date, end: dt.date) -> list[Path]:
        files_to_scan = []
        current_date = start

        while current_date <= end:
            file_path = self.__get_file_path(current_date)

            if file_path.exists():
                files_to_scan.append(Path(str(file_path)))

            current_date += dt.timedelta(days=1)

        return files_to_scan
    
    def scan_date(self, date: dt.date) -> pl.LazyFrame:
        path = self.__get_file_path(date)
        return pl.scan_parquet(path)

    def scan_range(self, start_date: dt.date, end_date: dt.date) -> pl.LazyFrame:
        paths = self.__get_file_paths(start_date, end_date)
        return pl.scan_parquet(paths)