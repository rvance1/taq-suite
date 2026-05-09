from pydantic import BaseModel
import datetime as dt
import polars as pl
from pathlib import Path
from enum import StrEnum


class TaqType(StrEnum):
    QUOTE = "quote"
    TRADE = "trade"
    MASTER = "master"


class TaqTable(BaseModel):
    root_dir: str

    def __get_file_paths_by_day(self, date: dt.date, type: TaqType) -> list[Path]:
        """Returns all parquet files for a given date, handling both single files and folders."""
        base_dir = Path(self.root_dir) / str(type) / str(date.year) / f"{date.month:02d}"
        date_str = date.strftime("%Y-%m-%d")

        single_file = base_dir / f"{date_str}.parquet"
        if single_file.is_file():
            return [single_file]

        folder = base_dir / date_str
        if folder.is_dir():
            return list(folder.glob("*.parquet"))

        return []

    def __get_file_paths(self, start: dt.date, end: dt.date, type: TaqType) -> list[Path]:
        files_to_scan = []
        current_date = start

        while current_date <= end:
            files_to_scan.extend(self.__get_file_paths_by_day(current_date, type))
            current_date += dt.timedelta(days=1)

        return files_to_scan
    
    def scan_date(self, date: dt.date, type: TaqType) -> pl.LazyFrame:
        paths = self.__get_file_paths_by_day(date, type)
        return pl.scan_parquet(paths)

    def scan_range(self, start_date: dt.date, end_date: dt.date, type: TaqType) -> pl.LazyFrame:
        paths = self.__get_file_paths(start_date, end_date, type)
        if not paths:
            raise FileNotFoundError(f"No data found for range {start_date} to {end_date}")
        return pl.scan_parquet(paths)