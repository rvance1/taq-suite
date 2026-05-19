from pydantic import BaseModel
import datetime as dt
import polars as pl
from enum import StrEnum
from pathlib import Path

from taq_etl.dal.models.database import Database


class CrspColumn(StrEnum):
    DATE = 'date'
    PERMNO = 'permno'
    TICKER = 'ticker'
    SHARE_CLASS = 'share_class'


class CrspDao(BaseModel):
    database: Database

    def load_crsp_by_range(self, start_date: dt.date, end_date: dt.date) -> pl.DataFrame:
        crsp_file_path = self.database.get_crsp_masterfile_path()
        df = pl.scan_parquet(crsp_file_path)
        return (
            df.filter(
                (pl.col("date") >= start_date) & (pl.col("date") <= end_date)
            )
            .sort(["date", "permno"])
            .collect()
        )
    
    def load_crsp_by_year(self, year: int) -> pl.DataFrame:
        crsp_file_path = self.database.get_crsp_masterfile_path()
        df = pl.scan_parquet(crsp_file_path)
        return (
            df.filter(pl.col("date").dt.year() == year)
            .sort(["date", "permno"])
            .collect()
        )
    
    def save_parquet_interim(self, df: pl.DataFrame, path: str, name: str) -> None:
        base_path = Path(self.database.output_path) / path
        base_path.mkdir(parents=True, exist_ok=True)

        file_path = base_path / f"{name}.parquet"
        df.write_parquet(file_path)