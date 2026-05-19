from pydantic import BaseModel
import datetime as dt
import lz4.frame
import numpy as np
import polars as pl
from pathlib import Path

from taq_etl.dal.models.database import Database


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