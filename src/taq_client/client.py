import polars as pl
import datetime as dt
from pathlib import Path
from pydantic import BaseModel, Field, field_validator, ConfigDict
from typing import List, Union

class TaqClient:
    def __init__(self, db_path: str | None = None):
        import os
        try:
            self.db_path = Path(db_path or os.getenv("TAQ_DB_PATH"))
        except Exception as e:
            raise ValueError(f"TAQ_DB_PATH environment variable is not set and no db_path was provided. Please set TAQ_DB_PATH or provide a db_path argument. Original error: {e}")
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database path {self.db_path} does not exist.")