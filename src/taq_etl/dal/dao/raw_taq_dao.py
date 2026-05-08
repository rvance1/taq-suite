from pydantic import BaseModel
import datetime as dt
import lz4.frame
import numpy as np
import polars as pl
from pathlib import Path

from taq_etl.dal.models.database import Database
from taq_etl.dal.models.taq_file import TaqFile, TaqType
from taq_etl.dal.models.byte_schema import get_bin_dtype, IDX_DTYPE


class RawTaqDao(BaseModel):
    database: Database

    def get_taq_file(self, date: dt.date, type: TaqType) -> TaqFile:
        return self.database.get_taq_file(date, type)
    
    def load_taq_index(self, date: dt.date, type: TaqType) -> pl.DataFrame:
        """Loads the TAQ index for a given date and type. Ex: date=dt.date(1998, 1, 1), type=TaqType.QUOTE"""
        taq_file = self.get_taq_file(date, type)
        with lz4.frame.open(taq_file.idx_path, 'rb') as f:
            raw = f.read()
            idx_data = np.frombuffer(raw, dtype=IDX_DTYPE)
            
        return pl.DataFrame({
            "ticker": [t.decode('latin-1').strip() for t in idx_data['ticker']],
            "date": idx_data['date'],
            "start_idx": idx_data['start_idx'],
            "end_idx": idx_data['end_idx']
        }).with_columns(
            pl.col("date")
            .cast(pl.String) 
            .str.pad_start(6, "0")  
            .str.to_date(format="%y%m%d") 
            .alias("date")
        )
    
    def detect_record_size(self, bin_path: Path, idx_df: pl.DataFrame) -> int:
        """Calculates the exact byte size of a single record for a given day."""
        with lz4.frame.open(bin_path, 'rb') as f:
            raw_bytes = f.read()
            
        total_bytes = len(raw_bytes)
        
        total_records = int((idx_df["end_idx"] - idx_df["start_idx"] + 1).sum())
        
        if total_bytes % total_records != 0:
            raise ValueError(f"Corrupted data or missing records: {total_bytes} bytes does not divide evenly by {total_records} records.")
            
        return total_bytes // total_records
    
    def load_data_for_day(self, date: dt.date, type: TaqType) -> pl.DataFrame:
        taq_file = self.get_taq_file(date, type)
        idx_df = self.load_taq_index(date, type)
        
        meta = idx_df.filter(pl.col("date") == date).sort("start_idx")
        
        if meta.is_empty():
            return pl.DataFrame()
        
        min_idx = meta["start_idx"].min()
        max_idx = meta["end_idx"].max()
        total_records = max_idx - min_idx + 1
        
        record_size = self.detect_record_size(taq_file.bin_path, idx_df)

        bin_dtype = get_bin_dtype(type, record_size)

        start_byte = (min_idx - 1) * record_size if min_idx > 0 else 0
        read_size = total_records * record_size

        with lz4.frame.open(taq_file.bin_path, 'rb') as f:
            try:
                f.seek(start_byte)
                raw_bin = f.read(read_size)
            except (AttributeError, IOError):
                f.read(start_byte)
                raw_bin = f.read(read_size)

        bin_np = np.frombuffer(raw_bin, dtype=bin_dtype)

        counts = (meta["end_idx"] - meta["start_idx"] + 1).to_numpy()
        tickers = meta["ticker"].to_numpy()
        ticker_col = np.repeat(tickers, counts)

        if type == TaqType.TRADE:
            packed_data = pl.DataFrame({
                "date": [date] * len(bin_np),
                "ticker": ticker_col,
                "time": bin_np['time'],
                "price": bin_np['price'] / 100000.0,
                "volume": bin_np['volume'],
                "seq": bin_np['seq'],
                "cond": bin_np['cond'],
                "sale": [s.decode('latin-1') for s in bin_np['sale']],
                "ex": [e.decode('latin-1') for e in bin_np['ex']],
            })
        else:
            packed_data = pl.DataFrame({
                "date": [date] * len(bin_np),
                "ticker": ticker_col,
                "time": bin_np['time'],
                "bid": bin_np['bid'] / 100000.0,
                "ask": bin_np['ask'] / 100000.0,
                "bid_size": bin_np['bid_size'],
                "ask_size": bin_np['ask_size'],
                "seq": bin_np['seq'],
                "mode": bin_np['mode'],
                "ex": [e.decode('latin-1') for e in bin_np['ex']],
            })
            
        return (
            packed_data
            .with_columns(
                (
                    pl.col("date").cast(pl.Datetime) + pl.duration(seconds=pl.col("time"))
                )
                .alias("datetime")
            )
            .select(
                "datetime", 
                pl.all().exclude(["datetime", "date", "time"])
            )
        )
        
    def write_file_for_day(self, date: dt.date, df: pl.DataFrame, taq_type: TaqType) -> None:
        
        match taq_type:
            case TaqType.QUOTE:
                folder = "quote"
            case TaqType.TRADE:
                folder = "trade"
            case TaqType.MASTER:
                folder = "master"

        path = Path(f"{self.database.get_interim_path()}/taq/{folder}/{date.year}/{date.month:02d}/{date.strftime("%Y%m%d")}.parquet")
        if not self.database.is_connected():
            return ValueError("Database is not connected")
        
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)