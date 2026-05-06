from pydantic import BaseModel
import datetime as dt
import lz4.frame
import numpy as np
import polars as pl
from tqdm import tqdm
from pathlib import Path

from taq_etl.dal.models.database import Database
from taq_etl.dal.models.taq_month import TaqMonth, TaqType
from taq_etl.dal.models.byte_schema import idx_dtype, bin_dtype


class RawTaqDao(BaseModel):
    database: Database

    def get_taq_month(self, date: dt.date, type: TaqType) -> TaqMonth:
        return self.database.get_taq_month(date, type)
    
    def load_taq_index(self, date: dt.date, type: TaqType) -> pl.DataFrame:
        """Loads the TAQ index for a given date and type. Ex: date=dt.date(1998, 1, 1), type=TaqType.QUOTE"""
        taq_month = self.get_taq_month(date, type)
        with lz4.frame.open(taq_month.idx_path, 'rb') as f:
            raw = f.read()
            idx_data = np.frombuffer(raw, dtype=idx_dtype)
            
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
    
    def load_data_for_day(self, date: dt.date, type: TaqType) -> pl.DataFrame:
        taq_month = self.get_taq_month(date, type)
        idx_df = self.load_taq_index(date, type)
        
        meta = idx_df.filter(pl.col("date") == date).sort("start_idx")
        
        if meta.is_empty():
            return pl.DataFrame()
        
        min_idx = meta["start_idx"].min()
        max_idx = meta["end_idx"].max()
        total_records = max_idx - min_idx + 1
        
        start_byte = (min_idx - 1) * 23 if min_idx > 0 else 0
        read_size = total_records * 23

        with lz4.frame.open(taq_month.bin_path, 'rb') as f:
            try:
                f.seek(start_byte)
                raw_bin = f.read(read_size)
            except (AttributeError, IOError):
                # If seek fails, we decompress up to the start byte once
                f.read(start_byte)
                raw_bin = f.read(read_size)

        bin_np = np.frombuffer(raw_bin, dtype=bin_dtype)
        
        # Use numpy to magically assign tickers to rows without loops
        # This repeats each ticker string exactly (end - start + 1) times
        counts = (meta["end_idx"] - meta["start_idx"] + 1).to_numpy()
        tickers = meta["ticker"].to_numpy()
        ticker_col = np.repeat(tickers, counts)
        
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
        
        path = Path(f"{self.database.get_interim_path()}/taq/{taq_type.value}/{date.year}/{date.month:02d}/{date.strftime("%Y%m%d")}.parquet")
        if not self.database.is_connected():
            return ValueError("Database is not connected")
        
        path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(path)