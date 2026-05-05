from pydantic import BaseModel
import datetime as dt
import lz4.frame
import numpy as np
import polars as pl

from taq_backtester.dal.models.database import Database
from taq_backtester.dal.models.taq_month import TaqMonth, TaqType
from taq_backtester.dal.models.byte_schema import idx_dtype, bin_dtype


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
            "start": idx_data['start'],
            "end_idx": idx_data['end_idx']
        })
    
    def load_ticker_data(self, ticker: str, date: dt.date, type: TaqType) -> pl.DataFrame:
        taq_month = self.get_taq_month(date, type)
        idx_df = self.load_taq_index(date, type)
        meta = idx_df.filter(pl.col("ticker") == ticker)
        if meta.is_empty():
            return pl.DataFrame()
        
        row = meta.row(0)
        date_int = row[1]
        start_idx = row[2]
        end_idx = row[3]
        
        num_records = end_idx - start_idx + 1
        
        start_byte = (start_idx - 1) * 23 if start_idx > 0 else 0
        read_size = num_records * 23
        
        with lz4.frame.open(taq_month.bin_path, 'rb') as f:
            try:
                f.seek(start_byte)
                raw_bin = f.read(read_size)
            except (AttributeError, IOError):
                f.read(start_byte)
                raw_bin = f.read(read_size)

        bin_np = np.frombuffer(raw_bin, dtype=bin_dtype)
        
        return pl.DataFrame({
            "date": [date_int] * len(bin_np),
            "time": bin_np['time'],
            "bid": bin_np['bid'] / 100000.0,
            "ask": bin_np['ask'] / 100000.0,
            "bid_size": bin_np['bid_size'],
            "ask_size": bin_np['ask_size'],
            "seq": bin_np['seq'],
            "mode": bin_np['mode'],
            "ex": [e.decode('latin-1') for e in bin_np['ex']],
            "ticker": [ticker] * len(bin_np)
        })
    
    def write_ticker_df(self, ticker: str, date: dt.date, df: pl.DataFrame) -> None:
        path = self.database.get_interim_path() + f"/taq/{date.year}/{ticker}_{date.month:02d}.parquet"
        df.to_parquet(path)