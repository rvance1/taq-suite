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

    def get_taq_file(self, date: dt.date, type: TaqType, letter: str | None = "A") -> TaqFile:
        return self.database.get_taq_file(date, type, letter=letter)
    
    def load_taq_index(self, date: dt.date, type: TaqType, letter: str | None = "A") -> pl.DataFrame:
        """Loads the TAQ index for a given date and type. Ex: date=dt.date(1998, 1, 1), type=TaqType.QUOTE"""
        taq_file = self.get_taq_file(date, type, letter=letter)
        with lz4.frame.open(taq_file.idx_path, 'rb') as f:
            raw = f.read()
            idx_data = np.frombuffer(raw, dtype=IDX_DTYPE)
            
        date_cast: pl.Expr
        if date < dt.date(1999, 12,1):
            date_cast = pl.col("date").cast(pl.String).str.pad_start(6, "0").str.to_date(format="%y%m%d")
        else:
            date_cast = pl.col("date").cast(pl.String).str.pad_start(6, "0").str.to_date(format="%Y%m%d")

        return pl.DataFrame({
            "ticker": [t.decode('latin-1').strip() for t in idx_data['ticker']],
            "date": idx_data['date'],
            "start_idx": idx_data['start_idx'],
            "end_idx": idx_data['end_idx']
        }).with_columns(
            date_cast
        )
    
    @staticmethod
    def detect_record_size(bin_path: Path, idx_df: pl.DataFrame) -> int:
        """Calculates the exact byte size of a single record for a given day."""
        with lz4.frame.open(bin_path, 'rb') as f:
            raw_bytes = f.read()
            
        total_bytes = len(raw_bytes)
        
        total_records = int((idx_df["end_idx"] - idx_df["start_idx"] + 1).sum())
        
        if total_bytes % total_records != 0:
            raise ValueError(f"Corrupted data or missing records: {total_bytes} bytes does not divide evenly by {total_records} records.")
            
        return total_bytes // total_records
    
    def load_data_for_day(self, date: dt.date, type: TaqType, letter: str | None = "A") -> pl.DataFrame:
        taq_file = self.get_taq_file(date, type, letter=letter)
        idx_df = self.load_taq_index(date, type, letter=letter)
        
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
    
    def process_month_chunk(self, year: int, month: int, type: TaqType, letter: str | None = "A") -> None:
        """Processes a chunk of an entire month of TAQ data by decompressing the file once."""
        
        # Use dummy date to resolve the file paths 
        dummy_date = dt.date(year, month, 1)
        taq_file = self.get_taq_file(dummy_date, type, letter)
        idx_df = self.load_taq_index(dummy_date, type, letter)
        
        if idx_df.is_empty():
            return

        # Decompress binary payload EXACTLY once to avoid lz4 overhead
        with lz4.frame.open(taq_file.bin_path, 'rb') as f:
            raw_bytes = f.read()
            
        total_bytes = len(raw_bytes)
        total_records = int((idx_df["end_idx"] - idx_df["start_idx"] + 1).sum())
        
        if total_bytes % total_records != 0:
            raise ValueError(f"Corrupted data or missing records: {total_bytes} bytes does not divide evenly by {total_records} records.")
            
        record_size = total_bytes // total_records
        bin_dtype = get_bin_dtype(type, record_size)

        unique_dates = idx_df["date"].drop_nulls().unique().sort().to_list()

        # Iterate and build one daily DataFrame at a time to optimize RAM
        for date in unique_dates:
            meta = idx_df.filter(pl.col("date") == date).sort("start_idx")
            
            if meta.is_empty():
                continue
            
            min_idx = meta["start_idx"].min()
            max_idx = meta["end_idx"].max()
            
            start_byte = (min_idx - 1) * record_size if min_idx > 0 else 0
            end_byte = max_idx * record_size
            
            day_raw = raw_bytes[start_byte:end_byte]
            bin_np = np.frombuffer(day_raw, dtype=bin_dtype)

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
                
            df = (
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
            
            if not df.is_empty():
                self.write_file_for_day(date=date, df=df, taq_type=type)

    def get_available_letters_for_month(self, year: int, month: int, type: TaqType) -> list[str]:
        import string
        letters = []
        dummy_date = dt.date(year, month, 1)
        for letter in string.ascii_uppercase:
            try:
                taq_file = self.get_taq_file(dummy_date, type, letter=letter)
                if Path(taq_file.bin_path).exists():
                    letters.append(letter)
                else:
                    break
            except Exception:
                break
        return letters
    
    def process_month(self, year: int, month: int, type: TaqType) -> None:
        """Processes an entire month of TAQ data by detecting all chunks and processing them iteratively."""
        if year >= 1996:
            letters = self.get_available_letters_for_month(year, month, type)
            if not letters:
                print(f"No files found for {year}-{month:02d} {type}")
                return
            for letter in letters:
                print(f"Processing chunk {letter} for {year}-{month:02d} {type}...")
                self.process_month_chunk(year=year, month=month, type=type, letter=letter)
        else:
            self.process_month_chunk(year=year, month=month, type=type, letter=None)
    
    def upsert_as_parquet(self, df: pl.DataFrame, path: Path) -> None:
        if path.exists():
            existing_df = pl.read_parquet(path)
            combined_df = (
                pl.concat([existing_df, df])
                .unique(subset=["datetime", "ticker"])
                .sort(["ticker", "datetime"])
            )
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            combined_df = df
        
        combined_df.write_parquet(path)
        
    def write_file_for_day(self, date: dt.date, df: pl.DataFrame, taq_type: TaqType) -> None:
        
        match taq_type:
            case TaqType.QUOTE:
                folder = "quote"
            case TaqType.TRADE:
                folder = "trade"
            case TaqType.MASTER:
                folder = "master"

        path = Path(f"{self.database.output_path}/taq/{folder}/{date.year}/{date.month:02d}/{date.strftime("%Y-%m-%d")}.parquet")
        if not self.database.is_connected():
            raise ValueError("Database is not connected")
        
        self.upsert_as_parquet(df, path)

    def hexdump_file(self, bin_path, num_bytes=160):
        with lz4.frame.open(bin_path, 'rb') as f:
            raw = f.read(num_bytes)
            
        print("--- RAW HEX DUMP (TICKER 'A' - BYTE 0) ---")
        for i in range(0, len(raw), 16):
            chunk = raw[i:i+16]
            # Format as Hex
            hex_str = " ".join([f"{b:02X}" for b in chunk])
            # Format as ASCII (dots for non-printable characters)
            ascii_str = "".join([chr(b) if 32 <= b <= 126 else "." for b in chunk])
            print(f"{i:04X}  {hex_str:<47}  |{ascii_str}|")