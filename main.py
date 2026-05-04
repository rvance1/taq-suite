import lz4.frame
import numpy as np
import polars as pl
import os

# 1. DEFINITIVE LITTLE-ENDIAN INDEX (22 bytes)
idx_dtype = np.dtype([
    ('ticker',  'S10'),
    ('date',    '<i4'), 
    ('start',   '<i4'), 
    ('end_idx', '<i4')  
])

# 2. DEFINITIVE LITTLE-ENDIAN BIN SCHEMA (23 bytes)
bin_dtype = np.dtype([
    ('time',     '<i4'),
    ('bid',      '<i4'),
    ('ask',      '<i4'),
    ('seq',      '<i4'),
    ('bid_size', '<i2'), # Fixed: Sizes are 2 bytes!
    ('ask_size', '<i2'), # Fixed: Sizes are 2 bytes!
    ('mode',     '<i2'), # Stored as a 2-byte integer condition code
    ('ex',       'S1')
])

def load_taq_index(path):
    with lz4.frame.open(path, 'rb') as f:
        raw = f.read()
        idx_data = np.frombuffer(raw, dtype=idx_dtype)
        
    return pl.DataFrame({
        "ticker": [t.decode('latin-1').strip() for t in idx_data['ticker']],
        "date": idx_data['date'],
        "start": idx_data['start'],
        "end_idx": idx_data['end_idx']
    })

def get_ticker_data(ticker, idx_df, bin_path):
    meta = idx_df.filter(pl.col("ticker") == ticker)
    if meta.is_empty():
        return f"Ticker {ticker} not found."
    
    row = meta.row(0)
    date_int = row[1]
    start_idx = row[2]
    end_idx = row[3]
    
    num_records = end_idx - start_idx + 1
    
    start_byte = (start_idx - 1) * 23 if start_idx > 0 else 0
    read_size = num_records * 23
    
    with lz4.frame.open(bin_path, 'rb') as f:
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

def main():
    # --- EXECUTION ---
    idx_path = 'extra/taq/taq1993/CQ9301.IDX.lz4'
    bin_path = 'extra/taq/taq1993/CQ9301.BIN.lz4'
    output_path = 'data/interim/taq/1993/aa_01.parquet'

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    idx_df = load_taq_index(idx_path)
    df_aa = get_ticker_data('AA', idx_df, bin_path)

    if isinstance(df_aa, pl.DataFrame):
        df_aa.write_parquet(output_path)
        print(f"\nSuccessfully saved 'AA' data to {output_path}")
        print(df_aa)
    else:
        print(df_aa)


if __name__ == "__main__":
    main()
