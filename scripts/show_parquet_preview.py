import polars as pl

print(pl.read_parquet('data/interim/taq/quote/1999/01/19990104.parquet').head())