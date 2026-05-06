import os
import datetime as dt
import polars as pl
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from taq_etl import RawTaqDao, Database, TaqType

os.environ["POLARS_MAX_THREADS"] = "2"

def process_single_day(date):
    """
    Worker function: This runs in its own process.
    We re-initialize the DAO inside the worker to avoid pickling issues.
    """
    database = Database(root_path="data")
    dao = RawTaqDao(database=database)
    
    try:
        df = dao.load_data_for_day(date=date, type=TaqType.QUOTE)
        if df is not None:
            dao.write_file_for_day(date=date, df=df, taq_type=TaqType.QUOTE)
        return f"Done: {date}"
    except Exception as e:
        return f"Error on {date}: {e}"

if __name__ == "__main__":
    # 1. Define your date range
    start_date = dt.date(1993, 1, 1)
    end_date = dt.date(1993, 1, 31)
    date_list = []
    
    curr = start_date
    while curr <= end_date:
        date_list.append(curr)
        curr += dt.timedelta(days=1)

    # 2. Run in parallel
    # max_workers: Start with 4 or 8. Don't go too high or Polars threads 
    # will fight each other for CPU cache.
    with ProcessPoolExecutor(max_workers=4) as executor:
        # Use tqdm to track progress across the processes
        list(tqdm(executor.map(process_single_day, date_list), total=len(date_list)))