from pydantic import BaseModel, PrivateAttr
import datetime as dt
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

from taq_etl.dal.dao.raw_taq_dao import RawTaqDao
from taq_etl.dal.models.taq_file import TaqType
from taq_etl.dal.models.database import Database

class RawTaqService(BaseModel):
    database: Database
    dao: RawTaqDao = PrivateAttr()

    def model_post_init(self, __context) -> None:
        self.dao = RawTaqDao(database=self.database)

    def get_record_size_for_day(self, date, type: TaqType):
        idx_df = self.dao.load_taq_index(date, type)
        taq_file = self.dao.get_taq_file(date, type)

        record_size = self.dao.detect_record_size(taq_file.bin_path, idx_df)
        print(f"Record size for {date} {type}: {record_size} bytes")
    
    def process_for_day(self, date, type: TaqType):
        try:
            df = self.dao.load_data_for_day(date=date, type=type)
            if not df.is_empty():
                self.dao.write_file_for_day(date=date, df=df, taq_type=type)
            print(f"Done: {date}")
        except Exception as e:
            print(f"Error on {date}: {e}")
    
    def process_range_parellel(self, start_date: dt.date, end_date: dt.date, type: TaqType):
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