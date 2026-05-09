from pydantic import BaseModel, PrivateAttr
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from tqdm import tqdm

from taq_etl.dal.dao.raw_taq_dao import RawTaqDao
from taq_etl.dal.models.taq_file import TaqType
from taq_etl.dal.models.database import Database

class RawTaqService(BaseModel):
    database: Database
    _dao: RawTaqDao = PrivateAttr()

    def model_post_init(self, __context) -> None:
        self._dao = RawTaqDao(database=self.database)

    def print_record_size_for_day(self, date, type: TaqType):
        idx_df = self._dao.load_taq_index(date, type)
        taq_file = self._dao.get_taq_file(date, type)

        record_size = self._dao.detect_record_size(taq_file.bin_path, idx_df)
        print(f"Record size for {date} {type}: {record_size} bytes")
    
    def process_for_day(self, date, type: TaqType):
        try:
            df = self._dao.load_data_for_day(date=date, type=type)
            if not df.is_empty():
                self._dao.write_file_for_day(date=date, df=df, taq_type=type)
            print(f"Done: {date}")
        except Exception as e:
            print(f"Error on {date}: {e}")
    
    def process_range_parallel(self, start_date: dt.date, end_date: dt.date, type: TaqType):
        date_list = []
        curr = start_date
        while curr <= end_date:
            date_list.append(curr)
            curr += dt.timedelta(days=1)

        with ThreadPoolExecutor(max_workers=4) as executor:
            results = executor.map(self.process_for_day, date_list, repeat(type))
            list(tqdm(results, total=len(date_list), desc=f"Processing {type}"))