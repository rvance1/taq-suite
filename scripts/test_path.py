from taq_etl import RawTaqDao, Database, TaqType
from pathlib import Path

import datetime as dt


database = Database(root_path="data")
dao = RawTaqDao(database=database)


date = dt.date(1993, 1, 4)

idx_df = dao.load_taq_index(date, TaqType.TRADE)
record_size = dao.detect_record_size(Path("data/raw/taq/taq1993/CT9301.BIN.lz4"), idx_df)

print(f"Record size: {record_size} bytes")
