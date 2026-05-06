from taq_etl import RawTaqDao, Database, TaqFile, TaqType

import datetime as dt
import polars as pl

database = Database(root_path="data")
dao = RawTaqDao(database=database)


date = dt.date(1993, 1, 4)

taq_file = TaqFile(root_path="data/raw/taq", date=date, type=TaqType.QUOTE)

print(taq_file.bin_path)

