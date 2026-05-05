from taq_backtester import RawTaqDao, Database, TaqType

import datetime as dt
import polars as pl

database = Database(root_path="data")
dao = RawTaqDao(database=database)


date = dt.date(1993, 1, 4)

df = dao.load_data_for_day(
    #ticker="AA",
    date=date,
    type=TaqType.QUOTE
)
dao.write_file_for_day(date=date, df=df, taq_type=TaqType.QUOTE)
print("Success!")
