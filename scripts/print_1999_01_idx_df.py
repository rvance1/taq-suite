from taq_etl import RawTaqDao, Database, TaqType

import datetime as dt
import polars as pl

database = Database(root_path="data")
dao = RawTaqDao(database=database)


date = dt.date(1999, 1, 4)

df = dao.load_taq_index(
    #ticker="AA",
    date=date,
    type=TaqType.QUOTE
)
print(df.head())
