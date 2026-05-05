from taq_backtester import RawTaqDao, Database

import datetime as dt

database = Database(root_path="data")
dao = RawTaqDao(database=database)


date = dt.date(1993, 1, 1)
df = dao.load_taq_index(
    #ticker="AA",
    date=date,
    type="CQ"
)

print(df)
