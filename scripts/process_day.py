from taq_etl import RawTaqDao, Database, TaqType

import datetime as dt

database = Database(root_path="data")
dao = RawTaqDao(database=database)


date = dt.date(1993, 1, 4)

df = dao.load_data_for_day(
    #ticker="AA",
    date=date,
    type=TaqType.QUOTE
)
if df.is_empty():
    print("No data for this day.")
else:
    dao.write_file_for_day(date=date, df=df, taq_type=TaqType.QUOTE)
    print("Success!")
