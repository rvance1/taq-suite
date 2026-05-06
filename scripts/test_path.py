from taq_etl import RawTaqDao, Database, TaqFile, TaqType

import datetime as dt


database = Database(root_path="data")
dao = RawTaqDao(database=database)


date = dt.date(1999, 1, 4)
taq_file = TaqFile(root_path="data/raw/taq", date=date, type=TaqType.QUOTE, letter="A")

print(taq_file.bin_path)

