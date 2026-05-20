from pydantic import BaseModel
import datetime as dt

from taq_backtester.dal.models.database import Database
from taq_backtester.dal.models.taq_table import TaqTable, TaqType
from taq_backtester.dal.models.schema import QuoteHistorySchema, QuoteHistoryDf, TradeHistorySchema, TradeHistoryDf

class TaqDao(BaseModel):
    database: Database

    def get_table(self) -> TaqTable:
        return self.database.get_taq_table()

    def load_quote_by_date(self, date: dt.date) -> QuoteHistoryDf:
        table = self.get_table()
        df = table.load_date(date, TaqType.QUOTE)
        if df.is_empty():
            return df
        return QuoteHistorySchema.validate(df)
    
    def load_quote_by_range(self, start_date: dt.date, end_date: dt.date) -> QuoteHistoryDf:
        table = self.get_table()
        df = table.load_range(start_date, end_date, TaqType.QUOTE)
        if not df.is_empty():
            df = df.sort(["datetime", "ticker"])
        return QuoteHistorySchema.validate(df)

    def load_trade_by_date(self, date: dt.date) -> TradeHistoryDf:
        table = self.get_table()
        df = table.load_date(date, TaqType.TRADE)
        if df.is_empty():
            return df
        return TradeHistorySchema.validate(df)
    
    def load_trade_by_range(self, start_date: dt.date, end_date: dt.date) -> TradeHistoryDf:
        table = self.get_table()
        df = table.load_range(start_date, end_date, TaqType.TRADE)
        if not df.is_empty():
            df = df.sort(["datetime", "ticker"])
        return TradeHistorySchema.validate(df)