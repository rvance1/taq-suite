from pydantic import BaseModel
import datetime as dt
import polars as pl

from taq_backtester.dal.models.database import Database
from taq_backtester.dal.models.taq_table import TaqTable, TaqType
from taq_backtester.dal.models.schema import QuoteSchema, QuoteDf, TradeSchema, TradeDf


class TaqDao(BaseModel):
    database: Database

    def get_table(self) -> TaqTable:
        return self.database.get_taq_table()
    
    def load_quote_by_date(self, date: dt.date) -> QuoteDf:
        table = self.get_table()
        return QuoteSchema.validate(table.scan_date(date, TaqType.QUOTE).collect())
    
    def load_quote_by_range(self, start_date: dt.datetime, end_date: dt.datetime) -> QuoteDf:
        table = self.get_table()
        return QuoteSchema.validate(
            table.scan_range(start_date, end_date, TaqType.QUOTE)
            .filter(pl.col("datetime").is_between(start_date, end_date))
            .collect()
        )
    
    def load_trade_by_date(self, date: dt.date) -> TradeDf:
        table = self.get_table()
        return TradeSchema.validate(table.scan_date(date, TaqType.TRADE).collect())
    
    def load_trade_by_range(self, start_date: dt.datetime, end_date: dt.datetime) -> TradeDf:
        table = self.get_table()
        return TradeSchema.validate(
            table.scan_range(start_date, end_date, TaqType.TRADE)
            .filter(pl.col("datetime").is_between(start_date, end_date))
            .collect()
        )