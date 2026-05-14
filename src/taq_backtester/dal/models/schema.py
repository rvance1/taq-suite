import dataframely as dy
from typing import TypeAlias


class QuoteHistorySchema(dy.Schema):
    datetime = dy.Datetime(nullable=False)
    ticker = dy.String(nullable=False)
    bid = dy.Float(nullable=False)
    ask = dy.Float(nullable=False)
    bid_size = dy.Int16(nullable=False)
    ask_size = dy.Int16(nullable=False)


class TradeHistorySchema(dy.Schema):
    datetime = dy.Datetime(nullable=False)
    ticker = dy.String(nullable=False)
    price = dy.Float(nullable=False)
    volume = dy.Int32(nullable=False)

QuoteHistoryDf: TypeAlias = dy.DataFrame[QuoteHistorySchema]
TradeHistoryDf: TypeAlias = dy.DataFrame[TradeHistorySchema]
