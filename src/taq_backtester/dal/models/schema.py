import dataframely as dy
from typing import TypeAlias


class QuoteSchema(dy.Schema):
    timestamp: dy.Datetime(nullable=False)
    symbol: dy.String(nullable=False)
    bid_price: dy.Float(nullable=False)
    ask_price: dy.Float(nullable=False)
    bid_size: dy.Int32(nullable=False)
    ask_size: dy.Int32(nullable=False)


class TradeSchema(dy.Schema):
    timestamp: dy.Datetime(nullable=False)
    symbol: dy.String(nullable=False)
    price: dy.Float(nullable=False)
    size: dy.Int32(nullable=False)

QuoteDf: TypeAlias = dy.DataFrame[QuoteSchema]
TradeDf: TypeAlias = dy.DataFrame[TradeSchema]
