import dataframely as dy
from typing import TypeAlias

class QuoteSchema(dy.Schema):
    timestamp: dy.Timestamp
    symbol: str
    bid_price: float
    ask_price: float
    bid_size: int
    ask_size: int

class TradeSchema(dy.Schema):
    timestamp: dy.Timestamp
    symbol: str
    price: float
    size: int

QuoteDf: TypeAlias = dy.DataFrame[QuoteSchema]
TradeDf: TypeAlias = dy.DataFrame[TradeSchema]
