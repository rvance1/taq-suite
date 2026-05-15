import dataframely as dy
from typing import TypeAlias


class WeightsSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    weight = dy.Float(nullable=False)

class SharesSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    shares = dy.Integer(nullable=False)

class PricesSchema(dy.Schema):
    ticker = dy.String(nullable=False)
    price = dy.Float(nullable=False)

class WeightsHistorySchema(dy.Schema):
    datetime = dy.Datetime(nullable=False)
    ticker = dy.String(nullable=False)
    weight = dy.Float(nullable=False)

class SharesHistorySchema(dy.Schema):
    datetime = dy.Datetime(nullable=False)
    ticker = dy.String(nullable=False)
    shares = dy.Integer(nullable=False)


WeightsDf: TypeAlias = dy.DataFrame[WeightsSchema]
SharesDf: TypeAlias = dy.DataFrame[SharesSchema]
PricesDf: TypeAlias = dy.DataFrame[PricesSchema]

WeightsHistoryDf: TypeAlias = dy.DataFrame[WeightsHistorySchema]
SharesHistoryDf: TypeAlias = dy.DataFrame[SharesHistorySchema]

