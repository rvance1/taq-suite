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


WeightsDf: TypeAlias = dy.DataFrame[WeightsSchema]
SharesDf: TypeAlias = dy.DataFrame[SharesSchema]
PricesDf: TypeAlias = dy.DataFrame[PricesSchema]
WeigthsHistoryDf: TypeAlias = dy.DataFrame[WeightsHistorySchema]

