import dataframely as dy
from typing import TypeAlias


class WeightsSchema(dy.Schema):
    datetime = dy.Datetime(nullable=False)
    ticker = dy.String(nullable=False)
    weight = dy.Float(nullable=False)

class PricesSchema(dy.Schema):
    datetime = dy.Datetime(nullable=False)
    ticker = dy.String(nullable=False)
    price = dy.Float(nullable=False)


WeightsDf: TypeAlias = dy.DataFrame[WeightsSchema]
PricesDf: TypeAlias = dy.DataFrame[PricesSchema]
