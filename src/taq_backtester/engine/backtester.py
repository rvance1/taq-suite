import polars as pl

from .model.config import BTConfig
from .model.schema import WeightsSchema, WeightsDf
from taq_backtester.dal.dao.taq_dao import TaqDao

class Backtester():
    def __init__(self, config: BTConfig, taq_dao: TaqDao | None = None):
        self.config = config
        self.portfolio: WeightsDf = WeightsDf.empty(schema=WeightsSchema)
        self.aum = self.config.initial_aum
        self.taq_dao = taq_dao or TaqDao()

    def make_orders_on_date(self, date, optimal_weights: WeightsDf):
        weights = optimal_weights.filter(pl.col("datetime") == date)
        taq = self.taq_dao.load_quote_by_date(date)
        prices = self.crsp_dao.load_price_by_date(date)

        #TODO: convert to shares first?
        
        combined = (
            self.portfolio.join(taq, on=["datetime", "ticker"], how="left")
            .join(prices, on=["datetime", "ticker"], how="left")
            .join(weights, on=["datetime", "ticker"], how="left")
        )



