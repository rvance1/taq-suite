import polars as pl

from .models.backtester_config import BTConfig
from .models.schema import WeightsSchema, WeightsDf, SharesSchema, SharesDf

from taq_backtester.dal.dao.taq_dao import TaqDao
from taq_backtester.dal.models.schema import QuoteHistorySchema, QuoteHistoryDf

from .computations import compute_hourly_prices, compute_aum, compute_optimal_shares

class Backtester():
    def __init__(self, config: BTConfig, taq_dao: TaqDao | None = None):
        self.config = config
        self.taq_dao = taq_dao or TaqDao()

        self.current_date = self.config.start_date
        self.holdings: SharesDf = SharesDf.empty(schema=SharesSchema)
        self.cash = self.config.initial_aum

    def get_aum(self, quote_data_day: QuoteHistoryDf) -> float:
        
        

    def make_orders_on_date(self, date, optimal_weights: WeightsDf):
        weights = optimal_weights.filter(pl.col("datetime") == date)
        taq = self.taq_dao.load_quote_by_date(date)
        prices = compute_hourly_prices(taq)

        # optimal weights -> optimal shares
        # optimal shares - current shares = orders
        # execute orders
        
        



