import polars as pl

from .models.backtester_config import BTConfig
from .models.schema import WeightsSchema, WeightsDf, SharesSchema, SharesDf

from taq_backtester.dal.dao.taq_dao import TaqDao
from taq_backtester.dal.models.schema import QuoteHistorySchema, QuoteHistoryDf

from .computations import compute_prices, compute_aum, compute_optimal_shares, compute_delta_shares

class Backtester():
    def __init__(self, config: BTConfig, taq_dao: TaqDao):
        self.config = config
        self.taq_dao = taq_dao

        self.current_date = self.config.start_date
        self.holdings: SharesDf = pl.DataFrame()
        self.cash = self.config.initial_aum
        
    def generate_orders(self, date, optimal_weights: WeightsDf):
        taq = self.taq_dao.load_quote_by_date(date)
        prices = compute_prices(taq)

        aum = compute_aum(
            holdings=self.holdings,
            prices=prices,
            cash=self.cash
        )

        optimal_shares = compute_optimal_shares(
            optimal_weights=optimal_weights,
            prices=prices,
            aum=aum
        )

        delta_shares = compute_delta_shares(
            optimal_shares=optimal_shares,
            current_shares=self.holdings
        )

        # optimal weights -> optimal shares
        # optimal shares - current shares = orders
        # execute orders
        pass
        



