import polars as pl

from .models.backtester_config import BTConfig
from .models.schema import WeightsSchema, WeightsDf, SharesSchema, SharesDf

from taq_backtester.dal.dao.taq_dao import TaqDao
from taq_backtester.dal.models.schema import QuoteHistorySchema, QuoteHistoryDf

from .computations import compute_prices, compute_aum, compute_optimal_shares, compute_delta_shares, add_delta_shares

class Backtester():
    def __init__(self, config: BTConfig, taq_dao: TaqDao):
        self.config = config
        self.taq_dao = taq_dao

        self.current_date = self.config.start_date
        self.holdings = pl.DataFrame()
        self.cash = self.config.initial_aum
        
    def generate_orders(self, optimal_weights: WeightsDf, quote_data: QuoteHistoryDf) -> SharesDf:
        prices = compute_prices(quote_data)

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

        return delta_shares
    
    def execute_orders(self, delta_shares: SharesDf, quote_data: QuoteHistoryDf) -> None:

        order_fills = (
            quote_data.join(delta_shares, on="ticker", how="inner")
            .unique(subset=["ticker"], keep="first", maintain_order=True)
            .with_columns(
                pl.when(pl.col("shares").gt(0))
                .then(pl.col("ask").mul(pl.col("shares")))
                .otherwise(pl.col("bid").mul(pl.col("shares")))
                .alias("cost")
            )
        )

        self.cash -= order_fills.get_column("cost").sum()
        
        self.holdings = add_delta_shares(
            current_shares=self.holdings, 
            delta_shares=delta_shares
        )

        



