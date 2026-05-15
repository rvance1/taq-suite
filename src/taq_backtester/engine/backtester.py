import polars as pl
import datetime as dt

from .models.backtester_config import BTConfig
from .models.schema import (
    WeightsDf, SharesDf, SharesSchema, WeightsSchema, WeightsHistoryDf, WeightsHistorySchema, SharesHistoryDf, SharesHistorySchema
)

from taq_backtester.dal.dao.taq_dao import TaqDao
from taq_backtester.dal.models.schema import QuoteHistoryDf

from .computations import compute_prices, compute_aum, compute_optimal_shares, compute_delta_shares, add_delta_shares


# TODO: Add aum history, finish rebalance function
class Backtester():
    def __init__(self, config: BTConfig, taq_dao: TaqDao):
        self.config = config
        self.taq_dao = taq_dao
        self.current_date = self.config.start_date
        self.holdings: SharesDf = pl.DataFrame({"ticker": pl.Series([], dtype=pl.String), "shares": pl.Series([], dtype=pl.Int64)})
        self.cash = self.config.initial_aum
        self.realized_holdings: SharesHistoryDf = pl.DataFrame({"datetime": pl.Series([], dtype=pl.Datetime), "ticker": pl.Series([], dtype=pl.String), "shares": pl.Series([], dtype=pl.Int64)})
        self.aum_history = {}
        
    def _record_holdings(self, order_fills: SharesHistoryDf) -> None:
        current_dt = dt.datetime.combine(self.current_date, dt.time(0, 0))
        holdings_snapshot: SharesHistoryDf = SharesHistorySchema.validate(
            self.holdings.with_columns(pl.lit(current_dt).alias("datetime"))
            .select(["datetime", "ticker", "shares"])
        )
    
        if self.realized_holdings.is_empty():
            self.realized_holdings = holdings_snapshot
        else:
            self.realized_holdings = pl.concat([self.realized_holdings, holdings_snapshot]).sort(["ticker", "datetime"])
    
    def generate_orders(self, optimal_weights: WeightsDf, quote_data: QuoteHistoryDf) -> SharesDf:
        """Generates the delta shares to trade based on the optimal weights and current holdings."""

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

        self.aum_history[self.current_date] = aum
        return delta_shares

    def execute_orders(self, delta_shares: SharesDf, quote_data: QuoteHistoryDf) -> None:
        """Executes the orders by calculating the cost and updating cash and holdings."""

        order_fills_raw = (
            quote_data.join(delta_shares, on="ticker", how="inner")
            .unique(subset=["ticker"], keep="first", maintain_order=True)
            .with_columns(
                pl.when(pl.col("shares").gt(0))
                .then(pl.col("ask").mul(pl.col("shares")))
                .otherwise(pl.col("bid").mul(pl.col("shares")))
                .alias("cost")
            )
        )

        self.cash -= order_fills_raw.get_column("cost").sum()

        self.holdings = add_delta_shares(
            current_shares=self.holdings, 
            delta_shares=delta_shares
        )

        order_fills = SharesHistorySchema.validate(order_fills_raw.select(["datetime", "ticker", "shares"]))
        self._record_holdings(order_fills)

    def rebalance(self, optimal_weights_history: WeightsHistoryDf, execute_at: dt.time = dt.time(9, 5)) -> None:
        """Rebalances the portfolio based on the optimal weights for the given datetime."""

        optimal_weights = (
            optimal_weights_history
            .with_columns([
                pl.col("datetime").dt.date().alias("date"),
                pl.col("datetime").dt.time().alias("time")
            ])
            .filter(
                pl.col("date").eq(self.current_date),
                pl.col("time") <= execute_at
            )
            .sort("datetime")
            .unique(subset=["ticker", "date"], keep="last")
            .drop(["date", "time"])
        )
        if optimal_weights.is_empty():
            print(f"No optimal weights for date: {self.current_date}, skipping rebalance")
            return
        
        optimal_weights = WeightsSchema.validate(
            optimal_weights.filter(
                pl.col("weight").is_not_nan() & pl.col("weight").is_finite()
            )
        )
        quote_data = self.taq_dao.load_quote_by_date(self.current_date)

        delta_shares = self.generate_orders(
            optimal_weights=optimal_weights,
            quote_data=quote_data
        )

        self.execute_orders(
            delta_shares=delta_shares,
            quote_data=quote_data
        )

    