import polars as pl

from .models.schema import WeightsDf, SharesSchema, SharesDf, PricesDf, PricesSchema
from taq_backtester.dal.models.schema import QuoteHistoryDf


def compute_prices(quote_data: QuoteHistoryDf) -> PricesDf:
    """Computes the price as the mid of the last bid and ask of the first day."""
    df = (
        quote_data.with_columns(
            pl.col("bid").add(pl.col("ask")).truediv(2).alias("price"),
        )
        .with_columns(
            pl.col("datetime").dt.truncate('1d').alias("hour")
        )
        .group_by(["hour", "ticker"]).agg(
            pl.col("price").last()
        )
        .rename({"hour": "datetime"})
        .sort(["ticker", "datetime"])
    )

    early_datetime = df.get_column("datetime").min()

    return PricesSchema.validate(
        df.filter(
            pl.col("datetime").eq(early_datetime),
            pl.col("price").gt(0)
        )
        .drop("datetime")
        .sort("ticker")
    )

def compute_aum(holdings: SharesDf, prices: PricesDf, cash: float) -> float:
    if holdings.is_empty():
        return cash
    
    return cash + (
        holdings.join(prices, on=["datetime", "ticker"], how="left")
        .with_columns(
            (pl.col("shares") * pl.col("price")).alias("position_value")
        )
        .select(pl.col("position_value"))
        .sum()
    )

def compute_optimal_shares(optimal_weights: WeightsDf, prices: PricesDf, aum: float) -> SharesDf:
    return SharesSchema.validate(
        optimal_weights.join(prices, on=["ticker"], how="left")
        .with_columns(
            ((aum * pl.col("weight") / pl.col("price")).floor()).alias("raw_shares")
        )
        .with_columns(
            pl.when(pl.col("raw_shares").is_null() | pl.col("raw_shares").is_nan() | pl.col("raw_shares").is_infinite())
            .then(0)
            .otherwise(pl.col("raw_shares"))
            .cast(pl.Int64)
            .alias("shares")
        )
        .select(["ticker", "shares"])
    )

def compute_delta_shares(optimal_shares: SharesDf, current_shares: SharesDf) -> SharesDf:
    if current_shares.is_empty():
        return optimal_shares
    
    return SharesSchema.validate(
        optimal_shares.join(
            current_shares, 
            on="ticker", 
            how="full", 
            coalesce=True
        )
        .with_columns(
            (pl.col("shares").fill_null(0) - pl.col("shares_right").fill_null(0))
            .cast(pl.Int64).alias("shares")
        )
        .select(["ticker", "shares"])
    )