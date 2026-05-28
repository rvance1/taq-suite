import duckdb
import polars as pl
from taq_client.dal.paths import get_file_paths
from taq_client.validation_models import TaqQuery
from taq_client.dal.models.schema import QuoteHistoryDf, QuoteHistorySchema, TradeHistoryDf, TradeHistorySchema

class TaqDao:
    def __init__(self, db_path: str):
        self.conn = duckdb.connect()
        # Setup CRSP view
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW crsp_map AS 
            SELECT * FROM read_parquet('{db_path}/interim/crsp_mapping/*.parquet')
        """)

    def execute_trade_query(self, query: TaqQuery, db_path: str) -> TradeHistoryDf:
        paths = get_file_paths(db_path, query.start_date, query.end_date, "trade")
        
        if not paths:
            raise DataMissingError(
                f"No TAQ trade data found on disk for the requested range: "
                f"{query.start_date} to {query.end_date}."
            )

        ticker_filter = ""
        if query.tickers:
            ticker_list = ", ".join([f"'{t}'" for t in query.tickers])
            ticker_filter = f"WHERE t.ticker IN ({ticker_list})"

        sql = f"""
            SELECT t.*, c.permno 
            FROM read_parquet({paths}) t
            LEFT JOIN crsp_map c 
              ON t.ticker = c.join_ticker 
             AND t.datetime::DATE = c.date
            {ticker_filter}
            ORDER BY t.datetime, t.ticker
        """
        
        return TradeHistorySchema.validate(self.conn.execute(sql).pl())
    
    def execute_quote_query(self, query: TaqQuery, db_path: str) -> QuoteHistoryDf:
        paths = get_file_paths(db_path, query.start_date, query.end_date, "quote")
        
        if not paths:
            raise DataMissingError(
                f"No TAQ trade data found on disk for the requested range: "
                f"{query.start_date} to {query.end_date}."
            )

        ticker_filter = ""
        if query.tickers:
            ticker_list = ", ".join([f"'{t}'" for t in query.tickers])
            ticker_filter = f"WHERE t.ticker IN ({ticker_list})"

        sql = f"""
            SELECT t.*, c.permno 
            FROM read_parquet({paths}) t
            LEFT JOIN crsp_map c 
              ON t.ticker = c.join_ticker 
             AND t.datetime::DATE = c.date
            {ticker_filter}
            ORDER BY t.datetime, t.ticker
        """
        
        return QuoteHistorySchema.validate(self.conn.execute(sql).pl())