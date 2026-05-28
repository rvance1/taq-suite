from pathlib import Path
import duckdb

from taq_client.dal.paths import get_file_paths
from taq_client.models.exceptions import CrspMappingMissingError, DataMissingError
from taq_client.models.taq_query import TaqQuery
from taq_client.models.schema import QuoteHistoryDf, QuoteHistorySchema, TradeHistoryDf, TradeHistorySchema

class TaqDao:
    def __init__(self, db_path: str | None = None):
        import os
        
        resolved_path = db_path or os.getenv("TAQ_DB_PATH")
        if not resolved_path:
            raise ValueError(
                "Missing database path. Please provide a valid path via "
                "the constructor or set the TAQ_DB_PATH environment variable."
            )
            
        self.db_path = Path(resolved_path)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database path '{self.db_path}' does not exist.")
        
        self.conn = duckdb.connect()

        crsp_path = Path(self.db_path) / "interim" / "crsp_mapping"
        if not crsp_path.exists() or not any(crsp_path.glob("*.parquet")):
            raise CrspMappingMissingError(
                f"CRSP mapping data is missing or empty. "
                f"Expected to find at least one .parquet file in: {crsp_path}"
            )
        
        self.conn.execute(f"""
            CREATE OR REPLACE VIEW crsp_map AS 
            SELECT * FROM read_parquet('{self.db_path}/interim/crsp_mapping/*.parquet')
        """)

    def execute_trade_query(self, query: TaqQuery) -> TradeHistoryDf:
        paths = get_file_paths(self.db_path, query.start_date, query.end_date, "trade")
        
        if not paths:
            raise DataMissingError(
                f"No TAQ trade data found on disk for the requested range: "
                f"{query.start_date} to {query.end_date}."
            )

        ticker_filter = ""
        if query.tickers:
            ticker_list = ", ".join([f"'{t}'" for t in query.tickers])
            ticker_filter = f"WHERE t.ticker IN ({ticker_list})"

        path_strs = ", ".join(f"'{p}'" for p in paths)
        sql = f"""
            SELECT t.*, c.permno 
            FROM read_parquet([{path_strs}]) t
            LEFT JOIN crsp_map c 
              ON t.ticker = c.join_ticker 
             AND t.datetime::DATE = c.date
            {ticker_filter}
            ORDER BY t.datetime, t.ticker
        """
        
        return TradeHistorySchema.validate(self.conn.execute(sql).pl())
    
    def execute_quote_query(self, query: TaqQuery) -> QuoteHistoryDf:
        paths = get_file_paths(self.db_path, query.start_date, query.end_date, "quote")
        
        if not paths:
            raise DataMissingError(
                f"No TAQ quote data found on disk for the requested range: "
                f"{query.start_date} to {query.end_date}."
            )

        ticker_filter = ""
        if query.tickers:
            ticker_list = ", ".join([f"'{t}'" for t in query.tickers])
            ticker_filter = f"WHERE t.ticker IN ({ticker_list})"

        path_strs = ", ".join(f"'{p}'" for p in paths)
        sql = f"""
            SELECT t.*, c.permno 
            FROM read_parquet([{path_strs}]) t
            LEFT JOIN crsp_map c 
              ON t.ticker = c.join_ticker 
             AND t.datetime::DATE = c.date
            {ticker_filter}
            ORDER BY t.datetime, t.ticker
        """
        
        return QuoteHistorySchema.validate(self.conn.execute(sql).pl())