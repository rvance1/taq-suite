from typing import Union, List
import datetime as dt

from taq_client.models.taq_query import TaqQuery
from taq_client.dal.taq_dao import TaqDao
from taq_client.models.schema import TradeHistoryDf, QuoteHistoryDf

class TaqClient:
    def __init__(self, db_path: str | None = None):
        """Initializes the DuckDB connection and mounts the CRSP map."""
        self._conn = TaqDao(db_path=db_path)

    def get_trades(
        self, 
        start_date: Union[dt.date, str], 
        end_date: Union[dt.date, str], 
        tickers: Union[str, List[str]] = [],
        ignore_warnings: bool = False
    ) -> TradeHistoryDf:
        """Fetches historical trades, joined with CRSP permno."""

        query = TaqQuery(
            start_date=start_date, 
            end_date=end_date, 
            tickers=tickers, 
            ignore_warnings=ignore_warnings
        )

        return self._conn.execute_trade_query(query)
    
    def get_quotes(
        self, 
        start_date: Union[dt.date, str], 
        end_date: Union[dt.date, str], 
        tickers: Union[str, List[str]] = [],
        ignore_warnings: bool = False
    ) -> QuoteHistoryDf:
        """Fetches historical quotes, joined with CRSP permno."""

        query = TaqQuery(
            start_date=start_date, 
            end_date=end_date, 
            tickers=tickers, 
            ignore_warnings=ignore_warnings
        )

        return self._conn.execute_trade_query(query)