from .client import TaqClient
from .models.schema import TradeHistoryDf, QuoteHistoryDf
from .models.exceptions import CrspMappingMissingError, DataMissingError
from .models.taq_query import TaqQuery

def connect(db_path: str | None = None) -> TaqClient:
    """Factory function to create a TaqClient instance."""
    return TaqClient(db_path=db_path)

__all__ = [
    "connect",
    "TaqClient",
    "TaqQuery",
    "TradeHistoryDf",
    "QuoteHistoryDf",
    "DataMissingError",
    "CrspMappingMissingError",
]