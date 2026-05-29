from .client import TaqClient

def connect(db_path: str | None = None) -> TaqClient:
    """Factory function to create a TaqClient instance."""
    return TaqClient(db_path=db_path)