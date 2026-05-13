from pydantic import BaseModel
import datetime as dt

class BTConfig(BaseModel):
    """Configuration for the backtester."""
    start_date: dt.date
    end_date: dt.date
    initial_aum: int
