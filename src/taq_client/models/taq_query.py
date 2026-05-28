import datetime as dt
from typing import List, Optional, Union
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict

class TaqQuery(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    start_date: dt.date
    end_date: dt.date
    tickers: Optional[Union[str, List[str]]] = Field(default=None)
    ignore_warnings: bool = Field(default=False)

    # 1. Automatically convert strings like "2026-05-28" into dt.date objects
    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def parse_dates(cls, v):
        if isinstance(v, str):
            return dt.datetime.strptime(v, "%Y-%m-%d").date()
        return v

    # 2. Ensure all tickers are uppercase lists for clean filtering
    @field_validator("tickers", mode="after")
    @classmethod
    def normalize_tickers(cls, v):
        if isinstance(v, str):
            return [v.upper()]
        return [t.upper() for t in v]

    # 3. Multi-field cross validation (The Guardrail)
    @model_validator(mode="after")
    def enforce_data_limits(self) -> "TaqQuery":
        if self.end_date < self.start_date:
            raise ValueError("end_date cannot be before start_date.")
        
        days_requested = (self.end_date - self.start_date).days
        max_safe_days = 365 if self.tickers else 5
        
        if days_requested > max_safe_days and not self.ignore_warnings:
            target = "specific tickers" if self.tickers else "the entire market"
            raise ValueError(
                f"Requested {days_requested} days for {target}. "
                f"This exceeds the safety limit of {max_safe_days} days. "
                f"Set `ignore_warnings=True` to bypass."
            )
        return self