# Getting Started with TaqClient

This guide will walk you through setting up the data-access client and pulling your first historical tick dataset. The `taq-client` is designed to provide rapid, memory-safe data queries via a streamlined Python interface.

---

## Prerequisites

Before initialization, ensure that your workspace environment has been properly synchronized using `uv`:

```bash
# Sync dependencies and activate your virtual environment
uv sync
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
```

### Setting the Database Environment Variable
The client looks for a local directory or network mount containing the partitioned Parquet tables. While you can pass this path explicitly into your code, the best practice is to set it as an environment variable in your terminal session or your local `.env` file:

```bash
export TAQ_DB_PATH="/path/to/your/local/taq_database_root"
```

---

## 2-Minute Quickstart

Create a new Python script or open a Jupyter Notebook cell, and execute the following boilerplate sequence to retrieve trade execution data.

```python
import taq_client as tc
import datetime as dt

# 1. Instantiate the workspace client wrapper
# (Leaving db_path empty forces it to read your TAQ_DB_PATH environment variable)
client = tc.connect()

# 2. Define your historical observation parameters
start = "1999-01-04"
end = "1999-01-05"
target_tickers = ["AAPL", "MSFT", "INTC"]

# 3. Pull the joined Trade executions grid
print(f"Fetching TAQ trades for {target_tickers}...")
trades_df = client.get_trades(
    start_date=start,
    end_date=end,
    tickers=target_tickers
)

# 4. View the resulting data matrix
print(f"Retrieved {len(trades_df):,} total records.")
print(trades_df.head())
```

---

## Basic Concepts to Keep in Mind

### 1. Data Type Normalization
You do not need to worry about formatting string inputs or cleaning case types. The client's validation engine automatically handles inputs:

* **Dates**: Accepts native Python `datetime.date` objects or standard ISO-strings (`"YYYY-MM-DD"`).
* **Tickers**: Single strings or lists of strings are automatically cleaned, normalized to uppercase, and stripped of whitespace.

### 2. Built-In Memory Protections
To protect shared research servers and your local system from crashing due to sudden Out-of-Memory (OOM) situations, the client establishes protective queries:

* Filtering for **Specific Tickers** restricts your query window to a maximum of **365 days**.
* Pulling the **Entire Market** (omitting the `tickers` argument entirely) limits your query window to a maximum of **5 days**.

To explicitly run an unusually large historical capture that breaks these limits, pass the override flag:
```python
# Bypasses the query firewall safely for extensive extractions
massive_df = client.get_trades(
    start_date="1999-01-01", 
    end_date="1999-12-31", 
    ignore_warnings=True
)
```

### 3. Data Format
The objects returned by `get_trades()` and `get_quotes()` are **Polars DataFrames** validated against a strict `dataframely` schema layer. This means you immediately inherit lightning-fast vectorized aggregation, filtering, and analysis performance without any translation lag.