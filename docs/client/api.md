# TaqClient API Reference

This page provides the comprehensive API documentation for interacting with the `taq-client` module. The module leverages an underlying DuckDB engine to query daily partitioned Parquet grids and automatically attaches CRSP mapping.

---

## Initialization

The package exposes a global factory function to connect to your database instance cleanly.

::: taq_client.connect

---

## Core Client Interface

The main workspace interaction occurs through the `TaqClient` class wrapper. 

::: taq_client.client.TaqClient
    options:
      show_root_heading: true
      show_source: true
      heading_level: 3

### Example Usage

The following example demonstrates how to initialize the client and fetch historical quote data for specific assets. 

!!! info "Data Volume Guardrails"
    The client implements automatic firewalls to prevent out-of-memory crashes:
    
    * **Specific Tickers Filtered:** The maximum safe query limit is **365 days**.
    * **Entire Market (No Tickers Provided):** The maximum safe query limit is **5 days**.

```python
import taq_client as tc

# 1. Connect to your database directory
client = tc.connect(db_path="home/user/tmp/taq_data")

# 2. Fetch a month of National Best Bid/Ask quote changes for Apple and Microsoft
quotes = client.get_quotes(
    start_date="1999-01-01", 
    end_date="1999-01-31", 
    tickers=["AAPL", "MSFT"]
)

# 3. Inspect the returned dataframely / Polars structure
print(quotes.head())
```

---

## Query Configurations & Guardrails

When query methods are executed on the client, user arguments are intercepted and evaluated by a strict validation lifecycle powered by Pydantic. This step guarantees data sanitization, handles date formatting, and establishes protective safeguards to protect system memory.

### Guardrail Mechanics & Limitations

The validation engine processes incoming inputs through three core phases:

1. **Date Coercion**: Input fields (`start_date` and `end_date`) accept standard Python `datetime.date` objects or standard ISO-formatted strings (e.g., `"1999-01-01"`). Strings are automatically parsed into native date types.
2. **Ticker Normalization**: Tickers can be provided as a single string (e.g., `"aapl"`) or a list of strings (e.g., `["aapl", "msft"]`). The validator automatically transforms all inputs into an uppercase list to ensure downstream filtering operates deterministically.
3. **Data Volume Firewalls**: To prevent sudden Out-of-Memory (OOM) crashes caused by accidentally scanning massive multi-file collections, the query window size is cross-validated against your filtering scope:

| Query Scope | Maximum Allowed Window | Error Behavior |
| :--- | :--- | :--- |
| **Filtered Tickers** (e.g. `["AAPL"]`) | **365 Days** | Throws `ValueError` if exceeded |
| **Entire Market** (No tickers provided) | **5 Days** | Throws `ValueError` if exceeded |

!!! warning "Bypassing the Firewall"
    If a large-scale structural pull is explicitly required (e.g., running an archive index migration), researchers can intentionally bypass these limits by passing `ignore_warnings=True` during method invocation.

### Technical Reference

Below is the automated documentation of the Pydantic configuration parameters compiled directly from the source module:

::: taq_client.models.taq_query.TaqQuery
    options:
      show_root_heading: true
      show_source: true
      heading_level: 4

---

## Exceptions

The custom error boundaries thrown by the package when data or configurations are missing.

::: taq_client.models.exceptions.DataMissingError
    options:
      show_root_heading: true
      heading_level: 3

::: taq_client.models.exceptions.CrspMappingMissingError
    options:
      show_root_heading: true
      heading_level: 3

---

## Return Type Schemas

Data is returned to your workspace formatted as strongly typed, native Polars DataFrames wrapped by `dataframely` validation schema layouts.

### Trade Records Schema
::: taq_client.models.schema.TradeHistorySchema
    options:
      heading_level: 4

### Quote Records Schema
::: taq_client.models.schema.QuoteHistorySchema
    options:
      heading_level: 4