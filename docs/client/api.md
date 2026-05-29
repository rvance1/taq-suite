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

---

## Query Configurations & Guardrails

When methods are invoked on the client, user inputs are intercepted, validated, and normalized using strict Pydantic parsing engine rules to enforce query safety boundaries.

::: taq_client.models.taq_query.TaqQuery
    options:
      show_root_heading: true
      heading_level: 3

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