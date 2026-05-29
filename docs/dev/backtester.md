# TAQ Backtester Architecture

The `taq-backtester` module contains a stateful, event-driven portfolio simulation engine. It ingests time-series target weights, evaluates executions against historical high-frequency TAQ quote grids, and outputs time-series realised weights and performance metrics.

---

## Current Architecture & Assumptions

The backtesting framework is explicitly structured as a systematic batch rebalancing processor. It operates with the following internal metrics:

1. **Time-Series Allocations**: The engine takes in a continuous time-series dataset of optimal weights (`WeightsHistoryDf`) and steps through dates to map out realized asset positions over time.
2. **Execution Timing**: The simulation filters the target alpha weights up to a designated execution cutoff parameter (defaults to `09:05:00` AM) and executes immediately against the local day's quote stream.
3. **Execution Model**: Orders are filled completely with no market-impact latency or partial execution:
    * **Buys (`delta_shares > 0`)**: Filled entirely at the market **Ask** price.
    * **Sells (`delta_shares < 0`)**: Filled entirely at the market **Bid** price.

!!! warning "Known Work-in-Progress Constraints"
    The engine currently assumes a simplified friction model:
    
    * **No Execution Slippage**: Orders are filled instantly against the first available quote, ignoring order-book queue depth or size-based price degradation.
    * **No Short Selling Borrow Costs**: Margin costs for negative equity or share allocations are not yet factored into cash calculations.

---

## Data Ingestion & Output Pipeline

The simulator transforms theoretical allocation weights into concrete historical performance vectors:

```text
 ┌─────────────────────────────┐
 │    WeightsHistoryDf         │  <-- 1. Input: Daily target weights
 └──────────────┬──────────────┘
                │
                ▼
 ┌─────────────────────────────┐
 │         Backtester          │  <-- 2. Processing: Evaluates fills against 
 │      Simulation Engine      │         Bid/Ask quotes per rebalance date
 └──────────────┬──────────────┘
                │
        ┌───────┴───────┐
        ▼               ▼
 ┌─────────────┐ ┌─────────────┐
 │ Realized    │ │ AUM History │  <-- 3. Output: Time-series weights achieved 
 │ Weights     │ │  Over Time  │         from the simulated order fills
 └─────────────┘ └─────────────┘
```

---

## Technical API Reference

Below is the automated documentation of the simulation framework compiled directly from the source code module:

::: taq_backtester.engine.backtester.Backtester
    options:
      show_root_heading: true
      show_source: true
      heading_level: 3
      members:
        - __init__
        - generate_orders
        - execute_orders
        - rebalance

---

## Ongoing Developer Roadmap

If you are expanding the backtester framework, please ensure enhancements resolve one of the active tracking tasks below:

* **Slippage Analytics**: Implement a custom cost model inside `execute_orders()` that tracks execution timing differences compared to market mid-prices using the recorded `order_fills` dataframe.
* **Look-Ahead Safeguards**: Enforce a strict programmatic check confirming that `quote_data` timestamps are strictly greater than or equal to the alpha generation `datetime` to eliminate look-ahead data leaking.
* **Transaction Cost Engines**: Integrate basic basis-point or fixed fee models inside the execution path logic to mimic realistic clearing frictions.