# TAQ Suite

Welcome to the internal documentation for the **TAQ Suite**—a high-performance, unified platform for ingestion, storage, simulation, and research using Trade and Quote (TAQ) tick data.

The suite is engineered using **DuckDB**, **Polars**, and **Apache Arrow** to deliver lightning-fast data processing boundaries while ensuring strong type safety and robust validation via **Pydantic** and **dataframely**.

---

## Suite Architecture

The platform is explicitly separated into three decoupled modules, enforcing a strict boundary between engineering, simulation, and interactive analysis.

```text
                                Raw TAQ    
                              (.BIN/.IDX)  
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │           taq-etl            │
                    └──────────────────────────────┘
                    │ • Decompresses .lz4 logs     │
                    │ • Parses binary struct arrays│
                    │ • Outputs structured grids   │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                                Database       
                       (Parquet Partition Grid)  
                                   │
             ┌─────────────────────┴───────────────────────┐
             ▼                                             ▼
┌───────────────────────────────┐               ┌───────────────────────────────┐
│          taq-client           │               │        taq-backtester         │
└───────────────────────────────┘               └───────────────────────────────┘
│  • User-Facing Read SDK       │               │  • Stateful Simulation Engine │
│  • Pydantic Query Guardrails  │               │  • Native dataframely Schemas │
│  • Returns Polars DataFrames  │               │  • Point-in-Time Executions   │
└──────────────┬────────────────┘               └──────────────┬────────────────┘
               │                                               │
               ▼                                               ▼
        Jupyter Notebook                                Alpha Execution   
         & Research Dfs                                   & Backtests     
```

### 1. 🛠️ `taq-etl`
* **Purpose**: The data processing backbone. It handles raw binary and index decompression (`.BIN.lz4` / `.IDX.lz4`) for historical NYSE/NASDAQ tick logs, parses little-endian schemas, maps daily trade and quote flags, and writes clean, sorted, date-partitioned Apache Parquet grids.
* **Who it's for**: Data Engineers maintaining the storage layout.

### 2. 📈 `taq-backtester`
* **Purpose**: A work in progress. This module contains a backtesting environment designed to run backtests using point-in-time TAQ data to simulate market orders.
* **Who it's for**: Researchers testing algorithmic strategies.

### 3. 🔬 `taq-client`
* **Purpose**: The research data-access SDK. It provides a simple, foolproof, pythonic interface (`TaqClient`) that abstracts away all underlying SQL, directory structures, and file path manipulation. It lets researchers pull massive chunks of data into memory safely using Polars LazyFrames, guarded by automatic input parsing and strict data-volume memory firewalls.
* **Who it's for**: Quantitative Researchers working in Jupyter Notebooks or building standalone alpha studies.

---

## Where should I go?

* **I am a researcher looking to load TAQ data into a notebook:** 👉 Go to the [TaqClient Getting Started Guide](client/getting_started.md).
* **I want to know the available columns and dtypes for trades/quotes:** 👉 Check out the [API & Schema Reference](client/api.md).
* **I want to run a simulation or look at the strategy environment:** 👉 Read the [Backtester Guide](dev/backtester.md).
* **I want to learn how raw binary payloads are structured and ingested:** 👉 View the [TAQ ETL Architecture](dev/etl.md).

---

## Local Development Installation

The suite is managed via `uv` for lightning-fast, reproducible builds. To clone the repository and set up your environment:

```bash
# Clone the repository
git clone git@github.com:rvance1/taq_suite.git
cd taq_suite

# Sync development dependencies and create virtual environment
uv sync
```