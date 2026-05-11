# TAQ Backtester

High-performance ETL pipeline for processing TAQ (Trade and Quote) financial data from binary formats into Parquet for analysis and backtesting.

## Installation

```bash
pip install -e .
```

This installs the `taq-etl` command-line tool.

## CLI Tutorial

The TAQ ETL tool provides a command-line interface for processing raw TAQ data. All commands follow this pattern:

```
taq-etl [group] [command] [options]
```

### Basic Usage

#### Processing a Single Day

To process TAQ data for a single day:

```bash
taq-etl process day -d 1993-01-04 -t CQ
```

**Options:**
- `-d, --date` — Date in `YYYY-MM-DD` format (required)
- `-t, --type` — Data type: `CQ` (quotes) or `CT` (trades) (required)

**Example:**
```bash
# Process trades for January 4, 1993
taq-etl process day -d 1993-01-04 -t CT

# Process quotes for January 4, 1993
taq-etl process day -d 1993-01-04 -t CQ
```

#### Processing a Date Range

To process multiple days in parallel:

```bash
taq-etl process range -s 1993-01-04 -e 1993-01-29 -t CQ
```

**Options:**
- `-s, --start` — Start date in `YYYY-MM-DD` format (required)
- `-e, --end` — End date in `YYYY-MM-DD` format (required)
- `-t, --type` — Data type: `CQ` (quotes) or `CT` (trades) (required)

**Example:**
```bash
# Process all trades for January 1993
taq-etl process range -s 1993-01-01 -e 1993-01-31 -t CT

# Process all quotes for January through March 1993
taq-etl process range -s 1993-01-01 -e 1993-03-31 -t CQ
```

### Utility Commands

#### Inspecting Record Sizes

To detect and print the record size for a specific daily binary file:

```bash
taq-etl utils print-size -d 1993-01-04 -t trades
```

**Options:**
- `-d, --date` — Date in `YYYY-MM-DD` format (required)
- `-t, --type` — Data type: `trades` or `quotes` (required)

**Example:**
```bash
# Inspect trade record size for January 4, 1993
taq-etl utils print-size -d 1993-01-04 -t trades

# Inspect quote record size for January 4, 1993
taq-etl utils print-size -d 1993-01-04 -t quotes
```

### Data Output

Processed data is stored in Parquet format in the configured database path, organized by:
- Data type (trades or quotes)
- Year
- Month
- Day

This structure allows efficient querying and analysis of historical market data.

### Environment Configuration

The tool reads configuration from a `.env` file in the project root:

```
DATABASE_PATH=/path/to/database
```
