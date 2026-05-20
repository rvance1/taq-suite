# TAQ Suite

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

All `utils` commands share a consistent interface using standard TAQ type codes:

**Standard Options:**
- `-d, --date` — Date in `YYYY-MM-DD` format (required)
- `-t, --type` — Data type: `CT` (trades) or `CQ` (quotes) (required)

#### Inspecting Record Sizes

```bash
taq-etl utils print-size -d 1993-01-04 -t CT
```

Prints the byte size of each record in the daily binary file.

```bash
# Inspect trade record size for January 4, 1993
taq-etl utils print-size -d 1993-01-04 -t CT

# Inspect quote record size for January 4, 1993
taq-etl utils print-size -d 1993-01-04 -t CQ
```

#### Hexdump a Binary File

```bash
taq-etl utils print-bin-hexdump -d 1993-01-04 -t CT
```

Prints a formatted hexdump of the raw `.BIN` data file for debugging binary parsing.

```bash
taq-etl utils print-bin-hexdump -d 1993-01-04 -t CQ
```

#### Hexdump an Index File

```bash
taq-etl utils print-index-hexdump -d 1993-01-04 -t CT
```

Prints a formatted hexdump of the `.IDX` index file, useful for understanding the index structure.

```bash
taq-etl utils print-index-hexdump -d 1993-01-04 -t CQ
```

#### Print Index DataFrame

```bash
taq-etl utils print-index-df -d 1993-01-04 -t CT
```

Parses the `.IDX` index file into a DataFrame and prints it, showing ticker, date, time, byte offset, and record count columns.

```bash
taq-etl utils print-index-df -d 1993-01-04 -t CQ
```

#### Print Index Grouped by Date

```bash
taq-etl utils print-index-by-date -d 1993-01-04 -t CT
```

Groups the parsed index by date and prints a per-date summary including ticker count, record range, and total records.

```bash
taq-etl utils print-index-by-date -d 1993-01-04 -t CQ
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
