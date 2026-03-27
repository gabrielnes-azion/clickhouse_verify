# ClickHouse Table Report Generator

A Go CLI tool that connects to ClickHouse and generates a comprehensive report with table statistics.

## Features

- Table name listing
- TTL configuration display
- Table size in bytes
- Row count per table
- Column count per table
- Average rows per minute (last 10 complete minutes)

## Prerequisites

- Go 1.21 or higher
- ClickHouse server access

## Configuration

Create a `.env` file in the project root with the following variables:

```env
CLICKHOUSE_HOST=your-clickhouse-host
CLICKHOUSE_PORT=9440
CLICKHOUSE_DB=your-database
CLICKHOUSE_USER=your-username
CLICKHOUSE_PASSWORD=your-password
```

## Usage

### Run directly

```bash
go run main.go
```

### With custom output file

```bash
go run main.go -output my_report.md
```

### Build and run

```bash
make build
./clickhouse_verify -output my_report.txt
```

### Using Makefile

```bash
make run              # Run with default output file (report.txt)
make run OUTPUT=custom.txt  # Run with custom output file
make build            # Build the binary
make clean            # Remove build artifacts
```

## Output Format

The report is displayed in a tabular format:

```
TABLE NAME    TTL                       SIZE (BYTES)    ROWS    COLUMNS    ROWS/MIN (10min avg)
----------    ---                       ------------    ----    -------    --------------------
table_name    ts + toIntervalDay(30     1234567890      1000    10         42.50
```

## Notes

- Tables with `_dist` suffix are ClickHouse Distributed tables
- TTL shows "N/A" for distributed tables (TTL is defined on local tables)
- Rows per minute is calculated from the last 10 complete minutes (excluding current minute)
- The tool auto-detects DateTime/Date columns for calculating rows per minute

## Dependencies

- [clickhouse-go/v2](https://github.com/ClickHouse/clickhouse-go) - Official ClickHouse Go driver
- [godotenv](https://github.com/joho/godotenv) - Environment variable loader
