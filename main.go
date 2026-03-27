package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/joho/godotenv"
)

const defaultOutputFile = "report.md"

type TableReport struct {
	Name           string
	TTL            string
	SizeBytes      uint64
	TotalRows      uint64
	ColumnCount    uint64
	RowsPerMinute  float64
}

func main() {
	outputFile := flag.String("output", defaultOutputFile, "Output file path for the report")
	flag.Parse()

	if err := godotenv.Load(); err != nil {
		log.Printf("Warning: .env file not found: %v", err)
	}

	host := getEnv("CLICKHOUSE_HOST", "localhost")
	port := getEnvInt("CLICKHOUSE_PORT", 9000)
	database := getEnv("CLICKHOUSE_DB", "default")
	user := getEnv("CLICKHOUSE_USER", "default")
	password := getEnv("CLICKHOUSE_PASSWORD", "")

	log.Printf("Connecting to ClickHouse at %s:%d (database: %s)", host, port, database)

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: password,
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	})
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping ClickHouse: %v", err)
	}
	log.Println("Connected to ClickHouse successfully")

	report, err := generateReport(ctx, conn, database)
	if err != nil {
		log.Fatalf("Failed to generate report: %v", err)
	}

	log.Printf("Writing report to %s", *outputFile)
	if err := writeReport(report, *outputFile); err != nil {
		log.Fatalf("Failed to write report: %v", err)
	}

	log.Printf("Report generated successfully: %s", *outputFile)
}

func generateReport(ctx context.Context, conn clickhouse.Conn, database string) ([]TableReport, error) {
	log.Println("Querying table list from system.tables")

	query := `
		SELECT
			name,
			total_rows,
			total_bytes,
			engine_full
		FROM system.tables
		WHERE database = ?
		AND engine NOT IN ('View', 'MaterializedView', 'Dictionary')
		ORDER BY name
	`

	rows, err := conn.Query(ctx, query, database)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var reports []TableReport

	for rows.Next() {
		var name string
		var totalRows *uint64
		var totalBytes *uint64
		var engineFull string

		if err := rows.Scan(&name, &totalRows, &totalBytes, &engineFull); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		log.Printf("Processing table: %s", name)

		var rowsVal uint64
		var bytesVal uint64
		if totalRows != nil {
			rowsVal = *totalRows
		}
		if totalBytes != nil {
			bytesVal = *totalBytes
		}

		ttl := extractTTL(engineFull)

		log.Printf("  - Getting column count for %s", name)
		colCount, err := getColumnCount(ctx, conn, database, name)
		if err != nil {
			log.Printf("  - Warning: failed to get column count for %s: %v", name, err)
		}

		log.Printf("  - Calculating rows per minute for %s", name)
		rowsPerMin, err := getRowsPerMinute(ctx, conn, database, name)
		if err != nil {
			log.Printf("  - Warning: failed to get rows per minute for %s: %v", name, err)
		}

		reports = append(reports, TableReport{
			Name:          name,
			TTL:           ttl,
			SizeBytes:     bytesVal,
			TotalRows:     rowsVal,
			ColumnCount:   colCount,
			RowsPerMinute: rowsPerMin,
		})
	}

	log.Printf("Processed %d tables", len(reports))
	return reports, rows.Err()
}

func extractTTL(engineFull string) string {
	upper := strings.ToUpper(engineFull)
	idx := strings.Index(upper, "TTL")
	if idx == -1 {
		return "N/A"
	}

	ttlPart := engineFull[idx+3:]
	ttlPart = strings.TrimSpace(ttlPart)

	endChars := []string{",", ")"}
	endIdx := len(ttlPart)
	for _, c := range endChars {
		if i := strings.Index(ttlPart, c); i != -1 && i < endIdx {
			endIdx = i
		}
	}

	ttlExpr := strings.TrimSpace(ttlPart[:endIdx])
	return ttlExpr
}

func getColumnCount(ctx context.Context, conn clickhouse.Conn, database, table string) (uint64, error) {
	var count uint64
	err := conn.QueryRow(ctx,
		"SELECT count() FROM system.columns WHERE database = ? AND table = ?",
		database, table,
	).Scan(&count)
	return count, err
}

func getRowsPerMinute(ctx context.Context, conn clickhouse.Conn, database, table string) (float64, error) {
	tsColumn, err := findTimestampColumn(ctx, conn, database, table)
	if err != nil {
		return 0, err
	}

	if tsColumn == "" {
		return 0, nil
	}

	var count uint64
	query := fmt.Sprintf(`
		SELECT count()
		FROM %s.%s
		WHERE %s >= now() - INTERVAL 11 MINUTE
		AND %s < now() - INTERVAL 1 MINUTE
	`, quoteIdent(database), quoteIdent(table), quoteIdent(tsColumn), quoteIdent(tsColumn))

	err = conn.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	return float64(count) / 10.0, nil
}

func findTimestampColumn(ctx context.Context, conn clickhouse.Conn, database, table string) (string, error) {
	query := `
		SELECT name
		FROM system.columns
		WHERE database = ?
		AND table = ?
		AND (type LIKE 'DateTime%' OR type LIKE 'Date%')
		ORDER BY name
		LIMIT 1
	`

	var name string
	err := conn.QueryRow(ctx, query, database, table).Scan(&name)
	if err != nil {
		return "", nil
	}

	return name, nil
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func writeReport(reports []TableReport, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	fmt.Fprintln(file, "# ClickHouse Table Report")
	fmt.Fprintln(file, "")
	fmt.Fprintln(file, "| TABLE NAME | TTL | SIZE (BYTES) | ROWS | COLUMNS | ROWS/MIN (10min avg) |")
	fmt.Fprintln(file, "|------------|-----|--------------|------|---------|----------------------|")

	for _, r := range reports {
		fmt.Fprintf(file, "| %s | %s | %d | %d | %d | %.2f |\n",
			r.Name,
			r.TTL,
			r.SizeBytes,
			r.TotalRows,
			r.ColumnCount,
			r.RowsPerMinute,
		)
	}

	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}
