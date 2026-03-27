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
	Cluster        string
	Database       string
	Name           string
	Engine         string
	TTL            string
	SizeBytes      uint64
	TotalRows      uint64
	ColumnCount    uint64
	RowsPerMinute  float64
}

func main() {
	outputFile := flag.String("output", defaultOutputFile, "Output file path for the report")
	complete := flag.Bool("complete", false, "Include column count and rows per minute (slower)")
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

	clusters, err := getClusters(ctx, conn)
	if err != nil {
		log.Fatalf("Failed to get clusters: %v", err)
	}
	log.Printf("Found %d clusters: %v", len(clusters), clusters)

	report, err := generateReport(ctx, conn, database, clusters, *complete)
	if err != nil {
		log.Fatalf("Failed to generate report: %v", err)
	}

	log.Printf("Writing report to %s", *outputFile)
	if err := writeReport(report, *outputFile, *complete); err != nil {
		log.Fatalf("Failed to write report: %v", err)
	}

	log.Printf("Report generated successfully: %s", *outputFile)
}

func getClusters(ctx context.Context, conn clickhouse.Conn) ([]string, error) {
	log.Println("Querying clusters from system.clusters")

	query := `SELECT DISTINCT cluster FROM system.clusters ORDER BY cluster`

	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query clusters: %w", err)
	}
	defer rows.Close()

	var clusters []string
	for rows.Next() {
		var cluster string
		if err := rows.Scan(&cluster); err != nil {
			return nil, fmt.Errorf("failed to scan cluster: %w", err)
		}
		clusters = append(clusters, cluster)
	}

	return clusters, rows.Err()
}

func generateReport(ctx context.Context, conn clickhouse.Conn, database string, clusters []string, complete bool) ([]TableReport, error) {
	log.Println("Querying table list from system.tables using clusterAllReplicas")

	var reports []TableReport

	for _, cluster := range clusters {
		log.Printf("Processing cluster: %s", cluster)

		query := `
			SELECT
				database,
				name,
				engine,
				total_rows,
				total_bytes,
				engine_full
			FROM clusterAllReplicas(?, system.tables)
			WHERE database = ?
			ORDER BY name
		`

		rows, err := conn.Query(ctx, query, cluster, database)
		if err != nil {
			log.Printf("Warning: failed to query tables for cluster %s: %v", cluster, err)
			continue
		}
		defer rows.Close()

		for rows.Next() {
			var db, name, engine string
			var totalRows *uint64
			var totalBytes *uint64
			var engineFull string

			if err := rows.Scan(&db, &name, &engine, &totalRows, &totalBytes, &engineFull); err != nil {
				log.Printf("Warning: failed to scan row: %v", err)
				continue
			}

			log.Printf("Processing table: %s.%s (%s)", db, name, engine)

			var rowsVal uint64
			var bytesVal uint64
			if totalRows != nil {
				rowsVal = *totalRows
			}
			if totalBytes != nil {
				bytesVal = *totalBytes
			}

			ttl := extractTTL(engineFull)

			var colCount uint64
			var rowsPerMin float64

			if complete {
				log.Printf("  - Getting column count for %s", name)
				colCount, err = getColumnCount(ctx, conn, database, name)
				if err != nil {
					log.Printf("  - Warning: failed to get column count for %s: %v", name, err)
				}

				log.Printf("  - Calculating rows per minute for %s", name)
				rowsPerMin, err = getRowsPerMinute(ctx, conn, database, name)
				if err != nil {
					log.Printf("  - Warning: failed to get rows per minute for %s: %v", name, err)
				}
			}

			reports = append(reports, TableReport{
				Cluster:       cluster,
				Database:      db,
				Name:          name,
				Engine:        engine,
				TTL:           ttl,
				SizeBytes:     bytesVal,
				TotalRows:     rowsVal,
				ColumnCount:   colCount,
				RowsPerMinute: rowsPerMin,
			})
		}
		rows.Close()
	}

	log.Printf("Processed %d tables", len(reports))
	return reports, nil
}

func extractTTL(engineFull string) string {
	upper := strings.ToUpper(engineFull)
	idx := strings.Index(upper, "TTL")
	if idx == -1 {
		return "N/A"
	}

	ttlPart := engineFull[idx+3:]
	ttlPart = strings.TrimSpace(ttlPart)

	// Find the end of TTL expression by counting parentheses
	// TTL expression ends when we reach a comma or closing paren at depth 0
	parenDepth := 0
	endIdx := -1

	for i, ch := range ttlPart {
		if ch == '(' {
			parenDepth++
		} else if ch == ')' {
			parenDepth--
			if parenDepth < 0 {
				// Found closing paren at depth 0, end here (don't include this paren)
				endIdx = i
				break
			}
		} else if ch == ',' && parenDepth == 0 {
			// Found comma at depth 0, end here
			endIdx = i
			break
		}
	}

	var ttlExpr string
	if endIdx != -1 {
		ttlExpr = strings.TrimSpace(ttlPart[:endIdx])
	} else {
		// No end delimiter found, take entire expression
		ttlExpr = strings.TrimSpace(ttlPart)
	}

	// Remove trailing closing paren if it got included
	ttlExpr = strings.TrimSuffix(ttlExpr, ")")

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

func writeReport(reports []TableReport, filePath string, complete bool) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer file.Close()

	fmt.Fprintln(file, "# ClickHouse Table Report")
	fmt.Fprintln(file, "")

	if complete {
		fmt.Fprintln(file, "| CLUSTER | DATABASE | TABLE NAME | ENGINE | TTL | SIZE (BYTES) | ROWS | COLUMNS | ROWS/MIN (10min avg) |")
		fmt.Fprintln(file, "|---------|----------|------------|--------|-----|--------------|------|---------|----------------------|")
	} else {
		fmt.Fprintln(file, "| CLUSTER | DATABASE | TABLE NAME | ENGINE | TTL | SIZE (BYTES) | ROWS |")
		fmt.Fprintln(file, "|---------|----------|------------|--------|-----|--------------|------|")
	}

	for _, r := range reports {
		if complete {
			fmt.Fprintf(file, "| %s | %s | %s | %s | %s | %d | %d | %d | %.2f |\n",
				r.Cluster,
				r.Database,
				r.Name,
				r.Engine,
				r.TTL,
				r.SizeBytes,
				r.TotalRows,
				r.ColumnCount,
				r.RowsPerMinute,
			)
		} else {
			fmt.Fprintf(file, "| %s | %s | %s | %s | %s | %d | %d |\n",
				r.Cluster,
				r.Database,
				r.Name,
				r.Engine,
				r.TTL,
				r.SizeBytes,
				r.TotalRows,
			)
		}
	}

	// Write CSV file
	csvPath := strings.TrimSuffix(filePath, ".md") + ".csv"
	csvFile, err := os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer csvFile.Close()

	if complete {
		fmt.Fprintln(csvFile, "CLUSTER,DATABASE,TABLE NAME,ENGINE,TTL,SIZE (BYTES),ROWS,COLUMNS,ROWS/MIN (10min avg)")
		for _, r := range reports {
			fmt.Fprintf(csvFile, "%s,%s,%s,%s,%s,%d,%d,%d,%.2f\n",
				r.Cluster,
				r.Database,
				r.Name,
				r.Engine,
				r.TTL,
				r.SizeBytes,
				r.TotalRows,
				r.ColumnCount,
				r.RowsPerMinute,
			)
		}
	} else {
		fmt.Fprintln(csvFile, "CLUSTER,DATABASE,TABLE NAME,ENGINE,TTL,SIZE (BYTES),ROWS")
		for _, r := range reports {
			fmt.Fprintf(csvFile, "%s,%s,%s,%s,%s,%d,%d\n",
				r.Cluster,
				r.Database,
				r.Name,
				r.Engine,
				r.TTL,
				r.SizeBytes,
				r.TotalRows,
			)
		}
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
