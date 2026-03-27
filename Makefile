.PHONY: run build clean fmt help

APP_NAME := clickhouse_verify
OUTPUT ?= report.md

help:
	@echo "Available targets:"
	@echo "  make run              - Run the application (default output: report.md)"
	@echo "  make run OUTPUT=file  - Run with custom output file"
	@echo "  make build            - Build the binary"
	@echo "  make clean            - Remove build artifacts"
	@echo "  make fmt              - Format code"
	@echo "  make help             - Show this help message"

run:
	go run main.go -output $(OUTPUT)

build:
	go build -o $(APP_NAME) main.go

clean:
	rm -f $(APP_NAME)

fmt:
	go fmt ./...
