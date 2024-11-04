.PHONY: help setup setup_python start_db stop_db init_db ingest_odi_all report_season clean

setup: setup_python start_db init_db

# Help
# ---------------------------------------------------

help:
	@echo "Available targets:"
	@echo "  setup               Set up the environment and database"
	@echo "  help                Show this help message"
	@echo "  setup_python        Create and set up Python virtual environment"
	@echo "  start_db            Start the PostgreSQL database"
	@echo "  stop_db             Stop the PostgreSQL database"
	@echo "  init_db             Initialize the database schema"
	@echo "  ingest_odi_all      Ingest all data"
	@echo "  clean               Clean up data and Docker volumes"

# Python environment + dependencies
# ---------------------------------------------------

setup_python:
	@echo "Creating Python environment..."
	@python3 -m venv venv
	@echo "Installing dependencies..."
	@venv/bin/pip install --upgrade pip
	@venv/bin/pip install -r requirements.txt

# Database
# ---------------------------------------------------

start_db:
	@docker-compose up -d
	@echo "PostgreSQL database started."

stop_db:
	@docker-compose down
	@echo "PostgreSQL database stopped."

init_db:
	@echo "Initializing the database schema..."
	@venv/bin/python -c "from app.infrastructure.database.db_wrapper import PostgresWrapper; PostgresWrapper().init_db()"
	@echo "Database schema initialized."

# Write
# ---------------------------------------------------

ingest_odi_all:
	@echo "Starting data ingestion for ODI cricket matches..."
	@venv/bin/python run.py ingest --mode odi_all
	@echo "Data ingestion process completed."

# Clean
# ---------------------------------------------------

clean:
	@docker-compose down -v
	@rm -rf data/01_raw/*
	@rm -rf data/02_unzipped/*
	@echo "Cleaned up data."
