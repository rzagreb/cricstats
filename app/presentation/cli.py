from __future__ import annotations

import sys
from typing import Any

import click

from app.application.services.ingest import INGEST_MODES
from app.application.services.query import REPORT_MODES


@click.group()
def cli():
    ...


@cli.command()
@click.option("--init_db", is_flag=True, help="Initialize the database.")
def init_db():
    """Initialize the database."""
    from app.infrastructure.database.db_wrapper import PostgresWrapper

    try:
        PostgresWrapper().init_db()
        click.echo("Database initialized successfully.")
    except Exception as e:
        click.echo(f"Failed to initialize database: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option(
    "--mode",
    type=click.Choice(sorted(list(INGEST_MODES.keys())), case_sensitive=True),
    default="all",
    show_default=True,
    help="Ingest mode: 'all' or 'incremental'.",
)
def ingest(mode: str):
    """Run the data ingestion process."""
    from app.application.services.ingest import IngestService

    try:
        service = IngestService()
        service.run_ingestion(ingest_mode=mode)
        click.echo(f"Data ingestion completed in '{mode}' mode.")
    except Exception as e:
        click.echo(f"Data ingestion failed: {e}", err=True)
        raise


@cli.command()
@click.option(
    "--name",
    type=click.Choice(sorted(list(REPORT_MODES.keys())), case_sensitive=True),
    required=True,
    help="Type of report to generate.",
)
@click.option(
    "--season", type=str, required=True, help="Season for which to generate the report."
)
def report(name: str, season: str):
    """Generate a report."""
    from app.application.services.query import QueryService

    try:
        service = QueryService()
        data = service.get_season_report(name, season)
        click.echo(_print_table(data))
    except Exception as e:
        click.echo(f"Failed to generate report: {e}", err=True)
        sys.exit(1)


def _print_table(data: list[dict[str, Any]]) -> None:
    """Print list of dictionaries as a ASCII table."""
    if not data:
        print("No data to display.")
        return

    columns = list(data[0].keys())

    # Calculate column widths
    col_widths = {}
    for col in columns:
        max_len = len(str(col))
        for row in data:
            cell = row.get(col, "")
            cell_str = str(cell)
            if len(cell_str) > max_len:
                max_len = len(cell_str)
        col_widths[col] = max_len

    # Print headers
    header = " | ".join(f"{str(col).ljust(col_widths[col])}" for col in columns)
    separator = "-+-".join("-" * col_widths[col] for col in columns)
    print(header)
    print(separator)

    # Print main data
    for row in data:
        row_str = " | ".join(
            f"{str(row.get(col, '')).ljust(col_widths[col])}" for col in columns
        )
        print(row_str)
