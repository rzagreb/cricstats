from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable, Optional, Union

import psycopg2
from dotenv import dotenv_values
from psycopg2.extensions import connection
from psycopg2.extras import Json

from app.infrastructure.database.models import NormRef

log = logging.getLogger(__name__)

config = {**dotenv_values(), **os.environ}
DATABASE_CONFIG = {
    "host": config["POSTGRES_HOST"],
    "port": config["POSTGRES_PORT"],
    "dbname": config["POSTGRES_DB"],
    "user": config["POSTGRES_USER"],
    "password": config["POSTGRES_PASSWORD"],
}


class PostgresWrapper:
    def __init__(self, conn: Optional[connection] = None):
        if conn is None:
            self.conn = psycopg2.connect(**DATABASE_CONFIG)
        else:
            self.conn = conn

        self.cte_name_orig = "new_rows"
        self.cte_name_norm = "new_rows_norm"

    def execute(
        self, sql: str, params: Optional[tuple[Any, ...]] = None
    ) -> list[dict[str, Any]]:
        """
        Executes a SQL query and returns the results as a list of dictionaries.

        Args:
            sql (str): The SQL query to execute.
            params (Optional[tuple[Any, ...]]): Optional tuple of parameters to pass with the SQL query.

        Returns:
            list[dict[str, Any]]: A list where each item is a dictionary representing a row from the query result.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(sql, params)

            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                result = [dict(zip(columns, row)) for row in rows]
            else:
                self.conn.commit()
                result = []

        return result

    def init_db(self):
        """Initializes the database by creating the schema and tables."""
        cursor = self.conn.cursor()
        schema_path = os.path.join(
            os.path.dirname(__file__), "sql_scripts", "schema.sql"
        )
        with open(schema_path, "r") as f:
            cursor.execute(f.read())
        self.conn.commit()
        cursor.close()

    def insert_values(
        self,
        table: str,
        rows: list[dict[str, Union[str, int, tuple[Any, ...], None, Json]]],
        columns_to_insert: Optional[list[str]] = None,
        unique_columns: Optional[list[Union[str, tuple[str, ...]]]] = None,
        norm_values: Optional[dict[str, NormRef]] = None,
        custom_columns_types: Optional[dict[str, str]] = None,
        schema_name: str = "public",
    ):
        """Inserts values into a PostgreSQL table with optional normalization and conflict checking.

        Parameters:
            table (str): Name of the table to insert into.
            rows (list[dict[str, Any]]): List of dictionaries containing values to insert.
            columns_to_insert (Optional[list[str]]): Columns to insert values into. Default is all columns.
            unique_columns (Optional[list[Union[str, tuple[str, ...]]]): Columns to check for conflicts.
            norm_values (Optional[dict[str, NormRef]]): Dictionary of columns to normalize.
            custom_columns_types (Optional[dict[str, str]]): If present, then uses the provided column types for the insert query.
            schema_name (str): Name of the schema where the table resides. Default is 'public'.

        Raises:
            ValueError: If any of the value dictionaries are missing required columns.
            psycopg2.DatabaseError: If a database error occurs during the operation.
        """
        if not rows:
            log.error(f"No values provided to insert into '{table}'.")
            return

        self._validate_columns(rows)

        sql, params = self._insert_values_query(
            table=table,
            rows=rows,
            columns_to_insert=columns_to_insert,
            unique_columns=unique_columns,
            norm_values=norm_values,
            custom_columns_types=custom_columns_types,
            schema_name=schema_name,
        )

        with self.conn.cursor() as cursor:
            try:
                cursor.execute(sql, params)
                self.conn.commit()
                log.info(f"Inserted {cursor.rowcount} new rows into '{table}'.")
            except Exception as e:
                self.conn.rollback()
                print("-" * 50)
                try:
                    print(f"{cursor.mogrify(sql, params).decode()}")
                except Exception:
                    print(sql)
                print("-" * 50)
                log.error(f"An error occurred while inserting into '{table}': {e}")

                raise

    def _insert_values_query(
        self,
        table: str,
        rows: list[dict[str, Any]],
        columns_to_insert: Optional[list[str]],
        unique_columns: Optional[list[Union[str, tuple[str, ...]]]] = None,
        norm_values: Optional[dict[str, NormRef]] = None,
        custom_columns_types: Optional[dict[str, str]] = None,
        schema_name: str = "public",
    ) -> tuple[str, tuple[Any, ...]]:
        columns = list(rows[0].keys())

        cols_csv = ", ".join([f'"{col}"' for col in columns])
        values_placeholders = ", ".join(
            ["(" + ", ".join(["%s"] * len(columns)) + ")" for _ in rows]
        )

        # CTE with values to insert
        query_parts = [
            f"WITH {self.cte_name_orig} ({cols_csv}) AS (VALUES {values_placeholders})"
        ]

        # CTE with normalized values to insert
        source_table = self.cte_name_orig
        if norm_values:
            sql_cte_norm_values = self._make_cte_for_norm_values(
                norm_values, columns, self.cte_name_orig, self.cte_name_norm
            )
            query_parts.append(sql_cte_norm_values)
            source_table = self.cte_name_norm

        self._validate_values(rows)

        if custom_columns_types is None:
            custom_columns_types = {}

        params = tuple(
            row[col] if not isinstance(row[col], (dict, list)) else json.dumps(row[col])
            for row in rows
            for col in columns
        )

        # Add handling for JSONB
        selected_columns_to_insert = columns_to_insert or columns
        columns_insert_csv = ", ".join(
            [f'"{col}"' for col in selected_columns_to_insert]
        )
        columns_insert_values_csv = ", ".join(
            [
                f'"{col}"'
                if col not in custom_columns_types
                else f'"{col}"::{custom_columns_types[col]}'
                for col in selected_columns_to_insert
            ]
        )

        query_parts.append(
            f"""
                INSERT INTO "{schema_name}"."{table}" ({columns_insert_csv})
                SELECT
                    {columns_insert_values_csv}
                FROM
                    {source_table}
                WHERE 1=1
            """
        )

        # Handle conflict checking if conflict_columns is provided
        if unique_columns:
            sql_unique_columns_clause = self._get_exclusion_clause(
                schema_name=schema_name,
                target_table=table,
                source_table=source_table,
                conflict_columns=unique_columns,
            )
            query_parts.append(sql_unique_columns_clause)

        # Combine all parts of the query
        sql = "\n".join(query_parts)

        return sql, params

    def _validate_columns(self, rows: list[dict]):
        columns = list(rows[0].keys())

        # Check if all value dictionaries have the same columns
        for idx, value_dict in enumerate(rows):
            missing_columns = set(columns) - set(value_dict.keys())
            if missing_columns:
                raise ValueError(
                    f"Value at index {idx} is missing columns: {', '.join(missing_columns)}"
                )

    def _validate_values(self, rows: list[dict]):
        # Make sure none of values are ellipsis
        for idx, value_dict in enumerate(rows):
            for key, value in value_dict.items():
                if value is ...:
                    raise ValueError(
                        f"Value at index {idx} has ellipsis value for key {key}"
                    )

    def _make_cte_for_norm_values(
        self,
        norm_values: dict[str, NormRef],
        columns: list[str],
        cte_name_orig: str,
        cte_name_norm: str,
    ) -> str:
        norm_cols = {c: f'{cte_name_orig}."{c}"' for c in columns}
        joins_clauses = []

        for join_index, (col, rule) in enumerate(norm_values.items()):
            table_alias = f"{rule.t2_name}_{join_index}"

            # Pick new column value from reference table
            norm_cols[col] = f'"{table_alias}"."{rule.t2_key_value}" AS "{col}"'

            joins = self._make_cte_for_norm_values__make_joins(
                cte_name_orig, rule, table_alias
            )
            joins_combined = " AND ".join(joins)

            join_clause = f"""
                LEFT JOIN "{rule.t2_name}" "{table_alias}"
                    ON {joins_combined} 
            """
            joins_clauses.append(join_clause)

        norm_cols_combined = ",\n".join(norm_cols[c] for c in columns)
        joins_clauses_combined = "\n".join(joins_clauses)

        # CTE for normalized rows
        sql_cte = f"""
            , {cte_name_norm} AS (
                SELECT
                    {norm_cols_combined}
                FROM {cte_name_orig}
                {joins_clauses_combined}
            )
        """
        return sql_cte

    def _make_cte_for_norm_values__make_joins(
        self, cte_name_orig: str, rule: NormRef, table_alias: str
    ) -> list[str]:
        joins = []
        # Join on single column
        if isinstance(rule.t1_key_join, str) and isinstance(rule.t2_key_join, str):
            joins.append(
                f'{cte_name_orig}."{rule.t1_key_join}" = "{table_alias}"."{rule.t2_key_join}"'
            )
        # Join on multiple columns
        elif isinstance(rule.t1_key_join, Iterable) and isinstance(
            rule.t2_key_join, Iterable
        ):
            if len(rule.t1_key_join) != len(rule.t2_key_join):
                raise ValueError(
                    f"Length of join keys must be the same, got {len(rule.t1_key_join)} and {len(rule.t2_key_join)}"
                )
            for t1, t2 in zip(rule.t1_key_join, rule.t2_key_join):
                joins.append(f'{cte_name_orig}."{t1}" = "{table_alias}"."{t2}"')
        else:
            raise TypeError(
                f"Join keys must be either a string or a tuple of strings, got {type(rule.t1_key_join)} and {type(rule.t2_key_join)}"
            )

        return joins

    def _get_exclusion_clause(
        self,
        schema_name: str,
        target_table: str,
        source_table: str,
        conflict_columns: list[Union[str, tuple[str, ...]]],
    ) -> str:
        """Returns a SQL clause to check for conflicts based on unique columns.

        Args:
            schema_name (str): Name of the schema where the target table resides.
            target_table (str): Name of the target table to check for conflicts.
            source_table (str): Name of the source table to insert from.
            conflict_columns (list[Union[str, tuple[str, ...]]]): Columns to check for conflicts.

        Returns:
            str: SQL clause to check for conflicts based on unique columns.
        """
        conflict_conditions = []

        for conflict in conflict_columns:
            if isinstance(conflict, str):
                condition = f'p."{conflict}" = {source_table}."{conflict}"'
            elif isinstance(conflict, tuple):
                condition = " AND ".join(
                    [f'p."{col}" = {source_table}."{col}"' for col in conflict]
                )
            else:
                raise TypeError(
                    f"Conflict column must be a string or a tuple of strings, got {type(conflict)}"
                )
            conflict_conditions.append(f"({condition})")

        combined_conditions = " OR ".join(conflict_conditions)

        sql = f"""
            AND NOT EXISTS (
                SELECT 1 FROM "{schema_name}"."{target_table}" p
                WHERE {combined_conditions}
            )
        """
        return sql
