"""
DuckDB Engine Module
SQL operations on data files using DuckDB
"""
import duckdb
import polars as pl
from typing import Dict, Any, List, Optional
import logging
import os

logger = logging.getLogger(__name__)


class DuckDBEngine:
    """Executes SQL operations on data files"""

    def __init__(self):
        self.conn = duckdb.connect(':memory:')

    def execute_query(
        self,
        query: str,
        files: Optional[Dict[str, str]] = None,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """
        Execute a SQL query on data files.

        Args:
            query: SQL query to execute
            files: Map of table aliases to file paths
            limit: Maximum rows to return

        Returns:
            Query results
        """
        try:
            # Register file paths as tables
            if files:
                for alias, path in files.items():
                    if path.endswith('.csv'):
                        self.conn.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM read_csv_auto('{path}')")
                    elif path.endswith('.parquet'):
                        self.conn.execute(f"CREATE OR REPLACE VIEW {alias} AS SELECT * FROM read_parquet('{path}')")
                    elif path.endswith('.xlsx') or path.endswith('.xls'):
                        # Load Excel with polars and register
                        df = pl.read_excel(path)
                        self.conn.register(alias, df.to_pandas())

            # Add LIMIT if not present
            query_upper = query.upper()
            if 'LIMIT' not in query_upper:
                query = f"{query} LIMIT {limit}"

            # Execute query
            result = self.conn.execute(query).pl()

            return {
                'success': True,
                'row_count': len(result),
                'columns': result.columns,
                'data': result.to_dicts()
            }

        except Exception as e:
            logger.error(f"Error executing query: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def update_where(
        self,
        path: str,
        updates: Dict[str, Any],
        where: str,
        output_path: Optional[str] = None,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """
        Update rows matching a condition.

        Args:
            path: Path to data file
            updates: Column updates (column_name: new_value)
            where: SQL WHERE clause
            output_path: Output path (defaults to overwrite)
            dry_run: Preview changes without applying

        Returns:
            Update results
        """
        try:
            # Load data
            df = self._load_file(path)
            self.conn.register('data', df.to_pandas())

            # Count affected rows
            count_query = f"SELECT COUNT(*) as cnt FROM data WHERE {where}"
            affected = self.conn.execute(count_query).fetchone()[0]

            if dry_run:
                # Show preview of changes
                preview_query = f"SELECT * FROM data WHERE {where} LIMIT 10"
                preview = self.conn.execute(preview_query).pl()

                return {
                    'success': True,
                    'dry_run': True,
                    'affected_rows': affected,
                    'preview': preview.to_dicts(),
                    'updates_to_apply': updates
                }
            else:
                # Build update expressions
                set_exprs = []
                for col, value in updates.items():
                    if isinstance(value, str) and not value.startswith('('):
                        set_exprs.append(f"CASE WHEN {where} THEN '{value}' ELSE {col} END AS {col}")
                    else:
                        set_exprs.append(f"CASE WHEN {where} THEN {value} ELSE {col} END AS {col}")

                # Build SELECT with all columns
                other_cols = [c for c in df.columns if c not in updates]
                all_cols = other_cols + set_exprs

                update_query = f"SELECT {', '.join(all_cols)} FROM data"
                updated_df = self.conn.execute(update_query).pl()

                # Save
                output = output_path or path
                self._save_file(updated_df, output)

                return {
                    'success': True,
                    'dry_run': False,
                    'affected_rows': affected,
                    'output_path': output
                }

        except Exception as e:
            logger.error(f"Error updating data: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def delete_where(
        self,
        path: str,
        where: str,
        output_path: Optional[str] = None,
        dry_run: bool = True
    ) -> Dict[str, Any]:
        """Delete rows matching a condition"""
        try:
            df = self._load_file(path)
            self.conn.register('data', df.to_pandas())

            # Count affected rows
            count_query = f"SELECT COUNT(*) as cnt FROM data WHERE {where}"
            affected = self.conn.execute(count_query).fetchone()[0]

            if dry_run:
                preview_query = f"SELECT * FROM data WHERE {where} LIMIT 10"
                preview = self.conn.execute(preview_query).pl()

                return {
                    'success': True,
                    'dry_run': True,
                    'rows_to_delete': affected,
                    'preview': preview.to_dicts()
                }
            else:
                # Delete by selecting NOT matching
                delete_query = f"SELECT * FROM data WHERE NOT ({where})"
                remaining_df = self.conn.execute(delete_query).pl()

                output = output_path or path
                self._save_file(remaining_df, output)

                return {
                    'success': True,
                    'dry_run': False,
                    'rows_deleted': affected,
                    'rows_remaining': len(remaining_df),
                    'output_path': output
                }

        except Exception as e:
            logger.error(f"Error deleting data: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def add_column(
        self,
        path: str,
        column: str,
        expression: str,
        output_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """Add a calculated column"""
        try:
            df = self._load_file(path)
            self.conn.register('data', df.to_pandas())

            # Build query to add column
            query = f"SELECT *, ({expression}) AS {column} FROM data"
            result_df = self.conn.execute(query).pl()

            output = output_path or path
            self._save_file(result_df, output)

            return {
                'success': True,
                'column_added': column,
                'expression': expression,
                'output_path': output,
                'sample': result_df.head(5).to_dicts()
            }

        except Exception as e:
            logger.error(f"Error adding column: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def transform_column(
        self,
        path: str,
        column: str,
        transformation: str,
        config: Optional[Dict] = None,
        output_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """Transform a column"""
        try:
            df = self._load_file(path)
            self.conn.register('data', df.to_pandas())

            config = config or {}

            # Build transformation expression
            transform_map = {
                'upper': f"UPPER({column})",
                'lower': f"LOWER({column})",
                'trim': f"TRIM({column})",
                'round': f"ROUND({column}, {config.get('decimals', 2)})",
                'abs': f"ABS({column})",
                'cast': f"CAST({column} AS {config.get('target_type', 'VARCHAR')})",
                'replace': f"REPLACE({column}, '{config.get('old', '')}', '{config.get('new', '')}')",
                'extract': f"REGEXP_EXTRACT({column}, '{config.get('pattern', '.*')}')",
                'custom_sql': config.get('expression', column)
            }

            transform_expr = transform_map.get(transformation, column)

            # Build query
            other_cols = [c for c in df.columns if c != column]
            if other_cols:
                query = f"SELECT {', '.join(other_cols)}, ({transform_expr}) AS {column} FROM data"
            else:
                query = f"SELECT ({transform_expr}) AS {column} FROM data"

            result_df = self.conn.execute(query).pl()

            output = output_path or path
            self._save_file(result_df, output)

            return {
                'success': True,
                'column': column,
                'transformation': transformation,
                'output_path': output,
                'sample': result_df.head(5).to_dicts()
            }

        except Exception as e:
            logger.error(f"Error transforming column: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def merge_tables(
        self,
        operation: str,
        tables: List[Dict[str, str]],
        join_config: Optional[Dict] = None,
        output_path: Optional[str] = None,
        output_format: str = 'csv'
    ) -> Dict[str, Any]:
        """Merge multiple tables"""
        try:
            # Register all tables
            for i, table in enumerate(tables):
                path = table['path']
                alias = table.get('alias', f't{i}')
                df = self._load_file(path)
                self.conn.register(alias, df.to_pandas())

            if operation == 'union':
                # UNION (distinct)
                selects = [f"SELECT * FROM {t.get('alias', f't{i}')}" for i, t in enumerate(tables)]
                query = " UNION ".join(selects)

            elif operation == 'union_all':
                # UNION ALL
                selects = [f"SELECT * FROM {t.get('alias', f't{i}')}" for i, t in enumerate(tables)]
                query = " UNION ALL ".join(selects)

            elif operation == 'join' and join_config:
                # JOIN
                join_type = join_config.get('type', 'inner').upper()
                on_clause = join_config.get('on', '')

                if len(tables) < 2:
                    return {'success': False, 'error': 'Join requires at least 2 tables'}

                t1 = tables[0].get('alias', 't0')
                t2 = tables[1].get('alias', 't1')
                query = f"SELECT * FROM {t1} {join_type} JOIN {t2} ON {on_clause}"

            else:
                return {'success': False, 'error': f'Invalid operation or missing config: {operation}'}

            result_df = self.conn.execute(query).pl()

            if output_path:
                self._save_file(result_df, output_path, output_format)

            return {
                'success': True,
                'operation': operation,
                'row_count': len(result_df),
                'columns': result_df.columns,
                'output_path': output_path,
                'sample': result_df.head(5).to_dicts()
            }

        except Exception as e:
            logger.error(f"Error merging tables: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _load_file(self, path: str) -> pl.DataFrame:
        """Load a file into a polars DataFrame"""
        ext = os.path.splitext(path)[1].lower()
        if ext == '.csv':
            return pl.read_csv(path)
        elif ext == '.parquet':
            return pl.read_parquet(path)
        elif ext in ['.xlsx', '.xls']:
            return pl.read_excel(path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

    def _save_file(self, df: pl.DataFrame, path: str, format: str = None):
        """Save a DataFrame to file"""
        if format is None:
            format = 'parquet' if path.endswith('.parquet') else 'csv'

        if format == 'parquet' or path.endswith('.parquet'):
            df.write_parquet(path)
        else:
            df.write_csv(path)
