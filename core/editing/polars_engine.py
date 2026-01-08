"""
Polars Engine Module
Data transformations using Polars
"""
import polars as pl
from typing import Dict, Any, List, Optional, Union
import logging
import os

logger = logging.getLogger(__name__)

# Token optimization constants
SAMPLE_OUTPUT_LIMIT = 50


class PolarsEngine:
    """Executes data operations using Polars"""

    def __init__(self):
        pass

    def load_file(self, path: str, n_rows: Optional[int] = None) -> pl.DataFrame:
        """Load a file into a DataFrame"""
        ext = os.path.splitext(path)[1].lower()

        if ext == '.csv':
            if n_rows:
                return pl.read_csv(path, n_rows=n_rows)
            return pl.read_csv(path)
        elif ext == '.parquet':
            if n_rows:
                return pl.read_parquet(path, n_rows=n_rows)
            return pl.read_parquet(path)
        elif ext in ['.xlsx', '.xls']:
            return pl.read_excel(path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")

    def save_file(self, df: pl.DataFrame, path: str, format: str = 'csv'):
        """Save a DataFrame to file"""
        if format == 'parquet' or path.endswith('.parquet'):
            df.write_parquet(path)
        else:
            df.write_csv(path)

    def filter_rows(
        self,
        df: pl.DataFrame,
        conditions: List[Dict[str, Any]]
    ) -> pl.DataFrame:
        """Filter rows based on conditions"""
        result = df

        for cond in conditions:
            column = cond['column']
            operator = cond.get('operator', '==')
            value = cond['value']

            if operator == '==':
                result = result.filter(pl.col(column) == value)
            elif operator == '!=':
                result = result.filter(pl.col(column) != value)
            elif operator == '>':
                result = result.filter(pl.col(column) > value)
            elif operator == '>=':
                result = result.filter(pl.col(column) >= value)
            elif operator == '<':
                result = result.filter(pl.col(column) < value)
            elif operator == '<=':
                result = result.filter(pl.col(column) <= value)
            elif operator == 'in':
                result = result.filter(pl.col(column).is_in(value))
            elif operator == 'not_in':
                result = result.filter(~pl.col(column).is_in(value))
            elif operator == 'contains':
                result = result.filter(pl.col(column).str.contains(value))
            elif operator == 'is_null':
                result = result.filter(pl.col(column).is_null())
            elif operator == 'is_not_null':
                result = result.filter(pl.col(column).is_not_null())

        return result

    def aggregate(
        self,
        df: pl.DataFrame,
        group_by: List[str],
        aggregations: List[Dict[str, Any]]
    ) -> pl.DataFrame:
        """Aggregate data"""
        agg_exprs = []

        for agg in aggregations:
            column = agg['column']
            func = agg.get('function', 'sum')
            alias = agg.get('alias', f"{column}_{func}")

            if func == 'sum':
                agg_exprs.append(pl.col(column).sum().alias(alias))
            elif func == 'avg' or func == 'mean':
                agg_exprs.append(pl.col(column).mean().alias(alias))
            elif func == 'count':
                agg_exprs.append(pl.col(column).count().alias(alias))
            elif func == 'min':
                agg_exprs.append(pl.col(column).min().alias(alias))
            elif func == 'max':
                agg_exprs.append(pl.col(column).max().alias(alias))
            elif func == 'first':
                agg_exprs.append(pl.col(column).first().alias(alias))
            elif func == 'last':
                agg_exprs.append(pl.col(column).last().alias(alias))

        return df.group_by(group_by).agg(agg_exprs)

    def join_tables(
        self,
        left_df: pl.DataFrame,
        right_df: pl.DataFrame,
        left_on: str,
        right_on: str,
        how: str = 'inner'
    ) -> pl.DataFrame:
        """Join two DataFrames"""
        return left_df.join(right_df, left_on=left_on, right_on=right_on, how=how)

    def pivot(
        self,
        df: pl.DataFrame,
        values: str,
        index: str,
        columns: str,
        aggregate_function: str = 'sum'
    ) -> pl.DataFrame:
        """Pivot a DataFrame"""
        agg_func = {
            'sum': 'sum',
            'mean': 'mean',
            'count': 'count',
            'min': 'min',
            'max': 'max',
            'first': 'first'
        }.get(aggregate_function, 'sum')

        return df.pivot(values=values, index=index, columns=columns, aggregate_function=agg_func)

    def unpivot(
        self,
        df: pl.DataFrame,
        id_vars: List[str],
        value_vars: List[str],
        variable_name: str = 'variable',
        value_name: str = 'value'
    ) -> pl.DataFrame:
        """Unpivot (melt) a DataFrame"""
        return df.unpivot(
            on=value_vars,
            index=id_vars,
            variable_name=variable_name,
            value_name=value_name
        )

    def fill_nulls(
        self,
        df: pl.DataFrame,
        column: str,
        strategy: str = 'literal',
        value: Any = None
    ) -> pl.DataFrame:
        """Fill null values in a column"""
        if strategy == 'literal':
            return df.with_columns(pl.col(column).fill_null(value))
        elif strategy == 'forward':
            return df.with_columns(pl.col(column).forward_fill())
        elif strategy == 'backward':
            return df.with_columns(pl.col(column).backward_fill())
        elif strategy == 'mean':
            mean_val = df[column].mean()
            return df.with_columns(pl.col(column).fill_null(mean_val))
        elif strategy == 'median':
            median_val = df[column].median()
            return df.with_columns(pl.col(column).fill_null(median_val))
        elif strategy == 'mode':
            mode_val = df[column].mode().first()
            return df.with_columns(pl.col(column).fill_null(mode_val))
        else:
            return df

    def cast_columns(
        self,
        df: pl.DataFrame,
        casts: Dict[str, str]
    ) -> pl.DataFrame:
        """Cast columns to new types"""
        type_map = {
            'int': pl.Int64,
            'integer': pl.Int64,
            'float': pl.Float64,
            'decimal': pl.Float64,
            'string': pl.Utf8,
            'str': pl.Utf8,
            'bool': pl.Boolean,
            'boolean': pl.Boolean,
            'date': pl.Date,
            'datetime': pl.Datetime
        }

        exprs = []
        for column, target_type in casts.items():
            pl_type = type_map.get(target_type.lower(), pl.Utf8)
            exprs.append(pl.col(column).cast(pl_type))

        return df.with_columns(exprs)

    def transform(
        self,
        file_path: str,
        transformations: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Apply a series of transformations to a data file.

        Args:
            file_path: Path to the data file
            transformations: List of transformation definitions

        Returns:
            Transformation result with 'success' and 'df' keys
        """
        try:
            df = self.load_file(file_path)
            original_columns = list(df.columns)
            transforms_executed = []

            for i, transform in enumerate(transformations):
                # Handle case where transform might be a string (JSON not parsed)
                if isinstance(transform, str):
                    import json
                    try:
                        transform = json.loads(transform)
                    except Exception as e:
                        logger.error(f"Transform {i} is not valid JSON: {e}")
                        transforms_executed.append({'index': i, 'error': 'invalid JSON string'})
                        continue

                if not isinstance(transform, dict):
                    logger.error(f"Transform {i} is not a dict: {type(transform).__name__}")
                    transforms_executed.append({'index': i, 'error': f'not a dict: {type(transform).__name__}'})
                    continue

                # Get transform type - check multiple possible key names and normalize
                transform_type = str(
                    transform.get('type') or
                    transform.get('operation') or
                    transform.get('op') or
                    ''
                ).lower().strip()

                # Support both nested config and flat structure
                config = dict(transform.get('config', {}))
                # Merge top-level keys into config
                for key, value in transform.items():
                    if key not in ('type', 'operation', 'op', 'config') and key not in config:
                        config[key] = value

                if transform_type == 'filter':
                    conditions = config.get('conditions', [])
                    df = self.filter_rows(df, conditions)

                elif transform_type == 'select':
                    columns = config.get('columns') or config.get('column') or config.get('cols')
                    if columns:
                        if isinstance(columns, str):
                            columns = [columns]
                        df = df.select(columns)
                        transforms_executed.append({'type': 'select', 'columns': columns, 'executed': True})
                    else:
                        transforms_executed.append({'type': 'select', 'executed': False, 'reason': 'no columns specified'})

                elif transform_type in ('rename', 'rename_column', 'rename_columns'):
                    renames = config.get('renames', {})
                    # Also support single column rename with 'old_name' and 'new_name'
                    if not renames:
                        old_name = config.get('old_name') or config.get('from') or config.get('column')
                        new_name = config.get('new_name') or config.get('to') or config.get('name')
                        if old_name and new_name:
                            renames = {old_name: new_name}
                    df = df.rename(renames)

                elif transform_type == 'cast':
                    casts = config.get('casts', {})
                    df = self.cast_columns(df, casts)

                elif transform_type == 'aggregate':
                    group_by = config.get('group_by', [])
                    aggregations = config.get('aggregations', [])
                    df = self.aggregate(df, group_by, aggregations)

                elif transform_type == 'sort':
                    columns = config.get('columns', [])
                    descending = config.get('descending', False)
                    df = df.sort(columns, descending=descending)

                elif transform_type == 'fill_null':
                    column = config.get('column')
                    strategy = config.get('strategy', 'literal')
                    value = config.get('value')
                    if column:
                        df = self.fill_nulls(df, column, strategy, value)

                elif transform_type == 'add_column':
                    name = config.get('name')
                    expression = config.get('expression')
                    if name and expression:
                        # Simple expression evaluation
                        df = df.with_columns(pl.lit(expression).alias(name))

                elif transform_type == 'drop_columns':
                    columns = config.get('columns', [])
                    df = df.drop(columns)

                elif transform_type == 'drop_nulls':
                    columns = config.get('columns')
                    if columns:
                        df = df.drop_nulls(subset=columns)
                    else:
                        df = df.drop_nulls()

                elif transform_type == 'unique':
                    columns = config.get('columns')
                    keep = config.get('keep', 'first')
                    if columns:
                        df = df.unique(subset=columns, keep=keep)
                    else:
                        df = df.unique(keep=keep)

                elif transform_type == 'limit':
                    n = config.get('n', 1000)
                    df = df.head(n)
                    transforms_executed.append({'type': 'limit', 'n': n, 'executed': True})

                else:
                    # Unrecognized transform type
                    logger.warning(f"Unrecognized transform type: '{transform_type}'. Full transform: {transform}")
                    transforms_executed.append({
                        'type': transform_type or '(empty)',
                        'executed': False,
                        'reason': 'unrecognized transform type',
                        'raw_transform': transform
                    })

            return {
                'success': True,
                'df': df,
                'row_count': len(df),
                'columns': list(df.columns),
                'original_columns': original_columns,
                'transformations_applied': len(transformations),
                'transforms_executed': transforms_executed
            }

        except Exception as e:
            logger.error(f"Error transforming data: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def union_files(
        self,
        file_paths: List[str]
    ) -> Dict[str, Any]:
        """
        Union multiple files into a single DataFrame.

        Args:
            file_paths: List of file paths to union

        Returns:
            Union result with 'success' and 'df' keys
        """
        try:
            dfs = []
            for path in file_paths:
                df = self.load_file(path)
                dfs.append(df)

            if not dfs:
                return {'success': False, 'error': 'No files provided'}

            # Concat all dataframes
            result_df = pl.concat(dfs, how='vertical_relaxed')

            return {
                'success': True,
                'df': result_df,
                'row_count': len(result_df),
                'columns': result_df.columns,
                'source_files': len(file_paths)
            }

        except Exception as e:
            logger.error(f"Error unioning files: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def join_files(
        self,
        file_paths: List[str],
        join_keys: List[str],
        how: str = 'inner'
    ) -> Dict[str, Any]:
        """
        Join multiple files on common keys.

        Args:
            file_paths: List of file paths to join
            join_keys: Column names to join on
            how: Join type ('inner', 'left', 'outer', 'cross')

        Returns:
            Join result with 'success' and 'df' keys
        """
        try:
            if len(file_paths) < 2:
                return {'success': False, 'error': 'At least 2 files required for join'}

            dfs = [self.load_file(path) for path in file_paths]

            # Start with first dataframe
            result_df = dfs[0]

            # Join subsequent dataframes
            for i, df in enumerate(dfs[1:], 1):
                # Suffix duplicate columns
                suffix = f"_file{i}"
                result_df = result_df.join(
                    df,
                    on=join_keys,
                    how=how,
                    suffix=suffix
                )

            return {
                'success': True,
                'df': result_df,
                'row_count': len(result_df),
                'columns': result_df.columns,
                'source_files': len(file_paths),
                'join_keys': join_keys,
                'join_type': how
            }

        except Exception as e:
            logger.error(f"Error joining files: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
