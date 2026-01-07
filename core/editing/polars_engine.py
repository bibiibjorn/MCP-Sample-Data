"""
Polars Engine Module
Data transformations using Polars
"""
import polars as pl
from typing import Dict, Any, List, Optional
import logging
import os

logger = logging.getLogger(__name__)


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
