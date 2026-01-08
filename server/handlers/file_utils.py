"""
File Utilities
Shared file reading utilities for all handlers
"""
import polars as pl
import os
from typing import Dict, Any, List, Optional, Union

from core.config.config_manager import config

# ============================================================================
# TOKEN OPTIMIZATION CONSTANTS
# ============================================================================
# These limits help control response sizes for MCP tool outputs
# Values are loaded from config, with sensible fallbacks

DEFAULT_ROW_LIMIT = config.get('response.default_row_limit', 1000)
MAX_ROW_LIMIT = config.get('response.max_row_limit', 200000)
SAMPLE_ROW_LIMIT = config.get('response.sample_row_limit', 100)
VALUE_SAMPLE_LIMIT = config.get('response.value_sample_limit', 10)
TOP_VALUES_LIMIT = config.get('response.top_values_limit', 20)
VIOLATION_SAMPLE_LIMIT = config.get('response.violation_sample_limit', 10)
MAX_STRING_LENGTH = config.get('response.max_string_length', 200)
MAX_LIST_ITEMS = config.get('response.max_list_items', 50)
HIERARCHY_MAX_DEPTH = config.get('response.hierarchy_max_depth', 5)


def truncate_string(value: Any, max_length: int = MAX_STRING_LENGTH) -> Any:
    """Truncate string values that exceed max_length."""
    if isinstance(value, str) and len(value) > max_length:
        return value[:max_length - 3] + '...'
    return value


def truncate_dict_values(d: Dict[str, Any], max_length: int = MAX_STRING_LENGTH) -> Dict[str, Any]:
    """Truncate all string values in a dictionary."""
    return {k: truncate_string(v, max_length) for k, v in d.items()}


def truncate_row_data(rows: List[Dict[str, Any]], max_length: int = MAX_STRING_LENGTH) -> List[Dict[str, Any]]:
    """Truncate string values in a list of row dictionaries."""
    return [truncate_dict_values(row, max_length) for row in rows]


def smart_sample_df(
    df: pl.DataFrame,
    limit: int = DEFAULT_ROW_LIMIT,
    include_row_count: bool = True,
    truncate_strings: bool = True
) -> Dict[str, Any]:
    """
    Convert DataFrame to dict with smart sampling and truncation.

    Returns:
        Dict with 'data', 'row_count', 'columns', and 'truncated' flag
    """
    total_rows = len(df)
    sample_df = df.head(limit)
    data = sample_df.to_dicts()

    if truncate_strings:
        data = truncate_row_data(data)

    result = {
        'data': data,
        'columns': df.columns,
    }

    if include_row_count:
        result['row_count'] = total_rows
        result['sample_size'] = len(data)
        if total_rows > limit:
            result['truncated'] = True
            result['note'] = f'Showing first {limit} of {total_rows} rows'

    return result


def limit_list(items: List[Any], limit: int = MAX_LIST_ITEMS, item_name: str = 'items') -> Dict[str, Any]:
    """
    Limit a list and return metadata about truncation.

    Returns:
        Dict with 'items', 'total_count', and optional 'truncated' note
    """
    total = len(items)
    limited = items[:limit]

    result = {
        'items': limited,
        'count': len(limited),
        'total_count': total
    }

    if total > limit:
        result['truncated'] = True
        result['note'] = f'Showing {limit} of {total} {item_name}'

    return result


def summarize_value_counts(
    series: pl.Series,
    limit: int = TOP_VALUES_LIMIT,
    include_percentages: bool = True
) -> List[Dict[str, Any]]:
    """
    Get top N value counts with optional percentages.
    More token-efficient than full value_counts().to_dicts()
    """
    total = len(series)
    non_null = series.drop_nulls()
    value_counts = non_null.value_counts().sort('count', descending=True).head(limit)

    result = []
    for row in value_counts.iter_rows(named=True):
        entry = {
            'value': truncate_string(row[series.name]),
            'count': row['count']
        }
        if include_percentages:
            entry['pct'] = round(row['count'] / total * 100, 1)
        result.append(entry)

    return result


def compact_column_stats(series: pl.Series) -> Dict[str, Any]:
    """
    Get compact column statistics - more token-efficient than full describe().
    Only includes non-null, meaningful statistics.
    """
    stats = {'dtype': str(series.dtype)}

    # Only compute what's needed
    null_count = series.null_count()
    if null_count > 0:
        stats['nulls'] = null_count
        stats['null_pct'] = round(null_count / len(series) * 100, 1)

    unique = series.n_unique()
    stats['unique'] = unique

    # Numeric stats only for numeric types
    if series.dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]:
        non_null = series.drop_nulls()
        if len(non_null) > 0:
            stats['min'] = non_null.min()
            stats['max'] = non_null.max()
            stats['mean'] = round(non_null.mean(), 2)

    return stats


def format_sample_values(
    series: pl.Series,
    limit: int = VALUE_SAMPLE_LIMIT
) -> List[Any]:
    """Get a compact list of sample values from a series."""
    unique_vals = series.drop_nulls().unique().head(limit).to_list()
    return [truncate_string(v) for v in unique_vals]


def compact_violation_summary(
    violations: List[Dict[str, Any]],
    limit: int = VIOLATION_SAMPLE_LIMIT
) -> Dict[str, Any]:
    """
    Create a compact violation summary with limited samples.
    """
    total = len(violations)
    sample = violations[:limit]

    result = {
        'total_violations': total,
        'samples': truncate_row_data(sample)
    }

    if total > limit:
        result['note'] = f'{total - limit} more violations not shown'

    return result


def estimate_response_size(data: Any) -> int:
    """
    Rough estimate of JSON response size in characters.
    Useful for debugging token usage.
    """
    import json
    try:
        return len(json.dumps(data, default=str))
    except Exception:
        return 0


def read_data_file(file_path: str) -> pl.DataFrame:
    """
    Read a data file based on its extension.

    Supports:
    - CSV (.csv)
    - Parquet (.parquet)
    - Excel (.xlsx, .xls)

    Uses fastexcel/calamine engine for Excel when available for better performance.
    """
    ext = os.path.splitext(file_path)[1].lower()

    if ext == '.csv':
        return pl.read_csv(file_path)
    elif ext == '.parquet':
        return pl.read_parquet(file_path)
    elif ext in ['.xlsx', '.xls']:
        # Use calamine engine (fastexcel) if available, fallback to openpyxl
        try:
            return pl.read_excel(file_path, engine='calamine')
        except Exception:
            return pl.read_excel(file_path)
    else:
        raise ValueError(f'Unsupported file format: {ext}. Supported formats: .csv, .parquet, .xlsx, .xls')


SUPPORTED_FORMATS = ['.csv', '.parquet', '.xlsx', '.xls']
SUPPORTED_FORMATS_STR = 'CSV, Excel (.xlsx/.xls), and Parquet'

# Response size limit for MCP (in characters, ~4 chars per token)
MAX_RESPONSE_CHARS = config.get('response.max_response_chars', 500000)
ENABLE_PAGINATION = config.get('response.enable_pagination', True)
DEFAULT_PAGE_SIZE = config.get('response.default_page_size', 100)


def paginate_response(
    data: Dict[str, Any],
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    data_key: str = 'data'
) -> Dict[str, Any]:
    """
    Add pagination to response data.

    Args:
        data: The response dict containing a list under data_key
        page: Page number (1-indexed)
        page_size: Items per page
        data_key: Key in data dict containing the list to paginate

    Returns:
        Paginated response with pagination metadata
    """
    if not ENABLE_PAGINATION or data_key not in data:
        return data

    items = data[data_key]
    if not isinstance(items, list):
        return data

    total_items = len(items)
    total_pages = (total_items + page_size - 1) // page_size

    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size

    result = data.copy()
    result[data_key] = items[start_idx:end_idx]
    result['pagination'] = {
        'page': page,
        'page_size': page_size,
        'total_items': total_items,
        'total_pages': total_pages,
        'has_next': page < total_pages,
        'has_prev': page > 1
    }

    return result


def truncate_response(
    data: Dict[str, Any],
    max_chars: int = MAX_RESPONSE_CHARS
) -> Dict[str, Any]:
    """
    Truncate response if it exceeds max_chars.
    Adds a warning and truncates large data arrays.

    Args:
        data: Response dict to potentially truncate
        max_chars: Maximum response size in characters

    Returns:
        Truncated response dict
    """
    import json

    try:
        response_str = json.dumps(data, default=str)
        current_size = len(response_str)

        if current_size <= max_chars:
            return data

        # Response is too large, need to truncate
        result = data.copy()

        # Find and truncate large data arrays
        keys_to_truncate = ['data', 'rows', 'results', 'items', 'records', 'samples', 'violations']

        for key in keys_to_truncate:
            if key in result and isinstance(result[key], list):
                original_len = len(result[key])
                # Progressively reduce until under limit
                while len(result[key]) > 10:
                    result[key] = result[key][:len(result[key]) // 2]
                    test_str = json.dumps(result, default=str)
                    if len(test_str) <= max_chars:
                        break

                if len(result[key]) < original_len:
                    result[f'{key}_truncated'] = True
                    result[f'{key}_original_count'] = original_len
                    result[f'{key}_shown'] = len(result[key])

        # Add warning
        result['_response_truncated'] = True
        result['_truncation_reason'] = f'Response exceeded {max_chars:,} characters'
        result['_suggestion'] = 'Use pagination parameters (page, page_size) or add filters to reduce response size'

        return result

    except Exception:
        return data


def safe_response(data: Dict[str, Any], max_chars: int = MAX_RESPONSE_CHARS) -> Dict[str, Any]:
    """
    Ensure response is safe to return (within size limits).
    Combines truncation and size checking.

    Args:
        data: Response to check
        max_chars: Maximum allowed characters

    Returns:
        Safe response dict
    """
    return truncate_response(data, max_chars)
