"""
Discovery Handlers
Handlers for data discovery and analysis tools
"""
import polars as pl
from typing import Dict, Any, List, Optional
import os
import glob as glob_module

from core.discovery import SchemaInference, DomainDetector, PatternAnalyzer, RelationshipFinder
from server.tool_schemas import TOOL_SCHEMAS
from server.handlers.file_utils import (
    read_data_file, SUPPORTED_FORMATS,
    truncate_row_data, format_sample_values, summarize_value_counts,
    SAMPLE_ROW_LIMIT, TOP_VALUES_LIMIT, VALUE_SAMPLE_LIMIT
)


def register_discovery_handlers(registry):
    """Register all discovery handlers"""

    schema_inference = SchemaInference()
    domain_detector = DomainDetector()
    pattern_analyzer = PatternAnalyzer()
    relationship_finder = RelationshipFinder()

    # 01_list_files
    def list_files(directory: str, pattern: str = '*', recursive: bool = False) -> Dict[str, Any]:
        """List data files in a directory"""
        if not os.path.exists(directory):
            return {'success': False, 'error': f'Directory not found: {directory}'}

        if not os.path.isdir(directory):
            return {'success': False, 'error': f'Not a directory: {directory}'}

        try:
            files = []
            if recursive:
                search_pattern = os.path.join(directory, '**', pattern)
                matches = glob_module.glob(search_pattern, recursive=True)
            else:
                search_pattern = os.path.join(directory, pattern)
                matches = glob_module.glob(search_pattern)

            for file_path in matches:
                if os.path.isfile(file_path):
                    ext = os.path.splitext(file_path)[1].lower()
                    size = os.path.getsize(file_path)
                    is_supported = ext in SUPPORTED_FORMATS

                    files.append({
                        'path': file_path,
                        'name': os.path.basename(file_path),
                        'extension': ext,
                        'size_bytes': size,
                        'size_readable': _format_size(size),
                        'supported': is_supported,
                        'type': _get_file_type(ext)
                    })

            # Sort by name
            files.sort(key=lambda x: x['name'].lower())

            # Filter to show supported files first
            supported_files = [f for f in files if f['supported']]
            other_files = [f for f in files if not f['supported']]

            return {
                'success': True,
                'directory': directory,
                'pattern': pattern,
                'total_files': len(files),
                'supported_files_count': len(supported_files),
                'supported_files': supported_files,
                'other_files': other_files if other_files else None
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _format_size(size_bytes: int) -> str:
        """Format file size in human-readable form"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"

    def _get_file_type(ext: str) -> str:
        """Get human-readable file type"""
        types = {
            '.csv': 'CSV',
            '.xlsx': 'Excel',
            '.xls': 'Excel (Legacy)',
            '.parquet': 'Parquet',
            '.json': 'JSON',
            '.txt': 'Text'
        }
        return types.get(ext, 'Unknown')

    schema = TOOL_SCHEMAS['01_list_files']
    registry.register(
        '01_list_files',
        list_files,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 01_read_file_preview
    def read_file_preview(file_path: str, rows: int = 10, include_schema: bool = True) -> Dict[str, Any]:
        """Preview first N rows of a data file (token-optimized)"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            df = read_data_file(file_path)
            # Limit preview rows for token efficiency
            effective_rows = min(rows, SAMPLE_ROW_LIMIT)
            preview_df = df.head(effective_rows)

            result = {
                'success': True,
                'file_path': file_path,
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'preview_rows': len(preview_df),
                'columns': df.columns,
                'data': truncate_row_data(preview_df.to_dicts())  # Truncate long strings
            }

            if include_schema:
                result['schema'] = [
                    {'name': col, 'type': str(df[col].dtype)}
                    for col in df.columns
                ]

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['01_read_file_preview']
    registry.register(
        '01_read_file_preview',
        read_file_preview,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 01_analyze_file
    def analyze_file(file_path: str, include_statistics: bool = True, include_patterns: bool = True) -> Dict[str, Any]:
        """Analyze a data file (CSV, Excel, or Parquet)"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            df = read_data_file(file_path)

            result = {
                'success': True,
                'file_path': file_path,
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': []
            }

            for col in df.columns:
                col_info = {
                    'name': col,
                    'dtype': str(df[col].dtype),
                    'null_count': df[col].null_count(),
                    'unique_count': df[col].n_unique()
                }

                if include_statistics and df[col].dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32]:
                    col_info['statistics'] = {
                        'min': df[col].min(),
                        'max': df[col].max(),
                        'mean': df[col].mean(),
                        'std': df[col].std()
                    }

                if include_patterns:
                    patterns = pattern_analyzer.analyze_column(df, col)
                    if patterns.get('patterns'):
                        col_info['detected_patterns'] = patterns['patterns']

                result['columns'].append(col_info)

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['01_analyze_file']
    registry.register(
        '01_analyze_file',
        analyze_file,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 01_detect_domain
    def detect_domain(file_path: str, confidence_threshold: float = 0.7) -> Dict[str, Any]:
        """Detect business domain (supports CSV, Excel, and Parquet)"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            df = read_data_file(file_path)

            result = domain_detector.detect(df)
            result['file_path'] = file_path

            # Filter by confidence
            if result.get('confidence', 0) < confidence_threshold:
                result['warning'] = f'Confidence below threshold ({confidence_threshold})'

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['01_detect_domain']
    registry.register(
        '01_detect_domain',
        detect_domain,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 01_find_relationships
    def find_relationships(file_paths: List[str], primary_file: Optional[str] = None) -> Dict[str, Any]:
        """Find relationships between files (supports CSV, Excel, and Parquet)"""
        try:
            dataframes = {}
            for path in file_paths:
                if not os.path.exists(path):
                    return {'success': False, 'error': f'File not found: {path}'}
                dataframes[path] = read_data_file(path)

            result = relationship_finder.find_relationships(dataframes, primary_file)
            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['01_find_relationships']
    registry.register(
        '01_find_relationships',
        find_relationships,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 01_profile_column
    def profile_column(file_path: str, column_name: str) -> Dict[str, Any]:
        """Profile a specific column (token-optimized output)"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            df = read_data_file(file_path)

            if column_name not in df.columns:
                return {'success': False, 'error': f'Column not found: {column_name}'}

            col = df[column_name]
            total_rows = len(col)

            profile = {
                'success': True,
                'column': column_name,
                'dtype': str(col.dtype),
                'rows': total_rows,
                'nulls': col.null_count(),
                'null_pct': round(col.null_count() / total_rows * 100, 1),
                'unique': col.n_unique(),
                'unique_pct': round(col.n_unique() / total_rows * 100, 1)
            }

            # Statistics for numeric columns (compact format)
            if col.dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8]:
                profile['stats'] = {
                    'min': col.min(),
                    'max': col.max(),
                    'mean': round(col.mean(), 2) if col.mean() else None,
                    'median': col.median(),
                    'std': round(col.std(), 2) if col.std() else None
                }

            # Value distribution (token-efficient using helper)
            profile['top_values'] = summarize_value_counts(col, TOP_VALUES_LIMIT)

            # Pattern analysis (limited output)
            patterns = pattern_analyzer.analyze_column(df, column_name)
            if patterns.get('patterns'):
                profile['patterns'] = patterns['patterns'][:5]  # Limit patterns

            return profile

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['01_profile_column']
    registry.register(
        '01_profile_column',
        profile_column,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 01_suggest_schema
    def suggest_schema(file_path: str, optimize_for: str = 'balanced') -> Dict[str, Any]:
        """Suggest optimal schema (supports CSV, Excel, and Parquet)"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            df = read_data_file(file_path)

            result = schema_inference.infer_schema(df)
            result['file_path'] = file_path
            result['optimization_target'] = optimize_for

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['01_suggest_schema']
    registry.register(
        '01_suggest_schema',
        suggest_schema,
        'discovery',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )
