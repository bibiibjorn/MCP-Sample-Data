"""
Editing Handlers
Handlers for data editing and transformation tools
"""
import polars as pl
from typing import Dict, Any, List, Optional
import os

from core.editing import DuckDBEngine, PolarsEngine, QueryBuilder
from server.tool_schemas import TOOL_SCHEMAS


def register_editing_handlers(registry):
    """Register all editing handlers"""

    duckdb_engine = DuckDBEngine()
    polars_engine = PolarsEngine()
    query_builder = QueryBuilder()

    # 03_query_data
    def query_data(
        file_path: str,
        query: str,
        output_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """Query data using SQL"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            result = duckdb_engine.query(file_path, query)

            if not result['success']:
                return result

            df = result['df']

            # Write output if specified
            if output_path:
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                ext = os.path.splitext(output_path)[1].lower()
                if ext == '.csv':
                    df.write_csv(output_path)
                elif ext == '.parquet':
                    df.write_parquet(output_path)

                return {
                    'success': True,
                    'output_path': output_path,
                    'row_count': len(df),
                    'columns': df.columns
                }

            return {
                'success': True,
                'row_count': len(df),
                'columns': df.columns,
                'data': df.head(100).to_dicts()
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['03_query_data']
    registry.register(
        '03_query_data',
        query_data,
        'editing',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 03_update_data
    def update_data(
        file_path: str,
        set_values: Dict[str, Any],
        where_conditions: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Update records in a file"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            result = duckdb_engine.update(
                file_path=file_path,
                set_values=set_values,
                where_conditions=where_conditions
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['03_update_data']
    registry.register(
        '03_update_data',
        update_data,
        'editing',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 03_delete_data
    def delete_data(
        file_path: str,
        where_conditions: Dict[str, Any],
        confirm: bool = False
    ) -> Dict[str, Any]:
        """Delete records from a file"""
        if not confirm:
            return {
                'success': False,
                'error': 'Deletion not confirmed. Set confirm=True to delete.'
            }

        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            result = duckdb_engine.delete(
                file_path=file_path,
                where_conditions=where_conditions
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['03_delete_data']
    registry.register(
        '03_delete_data',
        delete_data,
        'editing',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 03_transform_data
    def transform_data(
        file_path: str,
        transformations: List[Dict[str, Any]],
        output_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """Apply transformations to data"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            result = polars_engine.transform(
                file_path=file_path,
                transformations=transformations
            )

            if not result['success']:
                return result

            df = result['df']

            # Write output if specified
            target_path = output_path or file_path
            os.makedirs(os.path.dirname(target_path), exist_ok=True)

            ext = os.path.splitext(target_path)[1].lower()
            if ext == '.csv':
                df.write_csv(target_path)
            elif ext == '.parquet':
                df.write_parquet(target_path)

            return {
                'success': True,
                'output_path': target_path,
                'row_count': len(df),
                'columns': df.columns,
                'transformations_applied': len(transformations)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['03_transform_data']
    registry.register(
        '03_transform_data',
        transform_data,
        'editing',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 03_merge_files
    def merge_files(
        file_paths: List[str],
        output_path: str,
        merge_type: str = 'union',
        join_keys: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Merge multiple files"""
        try:
            # Validate files exist
            for path in file_paths:
                if not os.path.exists(path):
                    return {'success': False, 'error': f'File not found: {path}'}

            if merge_type == 'union':
                result = polars_engine.union_files(file_paths)
            elif merge_type == 'join':
                if not join_keys:
                    return {'success': False, 'error': 'join_keys required for join merge'}
                result = polars_engine.join_files(file_paths, join_keys)
            else:
                return {'success': False, 'error': f'Unknown merge type: {merge_type}'}

            if not result['success']:
                return result

            df = result['df']

            # Write output
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            ext = os.path.splitext(output_path)[1].lower()
            if ext == '.csv':
                df.write_csv(output_path)
            elif ext == '.parquet':
                df.write_parquet(output_path)

            return {
                'success': True,
                'output_path': output_path,
                'merge_type': merge_type,
                'source_files': len(file_paths),
                'row_count': len(df),
                'columns': df.columns
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['03_merge_files']
    registry.register(
        '03_merge_files',
        merge_files,
        'editing',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )
