"""
Export Handlers
Handlers for data export tools
"""
import polars as pl
from typing import Dict, Any, Optional
import os

from core.export import CSVExporter, ParquetExporter, PowerBIOptimizer
from server.tool_schemas import TOOL_SCHEMAS


def register_export_handlers(registry):
    """Register all export handlers"""

    csv_exporter = CSVExporter()
    parquet_exporter = ParquetExporter()
    powerbi_optimizer = PowerBIOptimizer()

    # 05_export_csv
    def export_csv(
        file_path: str,
        output_path: str,
        delimiter: str = ','
    ) -> Dict[str, Any]:
        """Export to CSV format"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                df = pl.read_csv(file_path)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path)
            elif ext in ['.xlsx', '.xls']:
                df = pl.read_excel(file_path)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            result = csv_exporter.export(
                df=df,
                output_path=output_path,
                delimiter=delimiter
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['05_export_csv']
    registry.register(
        '05_export_csv',
        export_csv,
        'export',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 05_export_parquet
    def export_parquet(
        file_path: str,
        output_path: str,
        compression: str = 'snappy'
    ) -> Dict[str, Any]:
        """Export to Parquet format"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                df = pl.read_csv(file_path)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path)
            elif ext in ['.xlsx', '.xls']:
                df = pl.read_excel(file_path)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            result = parquet_exporter.export(
                df=df,
                output_path=output_path,
                compression=compression
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['05_export_parquet']
    registry.register(
        '05_export_parquet',
        export_parquet,
        'export',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 05_optimize_for_powerbi
    def optimize_for_powerbi(
        file_path: str,
        output_path: str,
        table_type: str = 'dimension'
    ) -> Dict[str, Any]:
        """Optimize data for Power BI"""
        if not os.path.exists(file_path):
            return {'success': False, 'error': f'File not found: {file_path}'}

        try:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.csv':
                df = pl.read_csv(file_path)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path)
            elif ext in ['.xlsx', '.xls']:
                df = pl.read_excel(file_path)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            # Optimize
            opt_result = powerbi_optimizer.optimize_for_powerbi(
                df=df,
                table_type=table_type
            )

            if not opt_result['success']:
                return opt_result

            optimized_df = opt_result['optimized_df']

            # Write output
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            ext = os.path.splitext(output_path)[1].lower()
            if ext == '.csv':
                optimized_df.write_csv(output_path)
            elif ext == '.parquet':
                optimized_df.write_parquet(output_path)
            else:
                optimized_df.write_parquet(output_path)

            return {
                'success': True,
                'output_path': output_path,
                'original_size_bytes': opt_result['original_size_bytes'],
                'optimized_size_bytes': opt_result['optimized_size_bytes'],
                'size_reduction_pct': opt_result['size_reduction_pct'],
                'optimizations_applied': opt_result['optimizations_applied'],
                'row_count': len(optimized_df),
                'columns': optimized_df.columns
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['05_optimize_for_powerbi']
    registry.register(
        '05_optimize_for_powerbi',
        optimize_for_powerbi,
        'export',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )
