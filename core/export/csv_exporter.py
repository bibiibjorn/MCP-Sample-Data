"""
CSV Exporter Module
Exports data to CSV format with various options
"""
import polars as pl
from typing import Dict, Any, Optional, List
import os
import logging

logger = logging.getLogger(__name__)


class CSVExporter:
    """Exports data to CSV format"""

    def __init__(self):
        pass

    def export(
        self,
        df: pl.DataFrame,
        output_path: str,
        delimiter: str = ',',
        include_header: bool = True,
        quote_style: str = 'necessary',
        date_format: Optional[str] = None,
        null_value: str = '',
        encoding: str = 'utf-8'
    ) -> Dict[str, Any]:
        """
        Export DataFrame to CSV.

        Args:
            df: DataFrame to export
            output_path: Output file path
            delimiter: Field delimiter
            include_header: Include column headers
            quote_style: Quoting style (necessary, always, never)
            date_format: Format for date columns
            null_value: String to use for null values
            encoding: Output encoding

        Returns:
            Export result
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Map quote style
            quote_map = {
                'necessary': 'necessary',
                'always': 'always',
                'never': 'never'
            }
            quote = quote_map.get(quote_style, 'necessary')

            df.write_csv(
                output_path,
                separator=delimiter,
                include_header=include_header,
                quote_style=quote,
                null_value=null_value,
                date_format=date_format
            )

            file_size = os.path.getsize(output_path)

            return {
                'success': True,
                'output_path': output_path,
                'row_count': len(df),
                'column_count': len(df.columns),
                'file_size_bytes': file_size,
                'file_size_mb': round(file_size / (1024 * 1024), 2)
            }

        except Exception as e:
            logger.error(f"Error exporting CSV: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def export_multiple(
        self,
        dataframes: Dict[str, pl.DataFrame],
        output_dir: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Export multiple DataFrames to CSV files"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            results = []

            for name, df in dataframes.items():
                output_path = os.path.join(output_dir, f"{name}.csv")
                result = self.export(df, output_path, **kwargs)
                results.append({
                    'name': name,
                    'path': output_path,
                    'success': result['success'],
                    'rows': len(df) if result['success'] else 0
                })

            return {
                'success': all(r['success'] for r in results),
                'output_dir': output_dir,
                'files_exported': results
            }

        except Exception as e:
            logger.error(f"Error exporting multiple CSVs: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def preview_export(
        self,
        df: pl.DataFrame,
        rows: int = 5,
        delimiter: str = ','
    ) -> Dict[str, Any]:
        """Preview what the CSV export would look like"""
        try:
            preview_df = df.head(rows)
            lines = []

            # Header
            lines.append(delimiter.join(preview_df.columns))

            # Data rows
            for row in preview_df.iter_rows():
                lines.append(delimiter.join(str(v) if v is not None else '' for v in row))

            return {
                'success': True,
                'preview': '\n'.join(lines),
                'total_rows': len(df),
                'preview_rows': rows
            }

        except Exception as e:
            logger.error(f"Error previewing CSV: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
