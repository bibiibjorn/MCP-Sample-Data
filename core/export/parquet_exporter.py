"""
Parquet Exporter Module
Exports data to Parquet format optimized for Power BI
"""
import polars as pl
from typing import Dict, Any, Optional, List
import os
import logging

logger = logging.getLogger(__name__)


class ParquetExporter:
    """Exports data to Parquet format"""

    def __init__(self):
        pass

    def export(
        self,
        df: pl.DataFrame,
        output_path: str,
        compression: str = 'snappy',
        row_group_size: Optional[int] = None,
        statistics: bool = True
    ) -> Dict[str, Any]:
        """
        Export DataFrame to Parquet.

        Args:
            df: DataFrame to export
            output_path: Output file path
            compression: Compression algorithm (snappy, gzip, lz4, zstd, none)
            row_group_size: Rows per row group
            statistics: Include column statistics

        Returns:
            Export result
        """
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            df.write_parquet(
                output_path,
                compression=compression,
                row_group_size=row_group_size,
                statistics=statistics
            )

            file_size = os.path.getsize(output_path)

            return {
                'success': True,
                'output_path': output_path,
                'row_count': len(df),
                'column_count': len(df.columns),
                'compression': compression,
                'file_size_bytes': file_size,
                'file_size_mb': round(file_size / (1024 * 1024), 2)
            }

        except Exception as e:
            logger.error(f"Error exporting Parquet: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def export_multiple(
        self,
        dataframes: Dict[str, pl.DataFrame],
        output_dir: str,
        **kwargs
    ) -> Dict[str, Any]:
        """Export multiple DataFrames to Parquet files"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            results = []

            for name, df in dataframes.items():
                output_path = os.path.join(output_dir, f"{name}.parquet")
                result = self.export(df, output_path, **kwargs)
                results.append({
                    'name': name,
                    'path': output_path,
                    'success': result['success'],
                    'rows': len(df) if result['success'] else 0,
                    'size_mb': result.get('file_size_mb', 0)
                })

            total_size = sum(r.get('size_mb', 0) for r in results)

            return {
                'success': all(r['success'] for r in results),
                'output_dir': output_dir,
                'files_exported': results,
                'total_size_mb': round(total_size, 2)
            }

        except Exception as e:
            logger.error(f"Error exporting multiple Parquets: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def get_parquet_metadata(self, file_path: str) -> Dict[str, Any]:
        """Get metadata from a Parquet file"""
        try:
            import pyarrow.parquet as pq

            metadata = pq.read_metadata(file_path)

            return {
                'success': True,
                'num_rows': metadata.num_rows,
                'num_columns': metadata.num_columns,
                'num_row_groups': metadata.num_row_groups,
                'created_by': metadata.created_by,
                'format_version': str(metadata.format_version),
                'serialized_size': metadata.serialized_size,
                'schema': [
                    {
                        'name': metadata.schema[i].name,
                        'physical_type': str(metadata.schema[i].physical_type),
                        'logical_type': str(metadata.schema[i].logical_type)
                    }
                    for i in range(metadata.num_columns)
                ]
            }

        except Exception as e:
            logger.error(f"Error reading Parquet metadata: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def compare_compression(
        self,
        df: pl.DataFrame,
        output_dir: str
    ) -> Dict[str, Any]:
        """Compare different compression algorithms"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            compressions = ['snappy', 'gzip', 'lz4', 'zstd', 'uncompressed']
            results = []

            for comp in compressions:
                output_path = os.path.join(output_dir, f"test_{comp}.parquet")

                import time
                start = time.time()
                df.write_parquet(output_path, compression=comp if comp != 'uncompressed' else None)
                write_time = time.time() - start

                file_size = os.path.getsize(output_path)

                start = time.time()
                _ = pl.read_parquet(output_path)
                read_time = time.time() - start

                results.append({
                    'compression': comp,
                    'file_size_bytes': file_size,
                    'file_size_mb': round(file_size / (1024 * 1024), 2),
                    'write_time_sec': round(write_time, 3),
                    'read_time_sec': round(read_time, 3)
                })

                os.remove(output_path)

            # Sort by file size
            results.sort(key=lambda x: x['file_size_bytes'])

            return {
                'success': True,
                'row_count': len(df),
                'comparison': results,
                'recommended': results[0]['compression']
            }

        except Exception as e:
            logger.error(f"Error comparing compression: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
