"""
File Manager Module
Manages data files within projects
"""
import polars as pl
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import shutil

logger = logging.getLogger(__name__)


class FileManager:
    """Manages data files"""

    def __init__(self, projects_root: Optional[str] = None):
        if projects_root:
            self.projects_root = projects_root
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            self.projects_root = os.path.join(
                os.path.dirname(os.path.dirname(script_dir)),
                'projects'
            )

    def list_files(
        self,
        project_name: Optional[str] = None,
        path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        List files in a project or path.

        Args:
            project_name: Project name
            path: Direct path to list

        Returns:
            File listing result
        """
        try:
            if project_name:
                target_path = os.path.join(self.projects_root, project_name, 'data')
            elif path:
                target_path = path
            else:
                return {'success': False, 'error': 'Provide project_name or path'}

            if not os.path.exists(target_path):
                return {'success': False, 'error': f'Path not found: {target_path}'}

            files = []
            for f in os.listdir(target_path):
                file_path = os.path.join(target_path, f)
                if os.path.isfile(file_path):
                    ext = os.path.splitext(f)[1].lower()
                    files.append({
                        'name': f,
                        'path': file_path,
                        'extension': ext,
                        'size_bytes': os.path.getsize(file_path),
                        'size_mb': round(os.path.getsize(file_path) / (1024 * 1024), 2),
                        'modified_at': datetime.fromtimestamp(
                            os.path.getmtime(file_path)
                        ).isoformat()
                    })

            return {
                'success': True,
                'path': target_path,
                'files': files,
                'total_count': len(files)
            }

        except Exception as e:
            logger.error(f"Error listing files: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get detailed information about a file"""
        try:
            if not os.path.exists(file_path):
                return {'success': False, 'error': f'File not found: {file_path}'}

            ext = os.path.splitext(file_path)[1].lower()
            info = {
                'success': True,
                'name': os.path.basename(file_path),
                'path': file_path,
                'extension': ext,
                'size_bytes': os.path.getsize(file_path),
                'size_mb': round(os.path.getsize(file_path) / (1024 * 1024), 2),
                'modified_at': datetime.fromtimestamp(
                    os.path.getmtime(file_path)
                ).isoformat(),
                'created_at': datetime.fromtimestamp(
                    os.path.getctime(file_path)
                ).isoformat()
            }

            # Try to get schema info for supported formats
            if ext in ['.csv', '.parquet', '.xlsx', '.xls']:
                try:
                    if ext == '.csv':
                        df = pl.read_csv(file_path, n_rows=0)
                    elif ext == '.parquet':
                        df = pl.read_parquet(file_path, n_rows=0)
                    elif ext in ['.xlsx', '.xls']:
                        df = pl.read_excel(file_path)
                        df = df.head(0)

                    info['columns'] = df.columns
                    info['column_count'] = len(df.columns)
                    info['schema'] = [
                        {'name': col, 'dtype': str(df[col].dtype)}
                        for col in df.columns
                    ]

                    # Get row count
                    if ext == '.csv':
                        df_full = pl.scan_csv(file_path)
                        info['row_count'] = df_full.select(pl.count()).collect().item()
                    elif ext == '.parquet':
                        df_full = pl.scan_parquet(file_path)
                        info['row_count'] = df_full.select(pl.count()).collect().item()

                except Exception as e:
                    info['schema_error'] = str(e)

            return info

        except Exception as e:
            logger.error(f"Error getting file info: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def read_file(
        self,
        file_path: str,
        rows: Optional[int] = None,
        columns: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Read a file into a DataFrame"""
        try:
            if not os.path.exists(file_path):
                return {'success': False, 'error': f'File not found: {file_path}'}

            ext = os.path.splitext(file_path)[1].lower()

            if ext == '.csv':
                df = pl.read_csv(file_path, n_rows=rows)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path, n_rows=rows)
            elif ext in ['.xlsx', '.xls']:
                df = pl.read_excel(file_path)
                if rows:
                    df = df.head(rows)
            else:
                return {'success': False, 'error': f'Unsupported format: {ext}'}

            if columns:
                available = [c for c in columns if c in df.columns]
                df = df.select(available)

            return {
                'success': True,
                'df': df,
                'row_count': len(df),
                'column_count': len(df.columns),
                'columns': df.columns
            }

        except Exception as e:
            logger.error(f"Error reading file: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def write_file(
        self,
        df: pl.DataFrame,
        file_path: str,
        format: str = 'csv',
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """Write a DataFrame to a file"""
        try:
            if os.path.exists(file_path) and not overwrite:
                return {
                    'success': False,
                    'error': f'File exists. Set overwrite=True to replace: {file_path}'
                }

            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            if format == 'csv':
                df.write_csv(file_path)
            elif format == 'parquet':
                df.write_parquet(file_path)
            elif format == 'excel':
                df.write_excel(file_path)
            else:
                return {'success': False, 'error': f'Unsupported format: {format}'}

            return {
                'success': True,
                'path': file_path,
                'row_count': len(df),
                'column_count': len(df.columns),
                'size_bytes': os.path.getsize(file_path)
            }

        except Exception as e:
            logger.error(f"Error writing file: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def copy_file(
        self,
        source_path: str,
        dest_path: str,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """Copy a file"""
        try:
            if not os.path.exists(source_path):
                return {'success': False, 'error': f'Source not found: {source_path}'}

            if os.path.exists(dest_path) and not overwrite:
                return {
                    'success': False,
                    'error': f'Destination exists. Set overwrite=True: {dest_path}'
                }

            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.copy2(source_path, dest_path)

            return {
                'success': True,
                'source': source_path,
                'destination': dest_path,
                'size_bytes': os.path.getsize(dest_path)
            }

        except Exception as e:
            logger.error(f"Error copying file: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def delete_file(self, file_path: str, confirm: bool = False) -> Dict[str, Any]:
        """Delete a file"""
        try:
            if not confirm:
                return {
                    'success': False,
                    'error': 'Deletion not confirmed. Set confirm=True to delete.'
                }

            if not os.path.exists(file_path):
                return {'success': False, 'error': f'File not found: {file_path}'}

            os.remove(file_path)

            return {
                'success': True,
                'deleted': file_path
            }

        except Exception as e:
            logger.error(f"Error deleting file: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def preview_file(
        self,
        file_path: str,
        rows: int = 10
    ) -> Dict[str, Any]:
        """Preview file contents"""
        try:
            result = self.read_file(file_path, rows=rows)
            if not result['success']:
                return result

            df = result['df']

            return {
                'success': True,
                'file_path': file_path,
                'columns': df.columns,
                'dtypes': {col: str(df[col].dtype) for col in df.columns},
                'preview_rows': rows,
                'data': df.to_dicts()
            }

        except Exception as e:
            logger.error(f"Error previewing file: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
