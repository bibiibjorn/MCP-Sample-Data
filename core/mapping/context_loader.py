"""
Context Loader Module
Loads multiple files as a unified context for analysis
"""
import polars as pl
from typing import Dict, Any, List, Optional
import logging
import os

logger = logging.getLogger(__name__)


class ContextLoader:
    """Loads and manages multi-file contexts"""

    def __init__(self):
        self.contexts: Dict[str, Dict[str, Any]] = {}

    def load_context(
        self,
        files: List[Dict[str, Any]],
        context_name: str
    ) -> Dict[str, Any]:
        """
        Load multiple files as a unified context.

        Args:
            files: List of file definitions with path, role, and alias
            context_name: Name for this context

        Returns:
            Context loading result
        """
        try:
            context = {
                'name': context_name,
                'files': {},
                'summary': {
                    'total_files': 0,
                    'total_rows': 0,
                    'roles': {}
                }
            }

            for file_def in files:
                path = file_def['path']
                role = file_def.get('role', 'data')
                alias = file_def.get('alias', os.path.splitext(os.path.basename(path))[0])

                # Load file
                df = self._load_file(path)

                context['files'][alias] = {
                    'path': path,
                    'role': role,
                    'df': df,
                    'row_count': len(df),
                    'columns': df.columns
                }

                context['summary']['total_files'] += 1
                context['summary']['total_rows'] += len(df)

                if role not in context['summary']['roles']:
                    context['summary']['roles'][role] = []
                context['summary']['roles'][role].append(alias)

            self.contexts[context_name] = context

            return {
                'success': True,
                'context_name': context_name,
                'files_loaded': [
                    {
                        'alias': alias,
                        'path': info['path'],
                        'role': info['role'],
                        'rows': info['row_count'],
                        'columns': len(info['columns'])
                    }
                    for alias, info in context['files'].items()
                ],
                'summary': context['summary']
            }

        except Exception as e:
            logger.error(f"Error loading context: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def get_context(self, context_name: str) -> Optional[Dict[str, Any]]:
        """Get a loaded context by name"""
        return self.contexts.get(context_name)

    def get_file(self, context_name: str, alias: str) -> Optional[pl.DataFrame]:
        """Get a specific file from a context"""
        context = self.contexts.get(context_name)
        if context and alias in context['files']:
            return context['files'][alias]['df']
        return None

    def get_files_by_role(
        self,
        context_name: str,
        role: str
    ) -> List[Dict[str, Any]]:
        """Get all files with a specific role from a context"""
        context = self.contexts.get(context_name)
        if not context:
            return []

        return [
            {'alias': alias, 'df': info['df'], 'path': info['path']}
            for alias, info in context['files'].items()
            if info['role'] == role
        ]

    def unload_context(self, context_name: str) -> Dict[str, Any]:
        """Unload a context to free memory"""
        if context_name in self.contexts:
            del self.contexts[context_name]
            return {'success': True, 'unloaded': context_name}
        return {'success': False, 'error': f'Context not found: {context_name}'}

    def list_contexts(self) -> List[Dict[str, Any]]:
        """List all loaded contexts"""
        return [
            {
                'name': name,
                'files': list(ctx['files'].keys()),
                'total_rows': ctx['summary']['total_rows']
            }
            for name, ctx in self.contexts.items()
        ]

    def query_context(
        self,
        context_name: str,
        query: str
    ) -> Dict[str, Any]:
        """Execute a SQL query on context files using DuckDB"""
        import duckdb

        context = self.contexts.get(context_name)
        if not context:
            return {'success': False, 'error': f'Context not found: {context_name}'}

        try:
            conn = duckdb.connect(':memory:')

            # Register all files as tables
            for alias, info in context['files'].items():
                conn.register(alias, info['df'].to_pandas())

            # Execute query
            result = conn.execute(query).pl()

            return {
                'success': True,
                'row_count': len(result),
                'columns': result.columns,
                'data': result.to_dicts()
            }

        except Exception as e:
            logger.error(f"Error querying context: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _load_file(self, path: str) -> pl.DataFrame:
        """Load a file into a DataFrame"""
        ext = os.path.splitext(path)[1].lower()

        if ext == '.csv':
            return pl.read_csv(path)
        elif ext == '.parquet':
            return pl.read_parquet(path)
        elif ext in ['.xlsx', '.xls']:
            return pl.read_excel(path)
        else:
            raise ValueError(f"Unsupported file format: {ext}")
