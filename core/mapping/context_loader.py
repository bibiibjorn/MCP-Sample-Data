"""
Context Loader Module
Loads multiple files as a unified context for analysis
"""
import polars as pl
from typing import Dict, Any, List, Optional
import logging
import os
import re

from core.config.config_manager import config

logger = logging.getLogger(__name__)

# Token optimization constants - loaded from config
DEFAULT_QUERY_LIMIT = config.get('editing.default_query_limit', 1000)
MAX_QUERY_LIMIT = config.get('editing.max_query_limit', 200000)
SAMPLE_PREVIEW_LIMIT = config.get('editing.sample_limit', 50)


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
        query: str,
        limit: Optional[int] = None,
        include_data: bool = True
    ) -> Dict[str, Any]:
        """
        Execute a SQL query on context files using DuckDB.

        Args:
            context_name: Name of the loaded context
            query: SQL query to execute
            limit: Maximum rows to return (default: 100, max: 1000)
            include_data: Whether to include row data in response (set False for counts only)

        Returns:
            Query results with automatic row limiting for token efficiency
        """
        import duckdb

        context = self.contexts.get(context_name)
        if not context:
            return {'success': False, 'error': f'Context not found: {context_name}'}

        try:
            conn = duckdb.connect(':memory:')

            # Register all files as tables
            for alias, info in context['files'].items():
                conn.register(alias, info['df'].to_pandas())

            # Determine effective limit
            if limit is not None:
                # User specified limit - cap at max if set
                effective_limit = min(limit, MAX_QUERY_LIMIT) if MAX_QUERY_LIMIT else limit
            else:
                # Use default
                effective_limit = DEFAULT_QUERY_LIMIT

            # Check if query already has LIMIT clause
            query_upper = query.upper().strip()
            has_limit = bool(re.search(r'\bLIMIT\s+\d+', query_upper))

            # Execute query to get full count first (for metadata)
            result = conn.execute(query).pl()
            total_rows = len(result)

            # Apply limit if not already in query
            if not has_limit and total_rows > effective_limit:
                result = result.head(effective_limit)
                was_truncated = True
            else:
                was_truncated = False

            response = {
                'success': True,
                'row_count': total_rows,
                'columns': result.columns,
                'returned_rows': len(result)
            }

            if was_truncated:
                response['truncated'] = True
                response['note'] = f'Results limited to {effective_limit} rows. Use LIMIT in query or set limit parameter for different size.'

            if include_data:
                # Truncate long string values for token efficiency
                data = result.to_dicts()
                response['data'] = self._truncate_row_strings(data)
            else:
                response['data_omitted'] = True
                response['note'] = 'Set include_data=True to see row data'

            return response

        except Exception as e:
            logger.error(f"Error querying context: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _truncate_row_strings(self, rows: List[Dict], max_len: int = 100) -> List[Dict]:
        """Truncate long string values in row data for token efficiency."""
        def truncate(val):
            if isinstance(val, str) and len(val) > max_len:
                return val[:max_len - 3] + '...'
            return val

        return [{k: truncate(v) for k, v in row.items()} for row in rows]

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
