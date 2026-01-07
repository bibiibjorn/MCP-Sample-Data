"""
Resource Manager Module
Manages MCP resources (files, contexts)
"""
from typing import Dict, Any, List, Optional
import os
import logging

logger = logging.getLogger(__name__)


class ResourceManager:
    """Manages MCP resources"""

    def __init__(self, projects_root: Optional[str] = None):
        if projects_root:
            self.projects_root = projects_root
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            self.projects_root = os.path.join(
                os.path.dirname(script_dir),
                'projects'
            )

        self._resources: Dict[str, Dict[str, Any]] = {}

    def register_resource(
        self,
        uri: str,
        name: str,
        description: str,
        mime_type: str = 'application/json'
    ) -> None:
        """Register a resource"""
        self._resources[uri] = {
            'uri': uri,
            'name': name,
            'description': description,
            'mimeType': mime_type
        }

    def list_resources(self) -> List[Dict[str, Any]]:
        """List all registered resources"""
        resources = list(self._resources.values())

        # Add dynamic project resources
        if os.path.exists(self.projects_root):
            for project_name in os.listdir(self.projects_root):
                project_path = os.path.join(self.projects_root, project_name)
                if os.path.isdir(project_path):
                    resources.append({
                        'uri': f'project://{project_name}',
                        'name': f'Project: {project_name}',
                        'description': f'Sample data project: {project_name}',
                        'mimeType': 'application/json'
                    })

        return resources

    def get_resource(self, uri: str) -> Optional[Dict[str, Any]]:
        """Get a resource by URI"""
        # Static resources
        if uri in self._resources:
            return self._resources[uri]

        # Dynamic project resources
        if uri.startswith('project://'):
            project_name = uri.replace('project://', '')
            return self._load_project_resource(project_name)

        # File resources
        if uri.startswith('file://'):
            file_path = uri.replace('file://', '')
            return self._load_file_resource(file_path)

        return None

    def _load_project_resource(self, project_name: str) -> Optional[Dict[str, Any]]:
        """Load a project as a resource"""
        import json

        project_path = os.path.join(self.projects_root, project_name)
        manifest_path = os.path.join(project_path, 'project.json')

        if not os.path.exists(manifest_path):
            return None

        try:
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)

            # List data files
            data_path = os.path.join(project_path, 'data')
            files = []
            if os.path.exists(data_path):
                for f in os.listdir(data_path):
                    file_path = os.path.join(data_path, f)
                    if os.path.isfile(file_path):
                        files.append({
                            'name': f,
                            'size_bytes': os.path.getsize(file_path)
                        })

            return {
                'uri': f'project://{project_name}',
                'name': project_name,
                'mimeType': 'application/json',
                'contents': {
                    'manifest': manifest,
                    'files': files
                }
            }

        except Exception as e:
            logger.error(f"Error loading project resource: {e}")
            return None

    def _load_file_resource(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Load a file as a resource"""
        import polars as pl

        if not os.path.exists(file_path):
            return None

        try:
            ext = os.path.splitext(file_path)[1].lower()

            if ext == '.csv':
                df = pl.read_csv(file_path, n_rows=100)
            elif ext == '.parquet':
                df = pl.read_parquet(file_path, n_rows=100)
            else:
                with open(file_path, 'r', encoding='utf-8') as f:
                    contents = f.read()

                return {
                    'uri': f'file://{file_path}',
                    'name': os.path.basename(file_path),
                    'mimeType': 'text/plain',
                    'contents': contents
                }

            return {
                'uri': f'file://{file_path}',
                'name': os.path.basename(file_path),
                'mimeType': 'application/json',
                'contents': {
                    'columns': df.columns,
                    'row_count': len(df),
                    'preview': df.to_dicts()
                }
            }

        except Exception as e:
            logger.error(f"Error loading file resource: {e}")
            return None

    def create_resource_template(self, template_name: str) -> Dict[str, Any]:
        """Get a resource creation template"""
        templates = {
            'dimension_table': {
                'description': 'Template for creating a dimension table',
                'schema': {
                    'columns': [
                        {'name': 'id', 'type': 'integer', 'role': 'key'},
                        {'name': 'name', 'type': 'string'},
                        {'name': 'description', 'type': 'string'}
                    ]
                }
            },
            'fact_table': {
                'description': 'Template for creating a fact table',
                'schema': {
                    'columns': [
                        {'name': 'date_key', 'type': 'integer', 'role': 'foreign_key'},
                        {'name': 'amount', 'type': 'float', 'role': 'measure'},
                        {'name': 'quantity', 'type': 'integer', 'role': 'measure'}
                    ]
                }
            },
            'date_dimension': {
                'description': 'Standard date dimension',
                'schema': {
                    'columns': [
                        {'name': 'date_key', 'type': 'integer', 'role': 'key'},
                        {'name': 'date', 'type': 'date'},
                        {'name': 'year', 'type': 'integer'},
                        {'name': 'month', 'type': 'integer'},
                        {'name': 'quarter', 'type': 'integer'}
                    ]
                }
            }
        }

        return templates.get(template_name, {'error': f'Unknown template: {template_name}'})
