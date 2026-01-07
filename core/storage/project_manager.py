"""
Project Manager Module
Manages sample data projects
"""
import json
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import shutil

logger = logging.getLogger(__name__)


class ProjectManager:
    """Manages sample data projects"""

    def __init__(self, projects_root: Optional[str] = None):
        if projects_root:
            self.projects_root = projects_root
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            self.projects_root = os.path.join(
                os.path.dirname(os.path.dirname(script_dir)),
                'projects'
            )

        os.makedirs(self.projects_root, exist_ok=True)

    def create_project(
        self,
        project_name: str,
        description: str = '',
        domain: str = 'general',
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a new project.

        Args:
            project_name: Unique project name
            description: Project description
            domain: Business domain (financial, sales, inventory, etc.)
            metadata: Additional metadata

        Returns:
            Project creation result
        """
        try:
            project_path = os.path.join(self.projects_root, project_name)

            if os.path.exists(project_path):
                return {'success': False, 'error': f'Project already exists: {project_name}'}

            # Create project structure
            os.makedirs(project_path)
            os.makedirs(os.path.join(project_path, 'data'))
            os.makedirs(os.path.join(project_path, 'mappings'))
            os.makedirs(os.path.join(project_path, 'exports'))
            os.makedirs(os.path.join(project_path, 'templates'))

            # Create project manifest
            manifest = {
                'name': project_name,
                'description': description,
                'domain': domain,
                'metadata': metadata or {},
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat(),
                'files': [],
                'version': '1.0.0'
            }

            manifest_path = os.path.join(project_path, 'project.json')
            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2)

            return {
                'success': True,
                'project_name': project_name,
                'project_path': project_path,
                'manifest': manifest
            }

        except Exception as e:
            logger.error(f"Error creating project: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def get_project(self, project_name: str) -> Dict[str, Any]:
        """Get project details"""
        try:
            project_path = os.path.join(self.projects_root, project_name)

            if not os.path.exists(project_path):
                return {'success': False, 'error': f'Project not found: {project_name}'}

            manifest_path = os.path.join(project_path, 'project.json')
            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)

            # Get file list
            data_path = os.path.join(project_path, 'data')
            files = []
            if os.path.exists(data_path):
                for f in os.listdir(data_path):
                    file_path = os.path.join(data_path, f)
                    if os.path.isfile(file_path):
                        files.append({
                            'name': f,
                            'path': file_path,
                            'size_bytes': os.path.getsize(file_path),
                            'modified_at': datetime.fromtimestamp(
                                os.path.getmtime(file_path)
                            ).isoformat()
                        })

            return {
                'success': True,
                'project_name': project_name,
                'project_path': project_path,
                'manifest': manifest,
                'files': files
            }

        except Exception as e:
            logger.error(f"Error getting project: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def list_projects(self) -> Dict[str, Any]:
        """List all projects"""
        try:
            projects = []

            for name in os.listdir(self.projects_root):
                project_path = os.path.join(self.projects_root, name)
                if os.path.isdir(project_path):
                    manifest_path = os.path.join(project_path, 'project.json')
                    if os.path.exists(manifest_path):
                        try:
                            with open(manifest_path, 'r', encoding='utf-8') as f:
                                manifest = json.load(f)
                            projects.append({
                                'name': name,
                                'description': manifest.get('description', ''),
                                'domain': manifest.get('domain', 'general'),
                                'created_at': manifest.get('created_at'),
                                'file_count': len(manifest.get('files', []))
                            })
                        except:
                            projects.append({
                                'name': name,
                                'description': 'Error reading manifest',
                                'domain': 'unknown'
                            })

            return {
                'success': True,
                'projects': projects,
                'total_count': len(projects)
            }

        except Exception as e:
            logger.error(f"Error listing projects: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def update_project(
        self,
        project_name: str,
        description: Optional[str] = None,
        domain: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Update project details"""
        try:
            project_path = os.path.join(self.projects_root, project_name)
            manifest_path = os.path.join(project_path, 'project.json')

            if not os.path.exists(manifest_path):
                return {'success': False, 'error': f'Project not found: {project_name}'}

            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)

            if description is not None:
                manifest['description'] = description
            if domain is not None:
                manifest['domain'] = domain
            if metadata is not None:
                manifest['metadata'].update(metadata)

            manifest['updated_at'] = datetime.now().isoformat()

            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2)

            return {
                'success': True,
                'project_name': project_name,
                'manifest': manifest
            }

        except Exception as e:
            logger.error(f"Error updating project: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def delete_project(self, project_name: str, confirm: bool = False) -> Dict[str, Any]:
        """Delete a project"""
        try:
            if not confirm:
                return {
                    'success': False,
                    'error': 'Deletion not confirmed. Set confirm=True to delete.'
                }

            project_path = os.path.join(self.projects_root, project_name)

            if not os.path.exists(project_path):
                return {'success': False, 'error': f'Project not found: {project_name}'}

            shutil.rmtree(project_path)

            return {
                'success': True,
                'deleted': project_name
            }

        except Exception as e:
            logger.error(f"Error deleting project: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def add_file_to_project(
        self,
        project_name: str,
        file_name: str,
        file_type: str = 'data',
        role: str = 'dimension'
    ) -> Dict[str, Any]:
        """Register a file in the project manifest"""
        try:
            project_path = os.path.join(self.projects_root, project_name)
            manifest_path = os.path.join(project_path, 'project.json')

            if not os.path.exists(manifest_path):
                return {'success': False, 'error': f'Project not found: {project_name}'}

            with open(manifest_path, 'r', encoding='utf-8') as f:
                manifest = json.load(f)

            # Check if file already registered
            existing = [f for f in manifest.get('files', []) if f['name'] == file_name]
            if existing:
                return {'success': False, 'error': f'File already registered: {file_name}'}

            manifest.setdefault('files', []).append({
                'name': file_name,
                'type': file_type,
                'role': role,
                'added_at': datetime.now().isoformat()
            })

            manifest['updated_at'] = datetime.now().isoformat()

            with open(manifest_path, 'w', encoding='utf-8') as f:
                json.dump(manifest, f, indent=2)

            return {
                'success': True,
                'project_name': project_name,
                'file_added': file_name
            }

        except Exception as e:
            logger.error(f"Error adding file to project: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}
