"""
Project Handlers
Handlers for project management tools
"""
from typing import Dict, Any, Optional

from core.storage import ProjectManager
from server.tool_schemas import TOOL_SCHEMAS


def register_project_handlers(registry):
    """Register all project handlers"""

    project_manager = ProjectManager()

    # 06_create_project
    def create_project(
        project_name: str,
        description: str = '',
        domain: str = 'general'
    ) -> Dict[str, Any]:
        """Create a new project"""
        try:
            result = project_manager.create_project(
                project_name=project_name,
                description=description,
                domain=domain
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['06_create_project']
    registry.register(
        '06_create_project',
        create_project,
        'project',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 06_list_projects
    def list_projects() -> Dict[str, Any]:
        """List all projects"""
        try:
            result = project_manager.list_projects()
            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['06_list_projects']
    registry.register(
        '06_list_projects',
        list_projects,
        'project',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 06_get_project
    def get_project(project_name: str) -> Dict[str, Any]:
        """Get project details"""
        try:
            result = project_manager.get_project(project_name)
            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['06_get_project']
    registry.register(
        '06_get_project',
        get_project,
        'project',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 06_delete_project
    def delete_project(project_name: str, confirm: bool = False) -> Dict[str, Any]:
        """Delete a project"""
        try:
            result = project_manager.delete_project(
                project_name=project_name,
                confirm=confirm
            )

            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['06_delete_project']
    registry.register(
        '06_delete_project',
        delete_project,
        'project',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )
