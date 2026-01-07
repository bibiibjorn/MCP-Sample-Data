"""Handler modules for MCP tools"""
from .discovery_handlers import register_discovery_handlers
from .generation_handlers import register_generation_handlers
from .editing_handlers import register_editing_handlers
from .validation_handlers import register_validation_handlers
from .mapping_handlers import register_mapping_handlers
from .export_handlers import register_export_handlers
from .project_handlers import register_project_handlers
from .help_handlers import register_help_handlers


def register_all_handlers(registry):
    """Register all handlers with the registry"""
    register_discovery_handlers(registry)
    register_generation_handlers(registry)
    register_editing_handlers(registry)
    register_validation_handlers(registry)
    register_mapping_handlers(registry)
    register_export_handlers(registry)
    register_project_handlers(registry)
    register_help_handlers(registry)


__all__ = [
    'register_all_handlers',
    'register_discovery_handlers',
    'register_generation_handlers',
    'register_editing_handlers',
    'register_validation_handlers',
    'register_mapping_handlers',
    'register_export_handlers',
    'register_project_handlers',
    'register_help_handlers'
]
