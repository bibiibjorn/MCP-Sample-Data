"""
Handler Registry Module
Central registry for all tool handlers
"""
from typing import Dict, Any, Callable, List, Optional
import logging

logger = logging.getLogger(__name__)


class HandlerRegistry:
    """Central registry for tool handlers"""

    def __init__(self):
        self._handlers: Dict[str, Dict[str, Any]] = {}
        self._categories: Dict[str, List[str]] = {}

    def register(
        self,
        tool_name: str,
        handler: Callable,
        category: str,
        description: str,
        parameters: Dict[str, Any],
        required_params: Optional[List[str]] = None
    ) -> None:
        """
        Register a tool handler.

        Args:
            tool_name: Unique tool name
            handler: Handler function
            category: Tool category (discovery, generation, etc.)
            description: Tool description
            parameters: Parameter schema
            required_params: List of required parameter names
        """
        self._handlers[tool_name] = {
            'handler': handler,
            'category': category,
            'description': description,
            'parameters': parameters,
            'required': required_params or []
        }

        if category not in self._categories:
            self._categories[category] = []

        if tool_name not in self._categories[category]:
            self._categories[category].append(tool_name)

        logger.debug(f"Registered handler: {tool_name} in category: {category}")

    def get_handler(self, tool_name: str) -> Optional[Callable]:
        """Get a handler by tool name"""
        entry = self._handlers.get(tool_name)
        return entry['handler'] if entry else None

    def get_tool_info(self, tool_name: str) -> Optional[Dict[str, Any]]:
        """Get tool information"""
        entry = self._handlers.get(tool_name)
        if not entry:
            return None

        return {
            'name': tool_name,
            'category': entry['category'],
            'description': entry['description'],
            'parameters': entry['parameters'],
            'required': entry['required']
        }

    def list_tools(self, category: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all registered tools"""
        tools = []

        for name, entry in self._handlers.items():
            if category and entry['category'] != category:
                continue

            tools.append({
                'name': name,
                'category': entry['category'],
                'description': entry['description']
            })

        return sorted(tools, key=lambda x: (x['category'], x['name']))

    def list_categories(self) -> List[Dict[str, Any]]:
        """List all categories with tool counts"""
        return [
            {
                'category': cat,
                'tool_count': len(tools),
                'tools': tools
            }
            for cat, tools in sorted(self._categories.items())
        ]

    def get_tool_schema(self, tool_name: str) -> Optional[Dict[str, Any]]:
        """Get MCP-compatible tool schema"""
        entry = self._handlers.get(tool_name)
        if not entry:
            return None

        return {
            'name': tool_name,
            'description': entry['description'],
            'inputSchema': {
                'type': 'object',
                'properties': entry['parameters'],
                'required': entry['required']
            }
        }

    def get_all_schemas(self) -> List[Dict[str, Any]]:
        """Get all tool schemas for MCP registration"""
        return [
            self.get_tool_schema(name)
            for name in self._handlers.keys()
        ]

    def validate_params(
        self,
        tool_name: str,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Validate parameters for a tool"""
        entry = self._handlers.get(tool_name)
        if not entry:
            return {'valid': False, 'error': f'Unknown tool: {tool_name}'}

        # Check required parameters
        missing = [p for p in entry['required'] if p not in params]
        if missing:
            return {
                'valid': False,
                'error': f'Missing required parameters: {missing}'
            }

        return {'valid': True}
