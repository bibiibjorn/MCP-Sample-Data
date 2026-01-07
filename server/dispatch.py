"""
Tool Dispatcher Module
Dispatches tool calls to appropriate handlers
"""
from typing import Dict, Any, Optional
import logging
import traceback
from .registry import HandlerRegistry

logger = logging.getLogger(__name__)


class ToolDispatcher:
    """Dispatches tool calls to handlers"""

    def __init__(self, registry: HandlerRegistry):
        self.registry = registry

    async def dispatch(
        self,
        tool_name: str,
        arguments: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Dispatch a tool call.

        Args:
            tool_name: Name of the tool to call
            arguments: Tool arguments

        Returns:
            Tool execution result
        """
        try:
            # Validate tool exists
            handler = self.registry.get_handler(tool_name)
            if not handler:
                return {
                    'success': False,
                    'error': f'Unknown tool: {tool_name}',
                    'available_tools': [t['name'] for t in self.registry.list_tools()]
                }

            # Validate parameters
            validation = self.registry.validate_params(tool_name, arguments)
            if not validation['valid']:
                return {
                    'success': False,
                    'error': validation['error']
                }

            # Execute handler
            logger.info(f"Dispatching tool: {tool_name}")
            logger.debug(f"Arguments: {arguments}")

            result = await self._execute_handler(handler, arguments)

            logger.debug(f"Tool {tool_name} completed successfully")
            return result

        except Exception as e:
            logger.error(f"Error dispatching {tool_name}: {e}", exc_info=True)
            return {
                'success': False,
                'error': str(e),
                'traceback': traceback.format_exc()
            }

    async def _execute_handler(
        self,
        handler,
        arguments: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a handler, handling both sync and async"""
        import asyncio
        import inspect

        if inspect.iscoroutinefunction(handler):
            return await handler(**arguments)
        else:
            # Run sync handler in thread pool
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: handler(**arguments))

    def get_tool_help(self, tool_name: str) -> Optional[Dict[str, Any]]:
        """Get help information for a tool"""
        info = self.registry.get_tool_info(tool_name)
        if not info:
            return None

        return {
            'name': tool_name,
            'category': info['category'],
            'description': info['description'],
            'parameters': info['parameters'],
            'required_parameters': info['required'],
            'example': self._generate_example(info)
        }

    def _generate_example(self, info: Dict[str, Any]) -> Dict[str, Any]:
        """Generate an example call for a tool"""
        example = {}

        for param, schema in info['parameters'].items():
            param_type = schema.get('type', 'string')

            if param_type == 'string':
                example[param] = schema.get('default', 'example_value')
            elif param_type == 'integer':
                example[param] = schema.get('default', 100)
            elif param_type == 'number':
                example[param] = schema.get('default', 1.0)
            elif param_type == 'boolean':
                example[param] = schema.get('default', True)
            elif param_type == 'array':
                example[param] = []
            elif param_type == 'object':
                example[param] = {}

        return example

    def list_available_tools(self) -> Dict[str, Any]:
        """List all available tools with categorization"""
        categories = self.registry.list_categories()

        return {
            'total_tools': sum(c['tool_count'] for c in categories),
            'categories': categories
        }
