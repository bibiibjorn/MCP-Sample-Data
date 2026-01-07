"""
Error Handler Module
Provides consistent error handling and formatting for the MCP server
"""
import traceback
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ErrorHandler:
    """Handles errors consistently across the server"""

    @staticmethod
    def handle_unexpected_error(tool_name: str, exception: Exception) -> Dict[str, Any]:
        """Handle an unexpected error and return a formatted response"""
        error_type = type(exception).__name__
        error_message = str(exception)

        # Log the full traceback
        logger.error(f"Error in tool {tool_name}: {error_type}: {error_message}", exc_info=True)

        return {
            'success': False,
            'error': {
                'type': error_type,
                'message': error_message,
                'tool': tool_name
            }
        }

    @staticmethod
    def handle_validation_error(message: str, details: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Handle a validation error"""
        result = {
            'success': False,
            'error': {
                'type': 'ValidationError',
                'message': message
            }
        }
        if details:
            result['error']['details'] = details
        return result

    @staticmethod
    def handle_file_not_found(path: str) -> Dict[str, Any]:
        """Handle file not found error"""
        return {
            'success': False,
            'error': {
                'type': 'FileNotFound',
                'message': f'File not found: {path}',
                'path': path
            }
        }

    @staticmethod
    def handle_unsupported_format(format_type: str, supported: list) -> Dict[str, Any]:
        """Handle unsupported format error"""
        return {
            'success': False,
            'error': {
                'type': 'UnsupportedFormat',
                'message': f'Unsupported format: {format_type}',
                'supported_formats': supported
            }
        }

    @staticmethod
    def handle_missing_parameter(param_name: str) -> Dict[str, Any]:
        """Handle missing required parameter"""
        return {
            'success': False,
            'error': {
                'type': 'MissingParameter',
                'message': f'Required parameter missing: {param_name}'
            }
        }

    @staticmethod
    def wrap_result(data: Any, success: bool = True, message: Optional[str] = None) -> Dict[str, Any]:
        """Wrap a result in a standard response format"""
        result = {'success': success}
        if isinstance(data, dict):
            result.update(data)
        else:
            result['data'] = data
        if message:
            result['message'] = message
        return result
