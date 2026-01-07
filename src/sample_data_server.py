"""
MCP Sample Data Server
Main entry point for the Sample Data MCP server
"""
import asyncio
import logging
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from server.registry import HandlerRegistry
from server.dispatch import ToolDispatcher
from server.resources import ResourceManager
from server.handlers import register_all_handlers
from server.tool_schemas import get_all_schemas

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_server() -> Server:
    """Create and configure the MCP server"""
    server = Server("mcp-sample-data")

    # Initialize components
    registry = HandlerRegistry()
    dispatcher = ToolDispatcher(registry)
    resources = ResourceManager()

    # Register all handlers
    register_all_handlers(registry)

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        """List all available tools"""
        schemas = get_all_schemas()
        return [
            Tool(
                name=schema['name'],
                description=schema['description'],
                inputSchema=schema['inputSchema']
            )
            for schema in schemas
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[TextContent]:
        """Handle tool calls"""
        logger.info(f"Tool call: {name}")
        logger.debug(f"Arguments: {arguments}")

        result = await dispatcher.dispatch(name, arguments or {})

        # Format result
        import json
        result_text = json.dumps(result, indent=2, default=str)

        return [TextContent(type="text", text=result_text)]

    @server.list_resources()
    async def list_resources():
        """List available resources"""
        return resources.list_resources()

    @server.read_resource()
    async def read_resource(uri: str):
        """Read a resource by URI"""
        resource = resources.get_resource(uri)
        if resource:
            import json
            return json.dumps(resource.get('contents', {}), indent=2, default=str)
        return None

    return server


async def main():
    """Main entry point"""
    logger.info("Starting MCP Sample Data Server")

    server = create_server()

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())
