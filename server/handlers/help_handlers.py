"""
Help Handlers
Handlers for help and documentation tools
"""
from typing import Dict, Any, Optional
import os

from server.tool_schemas import TOOL_SCHEMAS


def register_help_handlers(registry):
    """Register all help handlers"""

    # Domain prompts location
    script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    domain_prompts_dir = os.path.join(script_dir, 'domain_prompts')

    # 07_list_tools
    def list_tools(category: Optional[str] = None) -> Dict[str, Any]:
        """List all available tools"""
        try:
            tools = registry.list_tools(category)

            # Group by category
            by_category = {}
            for tool in tools:
                cat = tool['category']
                if cat not in by_category:
                    by_category[cat] = []
                by_category[cat].append(tool)

            return {
                'success': True,
                'total_tools': len(tools),
                'categories': list(by_category.keys()),
                'tools_by_category': by_category
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['07_list_tools']
    registry.register(
        '07_list_tools',
        list_tools,
        'help',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 07_get_tool_help
    def get_tool_help(tool_name: str) -> Dict[str, Any]:
        """Get detailed help for a tool"""
        try:
            tool_info = registry.get_tool_info(tool_name)

            if not tool_info:
                return {
                    'success': False,
                    'error': f'Tool not found: {tool_name}',
                    'available_tools': [t['name'] for t in registry.list_tools()]
                }

            # Generate example
            example = {}
            for param, schema in tool_info['parameters'].items():
                param_type = schema.get('type', 'string')
                if param_type == 'string':
                    example[param] = schema.get('default', f'example_{param}')
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

            return {
                'success': True,
                'tool_name': tool_name,
                'category': tool_info['category'],
                'description': tool_info['description'],
                'parameters': tool_info['parameters'],
                'required_parameters': tool_info['required'],
                'example': example
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['07_get_tool_help']
    registry.register(
        '07_get_tool_help',
        get_tool_help,
        'help',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 07_get_domain_prompt
    def get_domain_prompt(domain: str) -> Dict[str, Any]:
        """Get domain-specific guidance"""
        try:
            # Domain prompt definitions
            domain_prompts = {
                'financial': {
                    'description': 'Financial reporting and analysis domain',
                    'typical_tables': [
                        'Chart of Accounts',
                        'General Ledger',
                        'Trial Balance',
                        'Journal Entries',
                        'Cost Centers'
                    ],
                    'key_measures': [
                        'Revenue',
                        'Expenses',
                        'Net Income',
                        'Assets',
                        'Liabilities',
                        'Equity'
                    ],
                    'validation_rules': [
                        'Balance sheet equation: Assets = Liabilities + Equity',
                        'Debit = Credit for journal entries',
                        'Account hierarchies sum correctly'
                    ],
                    'recommended_tools': [
                        '04_validate_balance',
                        '08_validate_balance_sheet',
                        '08_rollup_through_hierarchy'
                    ]
                },
                'sales': {
                    'description': 'Sales and revenue analysis domain',
                    'typical_tables': [
                        'Sales Transactions',
                        'Customers',
                        'Products',
                        'Sales Reps',
                        'Territories',
                        'Date Dimension'
                    ],
                    'key_measures': [
                        'Revenue',
                        'Units Sold',
                        'Average Order Value',
                        'Discount Amount',
                        'Profit Margin'
                    ],
                    'validation_rules': [
                        'Foreign key integrity to dimensions',
                        'No negative quantities (usually)',
                        'Order dates within valid range'
                    ],
                    'recommended_tools': [
                        '02_generate_star_schema',
                        '04_check_referential_integrity',
                        '01_find_relationships'
                    ]
                },
                'inventory': {
                    'description': 'Inventory management domain',
                    'typical_tables': [
                        'Products',
                        'Warehouses',
                        'Stock Levels',
                        'Stock Movements',
                        'Suppliers'
                    ],
                    'key_measures': [
                        'Quantity on Hand',
                        'Reorder Point',
                        'Safety Stock',
                        'Units Received',
                        'Units Shipped'
                    ],
                    'validation_rules': [
                        'Stock levels >= 0',
                        'Movements balance (in - out = current)',
                        'Product references exist'
                    ],
                    'recommended_tools': [
                        '04_validate_data',
                        '04_detect_anomalies',
                        '02_generate_dimension'
                    ]
                },
                'hr': {
                    'description': 'Human resources and workforce analytics domain',
                    'typical_tables': [
                        'Employees',
                        'Departments',
                        'Positions',
                        'Salary History',
                        'Performance Reviews'
                    ],
                    'key_measures': [
                        'Headcount',
                        'Average Salary',
                        'Turnover Rate',
                        'Tenure',
                        'Performance Score'
                    ],
                    'validation_rules': [
                        'Valid date ranges for employment',
                        'Department hierarchy integrity',
                        'Salary within range for position'
                    ],
                    'recommended_tools': [
                        '02_generate_dimension',
                        '08_analyze_hierarchy',
                        '04_detect_anomalies'
                    ]
                }
            }

            domain_lower = domain.lower()
            if domain_lower not in domain_prompts:
                return {
                    'success': False,
                    'error': f'Unknown domain: {domain}',
                    'available_domains': list(domain_prompts.keys())
                }

            prompt = domain_prompts[domain_lower]
            prompt['success'] = True
            prompt['domain'] = domain_lower

            return prompt

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['07_get_domain_prompt']
    registry.register(
        '07_get_domain_prompt',
        get_domain_prompt,
        'help',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )
