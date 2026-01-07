"""
Tool Schemas Module
Defines all tool schemas for MCP registration
"""

TOOL_SCHEMAS = {
    # ============ DISCOVERY TOOLS (01_) ============
    '01_analyze_file': {
        'description': 'Analyze a data file to understand its structure, content, and patterns',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the file to analyze'},
            'include_statistics': {'type': 'boolean', 'description': 'Include statistical analysis', 'default': True},
            'include_patterns': {'type': 'boolean', 'description': 'Detect data patterns', 'default': True}
        },
        'required': ['file_path']
    },
    '01_detect_domain': {
        'description': 'Detect the business domain of a data file (financial, sales, HR, etc.)',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the file'},
            'confidence_threshold': {'type': 'number', 'description': 'Minimum confidence threshold', 'default': 0.7}
        },
        'required': ['file_path']
    },
    '01_find_relationships': {
        'description': 'Find potential relationships between data files',
        'parameters': {
            'file_paths': {'type': 'array', 'items': {'type': 'string'}, 'description': 'List of file paths to analyze'},
            'primary_file': {'type': 'string', 'description': 'Primary fact table file'}
        },
        'required': ['file_paths']
    },
    '01_profile_column': {
        'description': 'Get detailed profile of a specific column',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the file'},
            'column_name': {'type': 'string', 'description': 'Column to profile'}
        },
        'required': ['file_path', 'column_name']
    },
    '01_suggest_schema': {
        'description': 'Suggest optimal schema for Power BI import',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the file'},
            'optimize_for': {'type': 'string', 'enum': ['performance', 'storage', 'balanced'], 'default': 'balanced'}
        },
        'required': ['file_path']
    },

    # ============ GENERATION TOOLS (02_) ============
    '02_generate_dimension': {
        'description': 'Generate a dimension table with realistic data',
        'parameters': {
            'dimension_type': {'type': 'string', 'description': 'Type: customer, product, geography, time, employee'},
            'row_count': {'type': 'integer', 'description': 'Number of rows to generate', 'default': 1000},
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'locale': {'type': 'string', 'description': 'Locale for data generation', 'default': 'en_US'}
        },
        'required': ['dimension_type', 'output_path']
    },
    '02_generate_fact': {
        'description': 'Generate a fact table with referential integrity to dimensions',
        'parameters': {
            'fact_type': {'type': 'string', 'description': 'Type: sales, inventory, finance, transactions'},
            'row_count': {'type': 'integer', 'description': 'Number of rows', 'default': 10000},
            'dimension_files': {'type': 'object', 'description': 'Map of dimension name to file path'},
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'date_range': {'type': 'object', 'description': 'Date range for fact records'}
        },
        'required': ['fact_type', 'dimension_files', 'output_path']
    },
    '02_generate_date_dimension': {
        'description': 'Generate a standard date dimension table',
        'parameters': {
            'start_date': {'type': 'string', 'description': 'Start date (YYYY-MM-DD)'},
            'end_date': {'type': 'string', 'description': 'End date (YYYY-MM-DD)'},
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'fiscal_year_start_month': {'type': 'integer', 'description': 'Fiscal year start month', 'default': 1},
            'include_holidays': {'type': 'boolean', 'description': 'Include holiday flags', 'default': False}
        },
        'required': ['start_date', 'end_date', 'output_path']
    },
    '02_generate_from_template': {
        'description': 'Generate data from a YAML template',
        'parameters': {
            'template_path': {'type': 'string', 'description': 'Path to YAML template'},
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'row_count': {'type': 'integer', 'description': 'Number of rows', 'default': 1000},
            'seed': {'type': 'integer', 'description': 'Random seed for reproducibility'}
        },
        'required': ['template_path', 'output_path']
    },
    '02_generate_star_schema': {
        'description': 'Generate a complete star schema with fact and dimension tables',
        'parameters': {
            'schema_name': {'type': 'string', 'description': 'Name for the schema'},
            'domain': {'type': 'string', 'description': 'Business domain', 'default': 'sales'},
            'output_dir': {'type': 'string', 'description': 'Output directory'},
            'fact_rows': {'type': 'integer', 'description': 'Rows in fact table', 'default': 100000}
        },
        'required': ['schema_name', 'output_dir']
    },

    # ============ EDITING TOOLS (03_) ============
    '03_query_data': {
        'description': 'Query data files using SQL syntax',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the data file'},
            'query': {'type': 'string', 'description': 'SQL query to execute'},
            'output_path': {'type': 'string', 'description': 'Optional output file path'}
        },
        'required': ['file_path', 'query']
    },
    '03_update_data': {
        'description': 'Update records in a data file',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the data file'},
            'set_values': {'type': 'object', 'description': 'Column-value pairs to set'},
            'where_conditions': {'type': 'object', 'description': 'Filter conditions'}
        },
        'required': ['file_path', 'set_values']
    },
    '03_delete_data': {
        'description': 'Delete records from a data file',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the data file'},
            'where_conditions': {'type': 'object', 'description': 'Filter conditions'},
            'confirm': {'type': 'boolean', 'description': 'Confirm deletion', 'default': False}
        },
        'required': ['file_path', 'where_conditions']
    },
    '03_transform_data': {
        'description': 'Apply transformations to data',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the data file'},
            'transformations': {'type': 'array', 'description': 'List of transformation operations'},
            'output_path': {'type': 'string', 'description': 'Output file path'}
        },
        'required': ['file_path', 'transformations']
    },
    '03_merge_files': {
        'description': 'Merge multiple data files',
        'parameters': {
            'file_paths': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Files to merge'},
            'merge_type': {'type': 'string', 'enum': ['union', 'join'], 'default': 'union'},
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'join_keys': {'type': 'array', 'description': 'Keys for join (if merge_type=join)'}
        },
        'required': ['file_paths', 'output_path']
    },

    # ============ VALIDATION TOOLS (04_) ============
    '04_validate_data': {
        'description': 'Validate data against rules',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the data file'},
            'rules': {'type': 'array', 'description': 'Validation rules to apply'}
        },
        'required': ['file_path', 'rules']
    },
    '04_check_referential_integrity': {
        'description': 'Check referential integrity between files',
        'parameters': {
            'fact_file': {'type': 'string', 'description': 'Path to fact table'},
            'dimension_files': {'type': 'object', 'description': 'Map of dimension name to file path'},
            'key_mappings': {'type': 'object', 'description': 'Map of fact key to dimension key'}
        },
        'required': ['fact_file', 'dimension_files', 'key_mappings']
    },
    '04_validate_balance': {
        'description': 'Validate financial balances (debit=credit)',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the file'},
            'debit_column': {'type': 'string', 'description': 'Debit column name'},
            'credit_column': {'type': 'string', 'description': 'Credit column name'},
            'group_by': {'type': 'array', 'description': 'Columns to group by'}
        },
        'required': ['file_path', 'debit_column', 'credit_column']
    },
    '04_detect_anomalies': {
        'description': 'Detect statistical anomalies in data',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the file'},
            'columns': {'type': 'array', 'description': 'Columns to analyze'},
            'method': {'type': 'string', 'enum': ['zscore', 'iqr', 'isolation_forest'], 'default': 'zscore'}
        },
        'required': ['file_path']
    },

    # ============ EXPORT TOOLS (05_) ============
    '05_export_csv': {
        'description': 'Export data to CSV format',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Source file path'},
            'output_path': {'type': 'string', 'description': 'Output CSV path'},
            'delimiter': {'type': 'string', 'description': 'Field delimiter', 'default': ','}
        },
        'required': ['file_path', 'output_path']
    },
    '05_export_parquet': {
        'description': 'Export data to Parquet format',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Source file path'},
            'output_path': {'type': 'string', 'description': 'Output Parquet path'},
            'compression': {'type': 'string', 'enum': ['snappy', 'gzip', 'lz4', 'zstd'], 'default': 'snappy'}
        },
        'required': ['file_path', 'output_path']
    },
    '05_optimize_for_powerbi': {
        'description': 'Optimize data for Power BI import',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Source file path'},
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'table_type': {'type': 'string', 'enum': ['dimension', 'fact', 'bridge'], 'default': 'dimension'}
        },
        'required': ['file_path', 'output_path']
    },

    # ============ PROJECT TOOLS (06_) ============
    '06_create_project': {
        'description': 'Create a new sample data project',
        'parameters': {
            'project_name': {'type': 'string', 'description': 'Project name'},
            'description': {'type': 'string', 'description': 'Project description'},
            'domain': {'type': 'string', 'description': 'Business domain', 'default': 'general'}
        },
        'required': ['project_name']
    },
    '06_list_projects': {
        'description': 'List all sample data projects',
        'parameters': {}
    },
    '06_get_project': {
        'description': 'Get details of a project',
        'parameters': {
            'project_name': {'type': 'string', 'description': 'Project name'}
        },
        'required': ['project_name']
    },
    '06_delete_project': {
        'description': 'Delete a project',
        'parameters': {
            'project_name': {'type': 'string', 'description': 'Project name'},
            'confirm': {'type': 'boolean', 'description': 'Confirm deletion', 'default': False}
        },
        'required': ['project_name', 'confirm']
    },

    # ============ HELP TOOLS (07_) ============
    '07_list_tools': {
        'description': 'List all available tools',
        'parameters': {
            'category': {'type': 'string', 'description': 'Filter by category'}
        }
    },
    '07_get_tool_help': {
        'description': 'Get detailed help for a tool',
        'parameters': {
            'tool_name': {'type': 'string', 'description': 'Tool name'}
        },
        'required': ['tool_name']
    },
    '07_get_domain_prompt': {
        'description': 'Get domain-specific guidance',
        'parameters': {
            'domain': {'type': 'string', 'description': 'Business domain'}
        },
        'required': ['domain']
    },

    # ============ MAPPING TOOLS (08_) ============
    '08_discover_mappings': {
        'description': 'Discover mappings between files',
        'parameters': {
            'file_paths': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Files to analyze'},
            'source_file': {'type': 'string', 'description': 'Primary source file'}
        },
        'required': ['file_paths', 'source_file']
    },
    '08_define_mapping': {
        'description': 'Define a mapping between files',
        'parameters': {
            'mapping_name': {'type': 'string', 'description': 'Mapping name'},
            'source_file': {'type': 'string', 'description': 'Source file path'},
            'source_column': {'type': 'string', 'description': 'Source column'},
            'target_file': {'type': 'string', 'description': 'Target file path'},
            'target_column': {'type': 'string', 'description': 'Target column'}
        },
        'required': ['mapping_name', 'source_file', 'source_column', 'target_file', 'target_column']
    },
    '08_validate_balance_sheet': {
        'description': 'Validate balance sheet equation (Assets = Liabilities + Equity)',
        'parameters': {
            'source_file': {'type': 'string', 'description': 'Source data file'},
            'mapping_file': {'type': 'string', 'description': 'Mapping file'},
            'amount_column': {'type': 'string', 'description': 'Amount column'},
            'category_column': {'type': 'string', 'description': 'Category column'}
        },
        'required': ['source_file', 'mapping_file', 'amount_column', 'category_column']
    },
    '08_load_context': {
        'description': 'Load multiple files as a unified context',
        'parameters': {
            'files': {'type': 'array', 'description': 'List of file definitions'},
            'context_name': {'type': 'string', 'description': 'Name for this context'}
        },
        'required': ['files', 'context_name']
    },
    '08_query_context': {
        'description': 'Query a loaded context with SQL',
        'parameters': {
            'context_name': {'type': 'string', 'description': 'Context name'},
            'query': {'type': 'string', 'description': 'SQL query'}
        },
        'required': ['context_name', 'query']
    },
    '08_analyze_hierarchy': {
        'description': 'Analyze hierarchical structure in data',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'File to analyze'},
            'parent_column': {'type': 'string', 'description': 'Parent column name'},
            'child_column': {'type': 'string', 'description': 'Child column name'}
        },
        'required': ['file_path']
    },
    '08_rollup_through_hierarchy': {
        'description': 'Aggregate data through a hierarchy',
        'parameters': {
            'source_file': {'type': 'string', 'description': 'Source data file'},
            'formula_file': {'type': 'string', 'description': 'Formula/hierarchy file'},
            'amount_column': {'type': 'string', 'description': 'Amount column'},
            'target_rollup': {'type': 'string', 'description': 'Target total to calculate'}
        },
        'required': ['source_file', 'formula_file', 'amount_column', 'target_rollup']
    }
}


def get_all_schemas():
    """Get all tool schemas in MCP format"""
    schemas = []
    for name, schema in TOOL_SCHEMAS.items():
        schemas.append({
            'name': name,
            'description': schema['description'],
            'inputSchema': {
                'type': 'object',
                'properties': schema['parameters'],
                'required': schema.get('required', [])
            }
        })
    return schemas


def get_schema(tool_name: str):
    """Get schema for a specific tool"""
    return TOOL_SCHEMAS.get(tool_name)
