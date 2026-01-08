"""
Tool Schemas Module
Defines all tool schemas for MCP registration
"""

TOOL_SCHEMAS = {
    # ============ DISCOVERY TOOLS (01_) ============
    '01_list_files': {
        'description': 'FIRST STEP: List data files in a directory to discover what files are available. Returns file paths that can be used directly with other tools. No file copying needed - just get the path and use it.',
        'parameters': {
            'directory': {'type': 'string', 'description': 'Directory path to list files from'},
            'pattern': {'type': 'string', 'description': 'Optional glob pattern to filter files (e.g., "*.csv", "*.xlsx")', 'default': '*'},
            'recursive': {'type': 'boolean', 'description': 'Search subdirectories recursively', 'default': False}
        },
        'required': ['directory']
    },
    '01_read_file_preview': {
        'description': 'Preview first N rows of a data file. Use this to quickly see file contents without full analysis. Server reads the file directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to the file (from 01_list_files or user-provided)'},
            'rows': {'type': 'integer', 'description': 'Number of rows to preview', 'default': 10},
            'include_schema': {'type': 'boolean', 'description': 'Include column types', 'default': True}
        },
        'required': ['file_path']
    },
    '01_analyze_file': {
        'description': 'Analyze a data file to understand its structure, content, and patterns. Server reads files directly from path - NO FILE COPYING NEEDED. Just provide the file path.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file (supports .csv, .xlsx, .xls, .parquet)'},
            'include_statistics': {'type': 'boolean', 'description': 'Include statistical analysis', 'default': True},
            'include_patterns': {'type': 'boolean', 'description': 'Detect data patterns', 'default': True}
        },
        'required': ['file_path']
    },
    '01_detect_domain': {
        'description': 'Detect the business domain of a data file (financial, sales, HR, etc.). Server reads files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file (.csv, .xlsx, .xls, .parquet)'},
            'confidence_threshold': {'type': 'number', 'description': 'Minimum confidence threshold', 'default': 0.7}
        },
        'required': ['file_path']
    },
    '01_find_relationships': {
        'description': 'Find potential relationships between data files. Server reads all files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_paths': {'type': 'array', 'items': {'type': 'string'}, 'description': 'List of file paths (.csv, .xlsx, .xls, .parquet)'},
            'primary_file': {'type': 'string', 'description': 'Primary fact table file'}
        },
        'required': ['file_paths']
    },
    '01_profile_column': {
        'description': 'Get detailed profile of a specific column. Server reads files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file (.csv, .xlsx, .xls, .parquet)'},
            'column_name': {'type': 'string', 'description': 'Column to profile'}
        },
        'required': ['file_path', 'column_name']
    },
    '01_suggest_schema': {
        'description': 'Suggest optimal schema for Power BI import. Server reads files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file (.csv, .xlsx, .xls, .parquet)'},
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
        'description': 'Generate a fact table with referential integrity to dimensions. Includes both numeric measures AND categorical attributes (status, payment method, channel, etc.)',
        'parameters': {
            'fact_type': {'type': 'string', 'description': 'Type: sales (order_status, payment_method, sales_channel), finance (transaction_type, status, currency_code), inventory (stock_status, movement_type, storage_location), hr (attendance_status, pay_type, shift), transactions (transaction_status, transaction_type, payment_processor)'},
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
    '02_generate_time_series': {
        'description': 'Generate time series data with realistic patterns including seasonality, trends, and special events. Built-in patterns: retail_seasonal, financial_quarterly, manufacturing_shift, ecommerce_daily, healthcare_weekly, linear_growth, exponential_growth.',
        'parameters': {
            'pattern': {'type': 'string', 'description': 'Pattern name (retail_seasonal, financial_quarterly, etc.) or "custom" with pattern_config'},
            'start_date': {'type': 'string', 'description': 'Start date (YYYY-MM-DD)'},
            'end_date': {'type': 'string', 'description': 'End date (YYYY-MM-DD)'},
            'base_value': {'type': 'number', 'description': 'Base value around which to generate', 'default': 100},
            'row_count': {'type': 'integer', 'description': 'Number of rows (if less than date range, samples dates)'},
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'date_column': {'type': 'string', 'description': 'Name for date column', 'default': 'date'},
            'value_column': {'type': 'string', 'description': 'Name for value column', 'default': 'value'},
            'additional_columns': {'type': 'object', 'description': 'Additional columns to include from dimensions'},
            'pattern_config': {'type': 'object', 'description': 'Custom pattern configuration (monthly_weights, weekly_weights, special_events, etc.)'},
            'seed': {'type': 'integer', 'description': 'Random seed for reproducibility'}
        },
        'required': ['pattern', 'start_date', 'end_date', 'output_path']
    },
    '02_list_time_patterns': {
        'description': 'List available time series patterns with descriptions',
        'parameters': {
            'category': {'type': 'string', 'description': 'Filter by category (retail, financial, operational)'}
        },
        'required': []
    },
    '02_generate_correlated_fact': {
        'description': 'Generate fact table with correlated columns. Supports statistical correlations (e.g., quantity ↔ discount), categorical correlations (region → shipping cost), formulas (total = qty × price), and tiered relationships (quantity tiers → discount rates).',
        'parameters': {
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'row_count': {'type': 'integer', 'description': 'Number of rows to generate', 'default': 10000},
            'base_columns': {
                'type': 'object',
                'description': 'Base column definitions: {col_name: {type: "random"|"uniform"|"choice"|"sequence", ...params} or [options] or constant}'
            },
            'correlation_rules': {
                'type': 'array',
                'description': 'Correlation rules: [{name, type: "statistical"|"categorical"|"formula"|"tiered"|"conditional", source_columns, target_column, parameters}]'
            },
            'preset_patterns': {
                'type': 'array',
                'items': {'type': 'string'},
                'description': 'Apply predefined patterns: sales_quantity_discount, sales_total_calculation, region_shipping_cost, price_cost_correlation'
            },
            'dimension_files': {'type': 'object', 'description': 'Link to dimension files for FK columns'},
            'seed': {'type': 'integer', 'description': 'Random seed for reproducibility'}
        },
        'required': ['output_path']
    },
    '02_list_correlation_patterns': {
        'description': 'List available predefined correlation patterns',
        'parameters': {},
        'required': []
    },
    '02_generate_currency_dimension': {
        'description': 'Generate ISO 4217 currency dimension table with currency codes, names, symbols, and metadata.',
        'parameters': {
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'currencies': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Specific currency codes to include (e.g., ["USD", "EUR", "GBP"])'},
            'include_all': {'type': 'boolean', 'description': 'Include all 30+ available currencies', 'default': False}
        },
        'required': ['output_path']
    },
    '02_generate_exchange_rates': {
        'description': 'Generate exchange rate time series with realistic volatility using Geometric Brownian Motion (GBM). Supports daily, weekly, or monthly rates.',
        'parameters': {
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'base_currency': {'type': 'string', 'description': 'Base currency code (rates will be X per 1 base)', 'default': 'USD'},
            'target_currencies': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Target currency codes'},
            'start_date': {'type': 'string', 'description': 'Start date (YYYY-MM-DD)'},
            'end_date': {'type': 'string', 'description': 'End date (YYYY-MM-DD)'},
            'frequency': {'type': 'string', 'enum': ['daily', 'weekly', 'monthly'], 'default': 'daily'},
            'seed': {'type': 'integer', 'description': 'Random seed for reproducibility'}
        },
        'required': ['output_path', 'target_currencies', 'start_date', 'end_date']
    },
    '02_generate_multicurrency_fact': {
        'description': 'Generate fact table with multi-currency support. Each row has transaction currency/amount and converted reporting currency/amount with exchange rates.',
        'parameters': {
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'row_count': {'type': 'integer', 'description': 'Number of rows', 'default': 10000},
            'transaction_currencies': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Possible transaction currencies'},
            'reporting_currency': {'type': 'string', 'description': 'Reporting/functional currency', 'default': 'USD'},
            'start_date': {'type': 'string', 'description': 'Date range start (YYYY-MM-DD)'},
            'end_date': {'type': 'string', 'description': 'Date range end (YYYY-MM-DD)'},
            'amount_config': {'type': 'object', 'description': 'Amount generation config: {mean, std, min, max}'},
            'include_fx_details': {'type': 'boolean', 'description': 'Include FX rate details', 'default': True},
            'seed': {'type': 'integer', 'description': 'Random seed'}
        },
        'required': ['output_path', 'transaction_currencies', 'start_date', 'end_date']
    },
    '02_generate_industry_schema': {
        'description': 'Generate complete industry-specific star schema. Available industries: retail, healthcare, manufacturing, banking, insurance, telecom. Creates all dimension and fact tables.',
        'parameters': {
            'template': {'type': 'string', 'description': 'Industry template: retail, healthcare, manufacturing, banking, insurance, telecom'},
            'output_dir': {'type': 'string', 'description': 'Output directory for generated files'},
            'scale_factor': {'type': 'number', 'description': 'Multiply default row counts by this factor', 'default': 1.0},
            'seed': {'type': 'integer', 'description': 'Random seed for reproducibility'}
        },
        'required': ['template', 'output_dir']
    },
    '02_list_industry_templates': {
        'description': 'List available industry star schema templates with their dimensions and fact tables',
        'parameters': {},
        'required': []
    },

    # ============ EDITING TOOLS (03_) ============
    '03_query_data': {
        'description': 'Query data files using SQL syntax. Server reads files directly - NO FILE COPYING NEEDED. Use "SELECT * FROM data" syntax.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file (.csv, .xlsx, .xls, .parquet)'},
            'query': {'type': 'string', 'description': 'SQL query (use "data" as table name)'},
            'output_path': {'type': 'string', 'description': 'Optional output file path'}
        },
        'required': ['file_path', 'query']
    },
    '03_update_data': {
        'description': 'Update records in a data file. Server reads/writes files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file (.csv, .xlsx, .xls, .parquet)'},
            'set_values': {'type': 'object', 'description': 'Column-value pairs to set'},
            'where_conditions': {'type': 'object', 'description': 'Filter conditions'}
        },
        'required': ['file_path', 'set_values']
    },
    '03_delete_data': {
        'description': 'Delete records from a data file. Server reads/writes files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file (.csv, .xlsx, .xls, .parquet)'},
            'where_conditions': {'type': 'object', 'description': 'Filter conditions'},
            'confirm': {'type': 'boolean', 'description': 'Confirm deletion', 'default': False}
        },
        'required': ['file_path', 'where_conditions']
    },
    '03_transform_data': {
        'description': 'Apply transformations to data. Server reads/writes files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file (.csv, .xlsx, .xls, .parquet)'},
            'transformations': {'type': 'array', 'description': 'List of transformation operations'},
            'output_path': {'type': 'string', 'description': 'Output file path'}
        },
        'required': ['file_path', 'transformations']
    },
    '03_merge_files': {
        'description': 'Merge multiple data files. Server reads all files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_paths': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Files to merge (.csv, .xlsx, .xls, .parquet)'},
            'merge_type': {'type': 'string', 'enum': ['union', 'join'], 'default': 'union'},
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'join_keys': {'type': 'array', 'description': 'Keys for join (if merge_type=join)'}
        },
        'required': ['file_paths', 'output_path']
    },

    # ============ VALIDATION TOOLS (04_) ============
    '04_validate_data': {
        'description': 'Validate data against rules. Server reads files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file (.csv, .xlsx, .xls, .parquet)'},
            'rules': {'type': 'array', 'description': 'Validation rules to apply'}
        },
        'required': ['file_path', 'rules']
    },
    '04_check_referential_integrity': {
        'description': 'Check referential integrity between files. Server reads all files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'fact_file': {'type': 'string', 'description': 'Path to fact table (.csv, .xlsx, .xls, .parquet)'},
            'dimension_files': {'type': 'object', 'description': 'Map of dimension name to file path'},
            'key_mappings': {'type': 'object', 'description': 'Map of fact key to dimension key'}
        },
        'required': ['fact_file', 'dimension_files', 'key_mappings']
    },
    '04_validate_balance': {
        'description': 'Validate financial balances (debit=credit). Server reads files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file (.csv, .xlsx, .xls, .parquet)'},
            'debit_column': {'type': 'string', 'description': 'Debit column name'},
            'credit_column': {'type': 'string', 'description': 'Credit column name'},
            'group_by': {'type': 'array', 'description': 'Columns to group by'}
        },
        'required': ['file_path', 'debit_column', 'credit_column']
    },
    '04_detect_anomalies': {
        'description': 'Detect statistical anomalies in data. Server reads files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file (.csv, .xlsx, .xls, .parquet)'},
            'columns': {'type': 'array', 'description': 'Columns to analyze'},
            'method': {'type': 'string', 'enum': ['zscore', 'iqr', 'isolation_forest'], 'default': 'zscore'}
        },
        'required': ['file_path']
    },
    '04_score_data_quality': {
        'description': 'Calculate comprehensive data quality score across 6 dimensions: Completeness (25%), Validity (20%), Uniqueness (15%), Accuracy (15%), Consistency (15%), Timeliness (10%). Returns overall score (0-100), grade (A-F), and detailed breakdown.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file to score (.csv, .xlsx, .xls, .parquet)'},
            'dimensions': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Dimensions to score (default: all). Options: completeness, validity, uniqueness, accuracy, consistency, timeliness'},
            'rules': {'type': 'object', 'description': 'Custom validation rules per column (e.g., {"email": {"pattern": "^.+@.+$"}, "age": {"min": 0, "max": 150}})'},
            'date_column': {'type': 'string', 'description': 'Column for timeliness scoring (evaluates data freshness)'},
            'key_columns': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Columns that should be unique (e.g., primary keys)'},
            'rules_file': {'type': 'string', 'description': 'Path to YAML rules file (alternative to inline rules)'}
        },
        'required': ['file_path']
    },
    '04_compare_quality': {
        'description': 'Compare data quality scores between two files or versions. Shows overall delta, improved/degraded dimensions, and issue changes.',
        'parameters': {
            'file_path_a': {'type': 'string', 'description': 'First file (baseline)'},
            'file_path_b': {'type': 'string', 'description': 'Second file (comparison)'},
            'dimensions': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Dimensions to compare (default: all)'}
        },
        'required': ['file_path_a', 'file_path_b']
    },
    '04_generate_quality_report': {
        'description': 'Generate a detailed data quality report in Markdown, HTML, or JSON format. Includes overall score, dimension breakdown, issues, and recommendations.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file to analyze'},
            'output_path': {'type': 'string', 'description': 'Path for output report'},
            'format': {'type': 'string', 'enum': ['markdown', 'html', 'json'], 'default': 'markdown', 'description': 'Report format'},
            'include_recommendations': {'type': 'boolean', 'default': True, 'description': 'Include actionable recommendations'},
            'include_column_details': {'type': 'boolean', 'default': True, 'description': 'Include per-column quality scores'},
            'rules': {'type': 'object', 'description': 'Custom validation rules'},
            'dimensions': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Dimensions to include'}
        },
        'required': ['file_path', 'output_path']
    },

    # ============ EXPORT TOOLS (05_) ============
    '05_export_csv': {
        'description': 'Export data to CSV format. Server reads source files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Source file (.csv, .xlsx, .xls, .parquet)'},
            'output_path': {'type': 'string', 'description': 'Output CSV path'},
            'delimiter': {'type': 'string', 'description': 'Field delimiter', 'default': ','}
        },
        'required': ['file_path', 'output_path']
    },
    '05_export_parquet': {
        'description': 'Export data to Parquet format. Server reads source files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Source file (.csv, .xlsx, .xls, .parquet)'},
            'output_path': {'type': 'string', 'description': 'Output Parquet path'},
            'compression': {'type': 'string', 'enum': ['snappy', 'gzip', 'lz4', 'zstd'], 'default': 'snappy'}
        },
        'required': ['file_path', 'output_path']
    },
    '05_optimize_for_powerbi': {
        'description': 'Optimize data for Power BI import. Server reads source files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Source file (.csv, .xlsx, .xls, .parquet)'},
            'output_path': {'type': 'string', 'description': 'Output file path'},
            'table_type': {'type': 'string', 'enum': ['dimension', 'fact', 'bridge'], 'default': 'dimension'}
        },
        'required': ['file_path', 'output_path']
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
        'description': 'Discover mappings between files. Server reads all files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_paths': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Files to analyze (.csv, .xlsx, .xls, .parquet)'},
            'source_file': {'type': 'string', 'description': 'Primary source file'}
        },
        'required': ['file_paths', 'source_file']
    },
    '08_define_mapping': {
        'description': 'Define a mapping between files. Server reads files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'mapping_name': {'type': 'string', 'description': 'Mapping name'},
            'source_file': {'type': 'string', 'description': 'Source file path'},
            'source_column': {'type': 'string', 'description': 'Source column'},
            'target_file': {'type': 'string', 'description': 'Target file path'},
            'target_column': {'type': 'string', 'description': 'Target column'}
        },
        'required': ['mapping_name', 'source_file', 'source_column', 'target_file', 'target_column']
    },
    '08_validate_amounts': {
        'description': 'Validate amounts using user-defined rules and mappings. Supports equations (A = B + C), sum checks, difference checks, and ratio validations. Server reads files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'source_file': {'type': 'string', 'description': 'Source data file (.csv, .xlsx, .xls, .parquet)'},
            'amount_column': {'type': 'string', 'description': 'Column containing amounts to validate'},
            'group_column': {'type': 'string', 'description': 'Column to group amounts by (e.g., account, category, department)'},
            'validation_rule': {
                'type': 'object',
                'description': 'Validation rule. Types: "sum_equals" (left = sum of right), "difference_equals" (left - right = expected), "groups_balance" (all groups sum to zero), "ratio_in_range" (left/right within min/max)',
                'properties': {
                    'type': {'type': 'string', 'enum': ['sum_equals', 'difference_equals', 'groups_balance', 'ratio_in_range', 'custom_equation']},
                    'left': {'type': 'array', 'description': 'Left side group(s)'},
                    'right': {'type': 'array', 'description': 'Right side group(s)'},
                    'expected': {'type': 'number', 'description': 'Expected value (for difference_equals)'},
                    'min_ratio': {'type': 'number', 'description': 'Minimum ratio (for ratio_in_range)'},
                    'max_ratio': {'type': 'number', 'description': 'Maximum ratio (for ratio_in_range)'}
                }
            },
            'mapping_file': {'type': 'string', 'description': 'Optional mapping file to classify source values into groups'},
            'mapping_source_column': {'type': 'string', 'description': 'Column in mapping that matches source group_column'},
            'mapping_target_column': {'type': 'string', 'description': 'Column in mapping with target group names used in validation_rule'},
            'tolerance': {'type': 'number', 'description': 'Allowed difference for equality checks', 'default': 0.01}
        },
        'required': ['source_file', 'amount_column', 'group_column', 'validation_rule']
    },
    '08_load_context': {
        'description': 'Load multiple files as a unified context for SQL queries. Server reads all files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'files': {'type': 'array', 'description': 'List of file definitions (.csv, .xlsx, .xls, .parquet)'},
            'context_name': {'type': 'string', 'description': 'Name for this context'}
        },
        'required': ['files', 'context_name']
    },
    '08_query_context': {
        'description': 'Query a loaded context with SQL. Use after 08_load_context. Results are automatically limited for token efficiency.',
        'parameters': {
            'context_name': {'type': 'string', 'description': 'Context name'},
            'query': {'type': 'string', 'description': 'SQL query'},
            'limit': {'type': 'integer', 'description': 'Max rows to return (default: 100, max: 1000). Use LIMIT in query for precise control.'},
            'include_data': {'type': 'boolean', 'description': 'Include row data in response. Set false for counts only.', 'default': True}
        },
        'required': ['context_name', 'query']
    },
    '08_analyze_hierarchy': {
        'description': 'Analyze hierarchical structure in data. Server reads files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'File to analyze (.csv, .xlsx, .xls, .parquet)'},
            'parent_column': {'type': 'string', 'description': 'Parent column name'},
            'child_column': {'type': 'string', 'description': 'Child column name'}
        },
        'required': ['file_path']
    },
    '08_rollup_through_hierarchy': {
        'description': 'Aggregate data through a hierarchy. Server reads all files directly - NO FILE COPYING NEEDED.',
        'parameters': {
            'source_file': {'type': 'string', 'description': 'Source data file (.csv, .xlsx, .xls, .parquet)'},
            'formula_file': {'type': 'string', 'description': 'Formula/hierarchy file (.csv, .xlsx, .xls, .parquet)'},
            'amount_column': {'type': 'string', 'description': 'Amount column'},
            'target_rollup': {'type': 'string', 'description': 'Target total to calculate'}
        },
        'required': ['source_file', 'formula_file', 'amount_column', 'target_rollup']
    },

    # ============ PRIVACY TOOLS (09_) ============
    '09_detect_pii': {
        'description': 'Detect personally identifiable information (PII) in a data file. Scans column names and content for emails, phones, SSNs, credit cards, names, addresses, and 30+ other PII types. Returns risk score, sensitivity levels, and GDPR categories.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file to scan (.csv, .xlsx, .xls, .parquet)'},
            'columns': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Specific columns to scan (default: all)'},
            'deep_scan': {'type': 'boolean', 'description': 'Scan all rows instead of sampling (slower but thorough)', 'default': False},
            'sample_size': {'type': 'integer', 'description': 'Number of rows to sample for detection', 'default': 1000},
            'confidence_threshold': {'type': 'number', 'description': 'Minimum confidence to report (0.0-1.0)', 'default': 0.5}
        },
        'required': ['file_path']
    },
    '09_anonymize_file': {
        'description': 'Anonymize PII in a data file. Strategies: mask (j***@e***.com), hash (SHA-256, preserves joins), synthetic (fake data), generalize (25→"20-29"), redact ([REDACTED]), shuffle, noise. Auto-detects PII or use manual column config.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to source file (.csv, .xlsx, .xls, .parquet)'},
            'output_path': {'type': 'string', 'description': 'Path for anonymized output (default: adds _anonymized suffix)'},
            'columns': {'type': 'object', 'description': 'Manual column→strategy mapping (e.g., {"email": "mask", "ssn": "redact"})'},
            'auto_detect': {'type': 'boolean', 'description': 'Auto-detect PII and apply default strategies', 'default': True},
            'strategy_overrides': {'type': 'object', 'description': 'Override default strategies for auto-detected columns'},
            'preserve_nulls': {'type': 'boolean', 'description': 'Keep null values as null', 'default': True},
            'seed': {'type': 'integer', 'description': 'Random seed for reproducibility'}
        },
        'required': ['file_path']
    },
    '09_generate_anonymization_report': {
        'description': 'Generate a PII detection and compliance report. Includes risk assessment, GDPR categories, sensitivity levels, and anonymization recommendations.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to file to analyze (.csv, .xlsx, .xls, .parquet)'},
            'output_path': {'type': 'string', 'description': 'Path for report output (default: adds _pii_report suffix)'},
            'format': {'type': 'string', 'enum': ['markdown', 'html', 'json'], 'default': 'markdown', 'description': 'Report format'},
            'include_samples': {'type': 'boolean', 'description': 'Include sample PII values (use with caution)', 'default': False}
        },
        'required': ['file_path']
    },

    # ============ SUBSETTING TOOLS (10_) ============
    '10_create_subset': {
        'description': 'Create a representative subset of data. Strategies: random, stratified (preserve category distributions), time_window, referential (maintain FK integrity), top_n, systematic. Supports multi-table subsets for star schemas.',
        'parameters': {
            'file_path': {'type': 'string', 'description': 'Path to source file (.csv, .xlsx, .xls, .parquet)'},
            'output_path': {'type': 'string', 'description': 'Path for subset output (default: adds _subset suffix)'},
            'strategy': {'type': 'string', 'enum': ['random', 'stratified', 'time_window', 'referential', 'top_n', 'systematic'], 'default': 'random', 'description': 'Sampling strategy'},
            'target_rows': {'type': 'integer', 'description': 'Target number of rows (use this OR target_percentage)'},
            'target_percentage': {'type': 'number', 'description': 'Target percentage of rows (e.g., 10 for 10%)'},
            'stratify_columns': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Columns to stratify by (for stratified sampling)'},
            'time_column': {'type': 'string', 'description': 'Date column for time-based sampling'},
            'time_start': {'type': 'string', 'description': 'Start date (YYYY-MM-DD) for time window'},
            'time_end': {'type': 'string', 'description': 'End date (YYYY-MM-DD) for time window'},
            'related_files': {'type': 'object', 'description': 'Dict of {file_path: key_column} for referential sampling'},
            'key_column': {'type': 'string', 'description': 'Primary key column for referential integrity'},
            'preserve_proportions': {'type': 'boolean', 'description': 'Maintain category proportions', 'default': True},
            'seed': {'type': 'integer', 'description': 'Random seed for reproducibility'}
        },
        'required': ['file_path']
    },
    '10_analyze_subset': {
        'description': 'Analyze how well a subset represents the original data. Compares distributions, statistics, and category coverage. Returns similarity scores and recommendations.',
        'parameters': {
            'source_file': {'type': 'string', 'description': 'Path to original file'},
            'subset_file': {'type': 'string', 'description': 'Path to subset file'},
            'columns': {'type': 'array', 'items': {'type': 'string'}, 'description': 'Specific columns to compare (default: all)'},
            'detailed': {'type': 'boolean', 'description': 'Include detailed statistics and differences', 'default': True}
        },
        'required': ['source_file', 'subset_file']
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
