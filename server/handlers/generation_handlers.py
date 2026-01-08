"""
Generation Handlers
Handlers for data generation tools
"""
import polars as pl
from typing import Dict, Any, List, Optional
import os
from datetime import datetime

from core.generation import DimensionGenerator, FactGenerator, DateDimensionGenerator, TemplateEngine
from core.generation.time_patterns import TimePatternEngine, TimePattern
from core.generation.correlation_engine import CorrelationEngine, CorrelationRule, get_correlation_pattern, list_correlation_patterns, CORRELATION_PATTERNS
from core.generation.currency_manager import (
    CurrencyDimensionGenerator, ExchangeRateGenerator, MultiCurrencyFactGenerator,
    ISO_CURRENCIES, BASE_RATES_VS_USD
)
from core.generation.industry_templates import IndustrySchemaGenerator, INDUSTRY_TEMPLATES
from server.tool_schemas import TOOL_SCHEMAS


def register_generation_handlers(registry):
    """Register all generation handlers"""

    dimension_gen = DimensionGenerator()
    fact_gen = FactGenerator()
    date_gen = DateDimensionGenerator()
    template_engine = TemplateEngine()

    # 02_generate_dimension
    def generate_dimension(
        dimension_type: str,
        output_path: str,
        row_count: int = 1000,
        locale: str = 'en_US'
    ) -> Dict[str, Any]:
        """Generate a dimension table"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            result = dimension_gen.generate(
                dimension_type=dimension_type,
                row_count=row_count,
                locale=locale
            )

            if not result['success']:
                return result

            df = result['df']

            # Write output
            ext = os.path.splitext(output_path)[1].lower()
            if ext == '.csv':
                df.write_csv(output_path)
            elif ext == '.parquet':
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)

            return {
                'success': True,
                'output_path': output_path,
                'dimension_type': dimension_type,
                'row_count': len(df),
                'columns': df.columns,
                'file_size_bytes': os.path.getsize(output_path)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_generate_dimension']
    registry.register(
        '02_generate_dimension',
        generate_dimension,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 02_generate_fact
    def generate_fact(
        fact_type: str,
        dimension_files: Dict[str, str],
        output_path: str,
        row_count: int = 10000,
        date_range: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Generate a fact table"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Load dimension DataFrames
            dimensions = {}
            for name, path in dimension_files.items():
                if not os.path.exists(path):
                    return {'success': False, 'error': f'Dimension file not found: {path}'}

                ext = os.path.splitext(path)[1].lower()
                if ext == '.csv':
                    dimensions[name] = pl.read_csv(path)
                elif ext == '.parquet':
                    dimensions[name] = pl.read_parquet(path)

            result = fact_gen.generate_from_type(
                fact_type=fact_type,
                dimensions=dimensions,
                row_count=row_count,
                date_range=date_range
            )

            if not result['success']:
                return result

            df = result['df']

            # Write output
            ext = os.path.splitext(output_path)[1].lower()
            if ext == '.csv':
                df.write_csv(output_path)
            elif ext == '.parquet':
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)

            return {
                'success': True,
                'output_path': output_path,
                'fact_type': fact_type,
                'row_count': len(df),
                'columns': df.columns,
                'file_size_bytes': os.path.getsize(output_path)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_generate_fact']
    registry.register(
        '02_generate_fact',
        generate_fact,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 02_generate_date_dimension
    def generate_date_dimension(
        start_date: str,
        end_date: str,
        output_path: str,
        fiscal_year_start_month: int = 1,
        include_holidays: bool = False
    ) -> Dict[str, Any]:
        """Generate a date dimension"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            result = date_gen.generate(
                start_date=start_date,
                end_date=end_date,
                fiscal_year_start_month=fiscal_year_start_month,
                include_holidays=include_holidays
            )

            if not result['success']:
                return result

            df = result['df']

            # Write output
            ext = os.path.splitext(output_path)[1].lower()
            if ext == '.csv':
                df.write_csv(output_path)
            elif ext == '.parquet':
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)

            return {
                'success': True,
                'output_path': output_path,
                'start_date': start_date,
                'end_date': end_date,
                'row_count': len(df),
                'columns': df.columns,
                'file_size_bytes': os.path.getsize(output_path)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_generate_date_dimension']
    registry.register(
        '02_generate_date_dimension',
        generate_date_dimension,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 02_generate_from_template
    def generate_from_template(
        template_path: str,
        output_path: str,
        row_count: int = 1000,
        seed: Optional[int] = None
    ) -> Dict[str, Any]:
        """Generate data from template"""
        try:
            if not os.path.exists(template_path):
                return {'success': False, 'error': f'Template not found: {template_path}'}

            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            result = template_engine.generate_from_template(
                template_path=template_path,
                row_count=row_count,
                seed=seed
            )

            if not result['success']:
                return result

            df = result['df']

            # Write output
            ext = os.path.splitext(output_path)[1].lower()
            if ext == '.csv':
                df.write_csv(output_path)
            elif ext == '.parquet':
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)

            return {
                'success': True,
                'output_path': output_path,
                'template_path': template_path,
                'row_count': len(df),
                'columns': df.columns,
                'file_size_bytes': os.path.getsize(output_path)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_generate_from_template']
    registry.register(
        '02_generate_from_template',
        generate_from_template,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 02_generate_star_schema
    def generate_star_schema(
        schema_name: str,
        output_dir: str,
        domain: str = 'sales',
        fact_rows: int = 100000
    ) -> Dict[str, Any]:
        """Generate a complete star schema"""
        try:
            os.makedirs(output_dir, exist_ok=True)

            generated_files = []

            # Generate dimensions based on domain
            dim_configs = {
                'sales': ['customer', 'product', 'geography', 'time'],
                'finance': ['account', 'department', 'time'],
                'inventory': ['product', 'warehouse', 'supplier', 'time'],
                'hr': ['employee', 'department', 'time']
            }

            dimensions = dim_configs.get(domain, ['customer', 'product', 'time'])

            dimension_files = {}
            for dim_type in dimensions:
                if dim_type == 'time':
                    output_path = os.path.join(output_dir, f'dim_{dim_type}.csv')
                    result = date_gen.generate(
                        start_date='2020-01-01',
                        end_date='2025-12-31'
                    )
                else:
                    output_path = os.path.join(output_dir, f'dim_{dim_type}.csv')
                    result = dimension_gen.generate(
                        dimension_type=dim_type,
                        row_count=1000
                    )

                if result['success']:
                    result['df'].write_csv(output_path)
                    dimension_files[dim_type] = output_path
                    generated_files.append({
                        'name': f'dim_{dim_type}',
                        'path': output_path,
                        'rows': len(result['df'])
                    })

            # Generate fact table
            fact_path = os.path.join(output_dir, f'fact_{domain}.csv')
            fact_type = domain if domain in ['sales', 'finance', 'inventory'] else 'sales'

            fact_result = fact_gen.generate_from_type(
                fact_type=fact_type,
                dimensions={k: pl.read_csv(v) for k, v in dimension_files.items()},
                row_count=fact_rows
            )

            if fact_result['success']:
                fact_result['df'].write_csv(fact_path)
                generated_files.append({
                    'name': f'fact_{domain}',
                    'path': fact_path,
                    'rows': len(fact_result['df'])
                })

            return {
                'success': True,
                'schema_name': schema_name,
                'domain': domain,
                'output_dir': output_dir,
                'files_generated': generated_files,
                'total_files': len(generated_files)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_generate_star_schema']
    registry.register(
        '02_generate_star_schema',
        generate_star_schema,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # Initialize time pattern engine
    config_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', 'time_patterns')
    time_engine = TimePatternEngine(patterns_dir=config_dir)

    # 02_generate_time_series
    def generate_time_series(
        pattern: str,
        start_date: str,
        end_date: str,
        output_path: str,
        base_value: float = 100.0,
        row_count: Optional[int] = None,
        date_column: str = 'date',
        value_column: str = 'value',
        additional_columns: Optional[Dict[str, Any]] = None,
        pattern_config: Optional[Dict[str, Any]] = None,
        seed: Optional[int] = None
    ) -> Dict[str, Any]:
        """Generate time series data with realistic patterns"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Handle custom pattern configuration
            if pattern == 'custom' and pattern_config:
                pattern_obj = TimePattern.from_dict(pattern_config)
            else:
                pattern_obj = pattern

            # Generate time series
            df = time_engine.generate_dataframe(
                pattern=pattern_obj,
                start_date=start_date,
                end_date=end_date,
                base_value=base_value,
                row_count=row_count,
                date_column=date_column,
                value_column=value_column,
                seed=seed
            )

            # Add additional columns if specified
            if additional_columns:
                for col_name, col_config in additional_columns.items():
                    if isinstance(col_config, list):
                        # Random selection from list
                        import random
                        if seed:
                            random.seed(seed + hash(col_name))
                        values = [random.choice(col_config) for _ in range(len(df))]
                        df = df.with_columns(pl.Series(name=col_name, values=values))
                    elif isinstance(col_config, dict):
                        # Config with type and parameters
                        col_type = col_config.get('type', 'choice')
                        if col_type == 'choice':
                            import random
                            if seed:
                                random.seed(seed + hash(col_name))
                            options = col_config.get('options', ['A', 'B', 'C'])
                            weights = col_config.get('weights')
                            if weights:
                                values = random.choices(options, weights=weights, k=len(df))
                            else:
                                values = [random.choice(options) for _ in range(len(df))]
                            df = df.with_columns(pl.Series(name=col_name, values=values))
                        elif col_type == 'sequence':
                            start = col_config.get('start', 1)
                            values = list(range(start, start + len(df)))
                            df = df.with_columns(pl.Series(name=col_name, values=values))
                        elif col_type == 'constant':
                            value = col_config.get('value', '')
                            df = df.with_columns(pl.lit(value).alias(col_name))

            # Write output
            ext = os.path.splitext(output_path)[1].lower()
            if ext == '.csv':
                df.write_csv(output_path)
            elif ext == '.parquet':
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)

            # Get pattern info for response
            if isinstance(pattern_obj, str):
                pattern_info = time_engine.patterns.get(pattern_obj)
                pattern_name = pattern_obj
                pattern_desc = pattern_info.description if pattern_info else ''
            else:
                pattern_name = 'custom'
                pattern_desc = pattern_obj.description

            # Calculate value statistics
            values = df[value_column].to_list()
            value_stats = {
                'min': min(values),
                'max': max(values),
                'mean': sum(values) / len(values),
                'total': sum(values)
            }

            return {
                'success': True,
                'output_path': output_path,
                'pattern': pattern_name,
                'pattern_description': pattern_desc,
                'date_range': {'start': start_date, 'end': end_date},
                'row_count': len(df),
                'columns': df.columns,
                'value_statistics': value_stats,
                'file_size_bytes': os.path.getsize(output_path)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_generate_time_series']
    registry.register(
        '02_generate_time_series',
        generate_time_series,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 02_list_time_patterns
    def list_time_patterns(
        category: Optional[str] = None
    ) -> Dict[str, Any]:
        """List available time series patterns"""
        try:
            patterns = time_engine.list_patterns()

            # Filter by category if specified
            if category:
                category = category.lower()
                category_keywords = {
                    'retail': ['retail', 'ecommerce', 'grocery', 'luxury'],
                    'financial': ['financial', 'banking', 'investment', 'insurance'],
                    'operational': ['manufacturing', 'logistics', 'healthcare', 'call_center', 'energy', 'website']
                }

                keywords = category_keywords.get(category, [category])
                patterns = [
                    p for p in patterns
                    if any(kw in p['name'].lower() or kw in p['description'].lower() for kw in keywords)
                ]

            # Group by category
            categorized = {
                'retail': [],
                'financial': [],
                'operational': [],
                'growth': [],
                'custom': []
            }

            for p in patterns:
                name = p['name'].lower()
                if any(kw in name for kw in ['retail', 'ecommerce', 'grocery', 'luxury']):
                    categorized['retail'].append(p)
                elif any(kw in name for kw in ['financial', 'banking', 'investment', 'insurance']):
                    categorized['financial'].append(p)
                elif any(kw in name for kw in ['growth', 'linear', 'exponential', 'logarithmic']):
                    categorized['growth'].append(p)
                elif any(kw in name for kw in ['manufacturing', 'logistics', 'healthcare', 'call', 'energy', 'website']):
                    categorized['operational'].append(p)
                else:
                    categorized['custom'].append(p)

            # Remove empty categories
            categorized = {k: v for k, v in categorized.items() if v}

            return {
                'success': True,
                'total_patterns': len(patterns),
                'patterns_by_category': categorized,
                'all_patterns': patterns
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_list_time_patterns']
    registry.register(
        '02_list_time_patterns',
        list_time_patterns,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # Initialize correlation engine
    correlation_engine = CorrelationEngine()

    # 02_generate_correlated_fact
    def generate_correlated_fact(
        output_path: str,
        row_count: int = 10000,
        base_columns: Optional[Dict[str, Any]] = None,
        correlation_rules: Optional[List[Dict[str, Any]]] = None,
        preset_patterns: Optional[List[str]] = None,
        dimension_files: Optional[Dict[str, str]] = None,
        seed: Optional[int] = None
    ) -> Dict[str, Any]:
        """Generate fact table with correlated columns"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            # Default base columns if not provided
            if base_columns is None:
                base_columns = {
                    'transaction_id': {'type': 'sequence', 'start': 1},
                    'quantity': {'type': 'uniform', 'min': 1, 'max': 100},
                    'unit_price': {'type': 'random', 'mean': 50, 'std': 25},
                    'region': {'type': 'choice', 'options': ['North', 'South', 'East', 'West']},
                    'customer_segment': {'type': 'choice', 'options': ['Consumer', 'Corporate', 'Home Office']}
                }

            # Build correlation rules from presets and custom rules
            all_rules = []

            # Add preset patterns
            if preset_patterns:
                for pattern_name in preset_patterns:
                    pattern = get_correlation_pattern(pattern_name)
                    if pattern:
                        all_rules.append(pattern)

            # Add custom rules
            if correlation_rules:
                all_rules.extend(correlation_rules)

            # Generate base data
            import random
            import numpy as np
            if seed is not None:
                random.seed(seed)
                np.random.seed(seed)

            # Load dimension data if provided
            if dimension_files:
                for dim_name, dim_path in dimension_files.items():
                    if os.path.exists(dim_path):
                        ext = os.path.splitext(dim_path)[1].lower()
                        if ext == '.csv':
                            dim_df = pl.read_csv(dim_path)
                        elif ext == '.parquet':
                            dim_df = pl.read_parquet(dim_path)
                        else:
                            continue

                        # Add FK column to base_columns
                        key_col = f'{dim_name}_id' if not dim_name.endswith('_id') else dim_name
                        if key_col not in base_columns:
                            # Get available IDs from dimension
                            id_col = dim_df.columns[0]  # Assume first column is ID
                            available_ids = dim_df[id_col].to_list()
                            base_columns[key_col] = {'type': 'choice', 'options': available_ids}

            # Generate the correlated fact table
            df = correlation_engine.generate_correlated_fact(
                base_columns=base_columns,
                correlation_rules=all_rules,
                row_count=row_count,
                seed=seed
            )

            # Write output
            ext = os.path.splitext(output_path)[1].lower()
            if ext == '.csv':
                df.write_csv(output_path)
            elif ext == '.parquet':
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)

            # Get column statistics
            column_stats = {}
            for col in df.columns:
                dtype = str(df[col].dtype)
                if 'Int' in dtype or 'Float' in dtype:
                    column_stats[col] = {
                        'type': 'numeric',
                        'min': df[col].min(),
                        'max': df[col].max(),
                        'mean': round(df[col].mean(), 2) if df[col].mean() else None
                    }
                else:
                    unique = df[col].n_unique()
                    column_stats[col] = {
                        'type': 'categorical',
                        'unique_values': unique,
                        'sample_values': df[col].unique().to_list()[:5]
                    }

            return {
                'success': True,
                'output_path': output_path,
                'row_count': len(df),
                'columns': df.columns,
                'correlation_rules_applied': len(all_rules),
                'preset_patterns_used': preset_patterns or [],
                'column_statistics': column_stats,
                'file_size_bytes': os.path.getsize(output_path)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_generate_correlated_fact']
    registry.register(
        '02_generate_correlated_fact',
        generate_correlated_fact,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 02_list_correlation_patterns
    def list_correlation_patterns_handler() -> Dict[str, Any]:
        """List available correlation patterns"""
        try:
            patterns = list_correlation_patterns()

            # Add detailed info
            detailed_patterns = []
            for pattern in patterns:
                name = pattern['name']
                full_pattern = CORRELATION_PATTERNS.get(name, {})
                detailed_patterns.append({
                    'name': name,
                    'description': pattern['description'],
                    'type': full_pattern.get('type', 'unknown'),
                    'source_columns': full_pattern.get('source_columns', []),
                    'target_column': full_pattern.get('target_column', '')
                })

            return {
                'success': True,
                'patterns': detailed_patterns,
                'total_count': len(patterns),
                'usage_example': {
                    'tool': '02_generate_correlated_fact',
                    'preset_patterns': ['sales_quantity_discount', 'sales_total_calculation']
                }
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_list_correlation_patterns']
    registry.register(
        '02_list_correlation_patterns',
        list_correlation_patterns_handler,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # Initialize currency generators
    currency_dim_gen = CurrencyDimensionGenerator()

    # 02_generate_currency_dimension
    def generate_currency_dimension(
        output_path: str,
        currencies: Optional[List[str]] = None,
        include_all: bool = False
    ) -> Dict[str, Any]:
        """Generate ISO 4217 currency dimension table"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            df = currency_dim_gen.generate(currencies=currencies, include_all=include_all)

            # Write output
            ext = os.path.splitext(output_path)[1].lower()
            if ext == '.csv':
                df.write_csv(output_path)
            elif ext == '.parquet':
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)

            return {
                'success': True,
                'output_path': output_path,
                'row_count': len(df),
                'columns': df.columns,
                'currencies_included': df['currency_code'].to_list(),
                'file_size_bytes': os.path.getsize(output_path)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_generate_currency_dimension']
    registry.register(
        '02_generate_currency_dimension',
        generate_currency_dimension,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 02_generate_exchange_rates
    def generate_exchange_rates(
        output_path: str,
        target_currencies: List[str],
        start_date: str,
        end_date: str,
        base_currency: str = 'USD',
        frequency: str = 'daily',
        seed: Optional[int] = None
    ) -> Dict[str, Any]:
        """Generate exchange rate time series"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            rate_gen = ExchangeRateGenerator(seed=seed)
            df = rate_gen.generate_rates(
                base_currency=base_currency,
                target_currencies=target_currencies,
                start_date=start_date,
                end_date=end_date,
                frequency=frequency
            )

            # Write output
            ext = os.path.splitext(output_path)[1].lower()
            if ext == '.csv':
                df.write_csv(output_path)
            elif ext == '.parquet':
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)

            # Get rate statistics
            rate_stats = {}
            for col in df.columns:
                if col != 'date' and '_' in col:
                    rate_stats[col] = {
                        'first': df[col][0],
                        'last': df[col][-1],
                        'min': round(df[col].min(), 4),
                        'max': round(df[col].max(), 4),
                        'change_pct': round((df[col][-1] / df[col][0] - 1) * 100, 2) if df[col][0] else 0
                    }

            return {
                'success': True,
                'output_path': output_path,
                'base_currency': base_currency,
                'target_currencies': target_currencies,
                'date_range': {'start': start_date, 'end': end_date},
                'frequency': frequency,
                'row_count': len(df),
                'columns': df.columns,
                'rate_statistics': rate_stats,
                'file_size_bytes': os.path.getsize(output_path)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_generate_exchange_rates']
    registry.register(
        '02_generate_exchange_rates',
        generate_exchange_rates,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 02_generate_multicurrency_fact
    def generate_multicurrency_fact(
        output_path: str,
        transaction_currencies: List[str],
        start_date: str,
        end_date: str,
        row_count: int = 10000,
        reporting_currency: str = 'USD',
        amount_config: Optional[Dict[str, Any]] = None,
        include_fx_details: bool = True,
        seed: Optional[int] = None
    ) -> Dict[str, Any]:
        """Generate fact table with multi-currency support"""
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            mc_gen = MultiCurrencyFactGenerator(seed=seed)
            df = mc_gen.generate(
                row_count=row_count,
                transaction_currencies=transaction_currencies,
                reporting_currency=reporting_currency,
                start_date=start_date,
                end_date=end_date,
                amount_config=amount_config,
                include_fx_details=include_fx_details
            )

            # Write output
            ext = os.path.splitext(output_path)[1].lower()
            if ext == '.csv':
                df.write_csv(output_path)
            elif ext == '.parquet':
                df.write_parquet(output_path)
            else:
                df.write_csv(output_path)

            # Get currency breakdown
            currency_counts = df.group_by('transaction_currency').agg(
                pl.count().alias('count'),
                pl.col('transaction_amount').sum().alias('total_tx_amount'),
                pl.col('reporting_amount').sum().alias('total_reporting_amount')
            ).to_dicts()

            return {
                'success': True,
                'output_path': output_path,
                'row_count': len(df),
                'columns': df.columns,
                'reporting_currency': reporting_currency,
                'transaction_currencies': transaction_currencies,
                'currency_breakdown': currency_counts,
                'total_reporting_amount': round(df['reporting_amount'].sum(), 2),
                'file_size_bytes': os.path.getsize(output_path)
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_generate_multicurrency_fact']
    registry.register(
        '02_generate_multicurrency_fact',
        generate_multicurrency_fact,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # Initialize industry schema generator
    industry_gen = IndustrySchemaGenerator()

    # 02_generate_industry_schema
    def generate_industry_schema(
        template: str,
        output_dir: str,
        scale_factor: float = 1.0,
        seed: Optional[int] = None
    ) -> Dict[str, Any]:
        """Generate complete industry-specific star schema"""
        try:
            result = industry_gen.generate(
                template_name=template,
                output_dir=output_dir,
                scale_factor=scale_factor,
                seed=seed
            )
            return result

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_generate_industry_schema']
    registry.register(
        '02_generate_industry_schema',
        generate_industry_schema,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # 02_list_industry_templates
    def list_industry_templates() -> Dict[str, Any]:
        """List available industry templates"""
        try:
            templates = industry_gen.list_templates()

            return {
                'success': True,
                'templates': templates,
                'total_count': len(templates),
                'available_industries': [t['name'] for t in templates]
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    schema = TOOL_SCHEMAS['02_list_industry_templates']
    registry.register(
        '02_list_industry_templates',
        list_industry_templates,
        'generation',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )
