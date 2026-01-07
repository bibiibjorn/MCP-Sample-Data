"""
Generation Handlers
Handlers for data generation tools
"""
import polars as pl
from typing import Dict, Any, List, Optional
import os
from datetime import datetime

from core.generation import DimensionGenerator, FactGenerator, DateDimensionGenerator, TemplateEngine
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

            result = fact_gen.generate(
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

            fact_result = fact_gen.generate(
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
