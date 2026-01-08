"""
Industry Templates - Pre-built star schema templates for specific industries

Provides complete, realistic data models for:
- Retail/E-commerce
- Healthcare
- Manufacturing
- Banking/Financial Services
- Insurance
- Telecommunications
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from datetime import date, datetime, timedelta
from pathlib import Path
import os
import random
import yaml

import polars as pl

from .dimension_generator import DimensionGenerator
from .fact_generator import FactGenerator
from .date_dimension import DateDimensionGenerator


# Industry Template Definitions
INDUSTRY_TEMPLATES = {
    'retail': {
        'name': 'Retail/E-commerce',
        'description': 'Complete retail star schema with customers, products, stores, and sales',
        'dimensions': {
            'customer': {
                'type': 'customer',
                'row_count': 5000,
                'columns': ['customer_id', 'first_name', 'last_name', 'email', 'segment', 'registration_date']
            },
            'product': {
                'type': 'product',
                'row_count': 1000,
                'columns': ['product_id', 'product_name', 'category', 'subcategory', 'brand', 'unit_price', 'unit_cost']
            },
            'store': {
                'type': 'geography',
                'row_count': 50,
                'columns': ['store_id', 'store_name', 'city', 'state', 'country', 'region']
            },
            'promotion': {
                'type': 'custom',
                'row_count': 100,
                'columns': ['promotion_id', 'promotion_name', 'discount_type', 'discount_value', 'start_date', 'end_date']
            },
            'date': {
                'type': 'date',
                'start_date': '2022-01-01',
                'end_date': '2025-12-31'
            }
        },
        'facts': {
            'sales': {
                'type': 'sales',
                'row_count': 500000,
                'measures': ['quantity', 'unit_price', 'discount_amount', 'total_amount', 'cost_amount', 'profit'],
                'dimension_keys': ['customer_id', 'product_id', 'store_id', 'date_key']
            },
            'inventory': {
                'type': 'inventory',
                'row_count': 50000,
                'measures': ['quantity_on_hand', 'quantity_on_order', 'reorder_point', 'days_of_supply'],
                'dimension_keys': ['product_id', 'store_id', 'date_key']
            }
        }
    },
    'healthcare': {
        'name': 'Healthcare',
        'description': 'Healthcare star schema with patients, providers, and encounters',
        'dimensions': {
            'patient': {
                'type': 'custom',
                'row_count': 10000,
                'columns': ['patient_id', 'first_name', 'last_name', 'date_of_birth', 'gender', 'blood_type', 'insurance_type']
            },
            'provider': {
                'type': 'custom',
                'row_count': 500,
                'columns': ['provider_id', 'provider_name', 'specialty', 'department', 'facility', 'npi_number']
            },
            'diagnosis': {
                'type': 'custom',
                'row_count': 1000,
                'columns': ['diagnosis_id', 'icd_code', 'diagnosis_name', 'category', 'severity']
            },
            'procedure': {
                'type': 'custom',
                'row_count': 500,
                'columns': ['procedure_id', 'cpt_code', 'procedure_name', 'category', 'typical_duration_minutes']
            },
            'date': {
                'type': 'date',
                'start_date': '2022-01-01',
                'end_date': '2025-12-31'
            }
        },
        'facts': {
            'encounters': {
                'type': 'transactions',
                'row_count': 200000,
                'measures': ['duration_minutes', 'charge_amount', 'payment_amount', 'copay_amount'],
                'dimension_keys': ['patient_id', 'provider_id', 'diagnosis_id', 'date_key']
            },
            'claims': {
                'type': 'finance',
                'row_count': 150000,
                'measures': ['billed_amount', 'allowed_amount', 'paid_amount', 'patient_responsibility'],
                'dimension_keys': ['patient_id', 'provider_id', 'procedure_id', 'date_key']
            }
        }
    },
    'manufacturing': {
        'name': 'Manufacturing',
        'description': 'Manufacturing star schema with production, quality, and equipment',
        'dimensions': {
            'machine': {
                'type': 'custom',
                'row_count': 100,
                'columns': ['machine_id', 'machine_name', 'machine_type', 'manufacturer', 'install_date', 'location']
            },
            'operator': {
                'type': 'employee',
                'row_count': 200,
                'columns': ['operator_id', 'first_name', 'last_name', 'shift', 'skill_level', 'hire_date']
            },
            'product': {
                'type': 'custom',
                'row_count': 500,
                'columns': ['product_id', 'product_name', 'product_line', 'specification', 'unit_of_measure']
            },
            'shift': {
                'type': 'custom',
                'row_count': 3,
                'columns': ['shift_id', 'shift_name', 'start_time', 'end_time']
            },
            'date': {
                'type': 'date',
                'start_date': '2022-01-01',
                'end_date': '2025-12-31'
            }
        },
        'facts': {
            'production': {
                'type': 'custom',
                'row_count': 300000,
                'measures': ['units_produced', 'units_scrapped', 'cycle_time_seconds', 'setup_time_minutes', 'downtime_minutes'],
                'dimension_keys': ['machine_id', 'operator_id', 'product_id', 'shift_id', 'date_key']
            },
            'quality': {
                'type': 'custom',
                'row_count': 50000,
                'measures': ['sample_size', 'defects_found', 'defect_rate', 'rework_time_minutes'],
                'dimension_keys': ['machine_id', 'product_id', 'date_key']
            }
        }
    },
    'banking': {
        'name': 'Banking/Financial Services',
        'description': 'Banking star schema with accounts, customers, and transactions',
        'dimensions': {
            'customer': {
                'type': 'customer',
                'row_count': 10000,
                'columns': ['customer_id', 'first_name', 'last_name', 'customer_type', 'segment', 'risk_rating', 'relationship_start_date']
            },
            'account': {
                'type': 'custom',
                'row_count': 20000,
                'columns': ['account_id', 'account_number', 'account_type', 'status', 'open_date', 'interest_rate']
            },
            'branch': {
                'type': 'geography',
                'row_count': 100,
                'columns': ['branch_id', 'branch_name', 'city', 'state', 'region', 'branch_type']
            },
            'transaction_type': {
                'type': 'custom',
                'row_count': 20,
                'columns': ['transaction_type_id', 'transaction_type_name', 'category', 'is_debit']
            },
            'date': {
                'type': 'date',
                'start_date': '2022-01-01',
                'end_date': '2025-12-31'
            }
        },
        'facts': {
            'transactions': {
                'type': 'finance',
                'row_count': 1000000,
                'measures': ['transaction_amount', 'fee_amount', 'running_balance'],
                'dimension_keys': ['customer_id', 'account_id', 'branch_id', 'transaction_type_id', 'date_key']
            },
            'account_balances': {
                'type': 'custom',
                'row_count': 100000,
                'measures': ['opening_balance', 'closing_balance', 'average_balance', 'interest_earned'],
                'dimension_keys': ['account_id', 'date_key']
            }
        }
    },
    'insurance': {
        'name': 'Insurance',
        'description': 'Insurance star schema with policies, claims, and agents',
        'dimensions': {
            'policyholder': {
                'type': 'customer',
                'row_count': 15000,
                'columns': ['policyholder_id', 'first_name', 'last_name', 'date_of_birth', 'gender', 'occupation', 'risk_class']
            },
            'policy': {
                'type': 'custom',
                'row_count': 20000,
                'columns': ['policy_id', 'policy_number', 'policy_type', 'coverage_type', 'effective_date', 'expiration_date', 'premium_amount']
            },
            'agent': {
                'type': 'employee',
                'row_count': 200,
                'columns': ['agent_id', 'first_name', 'last_name', 'agency', 'license_state', 'commission_rate']
            },
            'coverage': {
                'type': 'custom',
                'row_count': 50,
                'columns': ['coverage_id', 'coverage_name', 'coverage_category', 'deductible', 'max_benefit']
            },
            'date': {
                'type': 'date',
                'start_date': '2022-01-01',
                'end_date': '2025-12-31'
            }
        },
        'facts': {
            'claims': {
                'type': 'custom',
                'row_count': 50000,
                'measures': ['claim_amount', 'approved_amount', 'paid_amount', 'deductible_applied', 'processing_days'],
                'dimension_keys': ['policyholder_id', 'policy_id', 'coverage_id', 'date_key']
            },
            'premiums': {
                'type': 'custom',
                'row_count': 100000,
                'measures': ['premium_amount', 'commission_amount', 'tax_amount', 'net_premium'],
                'dimension_keys': ['policyholder_id', 'policy_id', 'agent_id', 'date_key']
            }
        }
    },
    'telecom': {
        'name': 'Telecommunications',
        'description': 'Telecom star schema with subscribers, plans, and usage',
        'dimensions': {
            'subscriber': {
                'type': 'customer',
                'row_count': 50000,
                'columns': ['subscriber_id', 'first_name', 'last_name', 'email', 'account_type', 'status', 'activation_date']
            },
            'plan': {
                'type': 'custom',
                'row_count': 30,
                'columns': ['plan_id', 'plan_name', 'plan_type', 'monthly_fee', 'data_limit_gb', 'voice_minutes', 'text_limit']
            },
            'device': {
                'type': 'custom',
                'row_count': 200,
                'columns': ['device_id', 'device_name', 'manufacturer', 'device_type', 'release_date', 'retail_price']
            },
            'tower': {
                'type': 'custom',
                'row_count': 500,
                'columns': ['tower_id', 'tower_name', 'city', 'state', 'latitude', 'longitude', 'tower_type']
            },
            'date': {
                'type': 'date',
                'start_date': '2022-01-01',
                'end_date': '2025-12-31'
            }
        },
        'facts': {
            'usage': {
                'type': 'custom',
                'row_count': 500000,
                'measures': ['data_mb', 'voice_minutes', 'text_count', 'roaming_charges', 'overage_charges'],
                'dimension_keys': ['subscriber_id', 'plan_id', 'tower_id', 'date_key']
            },
            'billing': {
                'type': 'custom',
                'row_count': 150000,
                'measures': ['plan_charges', 'usage_charges', 'taxes', 'total_amount', 'payment_amount'],
                'dimension_keys': ['subscriber_id', 'plan_id', 'date_key']
            }
        }
    }
}


class IndustrySchemaGenerator:
    """Generate complete industry-specific star schemas"""

    def __init__(self, templates_dir: Optional[str] = None):
        """Initialize with optional custom templates directory"""
        self.templates = dict(INDUSTRY_TEMPLATES)
        self.dimension_gen = DimensionGenerator()
        self.fact_gen = FactGenerator()
        self.date_gen = DateDimensionGenerator()

        if templates_dir:
            self._load_custom_templates(templates_dir)

    def _load_custom_templates(self, templates_dir: str):
        """Load custom templates from YAML files"""
        path = Path(templates_dir)
        if not path.exists():
            return

        for yaml_file in path.glob('*.yaml'):
            try:
                with open(yaml_file, 'r') as f:
                    data = yaml.safe_load(f)
                if isinstance(data, dict):
                    for name, template in data.items():
                        self.templates[name] = template
            except Exception:
                pass

    def list_templates(self) -> List[Dict[str, Any]]:
        """List available industry templates"""
        return [
            {
                'name': name,
                'display_name': template.get('name', name),
                'description': template.get('description', ''),
                'dimensions': list(template.get('dimensions', {}).keys()),
                'facts': list(template.get('facts', {}).keys())
            }
            for name, template in self.templates.items()
        ]

    def get_template_details(self, template_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a template"""
        template = self.templates.get(template_name)
        if not template:
            return None

        return {
            'name': template_name,
            'display_name': template.get('name', template_name),
            'description': template.get('description', ''),
            'dimensions': {
                name: {
                    'type': dim.get('type'),
                    'row_count': dim.get('row_count'),
                    'columns': dim.get('columns', [])
                }
                for name, dim in template.get('dimensions', {}).items()
            },
            'facts': {
                name: {
                    'type': fact.get('type'),
                    'row_count': fact.get('row_count'),
                    'measures': fact.get('measures', []),
                    'dimension_keys': fact.get('dimension_keys', [])
                }
                for name, fact in template.get('facts', {}).items()
            }
        }

    def generate(
        self,
        template_name: str,
        output_dir: str,
        scale_factor: float = 1.0,
        seed: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Generate complete star schema for an industry template.

        Args:
            template_name: Name of the industry template
            output_dir: Directory for output files
            scale_factor: Multiply row counts by this factor
            seed: Random seed for reproducibility

        Returns:
            Dictionary with generation results
        """
        if seed is not None:
            random.seed(seed)

        template = self.templates.get(template_name)
        if not template:
            return {
                'success': False,
                'error': f'Unknown template: {template_name}. Available: {list(self.templates.keys())}'
            }

        os.makedirs(output_dir, exist_ok=True)

        generated_files = []
        dimension_dfs = {}
        errors = []

        # Generate dimensions
        for dim_name, dim_config in template.get('dimensions', {}).items():
            try:
                output_path = os.path.join(output_dir, f'dim_{dim_name}.csv')

                if dim_config.get('type') == 'date':
                    result = self.date_gen.generate(
                        start_date=dim_config.get('start_date', '2022-01-01'),
                        end_date=dim_config.get('end_date', '2025-12-31')
                    )
                elif dim_config.get('type') in ['customer', 'product', 'geography', 'employee']:
                    row_count = int(dim_config.get('row_count', 1000) * scale_factor)
                    result = self.dimension_gen.generate(
                        dimension_type=dim_config['type'],
                        row_count=row_count
                    )
                else:
                    # Custom dimension - generate with basic structure
                    row_count = int(dim_config.get('row_count', 1000) * scale_factor)
                    result = self._generate_custom_dimension(
                        dim_name=dim_name,
                        columns=dim_config.get('columns', []),
                        row_count=row_count
                    )

                if result.get('success', True):
                    df = result.get('df')
                    if df is not None:
                        df.write_csv(output_path)
                        dimension_dfs[dim_name] = df
                        generated_files.append({
                            'name': f'dim_{dim_name}',
                            'path': output_path,
                            'type': 'dimension',
                            'rows': len(df),
                            'columns': df.columns
                        })
                else:
                    errors.append(f"Failed to generate dimension {dim_name}: {result.get('error')}")

            except Exception as e:
                errors.append(f"Error generating dimension {dim_name}: {str(e)}")

        # Generate facts
        for fact_name, fact_config in template.get('facts', {}).items():
            try:
                output_path = os.path.join(output_dir, f'fact_{fact_name}.csv')
                row_count = int(fact_config.get('row_count', 10000) * scale_factor)

                # Map dimension names to DataFrames
                dims_for_fact = {}
                for key in fact_config.get('dimension_keys', []):
                    # Find matching dimension
                    dim_name = key.replace('_id', '').replace('_key', '')
                    if dim_name in dimension_dfs:
                        dims_for_fact[dim_name] = dimension_dfs[dim_name]
                    elif 'date' in dim_name and 'date' in dimension_dfs:
                        dims_for_fact['date'] = dimension_dfs['date']

                fact_type = fact_config.get('type', 'transactions')
                if fact_type in ['sales', 'finance', 'inventory', 'hr', 'transactions']:
                    result = self.fact_gen.generate_from_type(
                        fact_type=fact_type,
                        dimensions=dims_for_fact,
                        row_count=row_count
                    )
                else:
                    result = self._generate_custom_fact(
                        fact_name=fact_name,
                        measures=fact_config.get('measures', []),
                        dimension_keys=fact_config.get('dimension_keys', []),
                        dimensions=dims_for_fact,
                        row_count=row_count
                    )

                if result.get('success', True):
                    df = result.get('df')
                    if df is not None:
                        df.write_csv(output_path)
                        generated_files.append({
                            'name': f'fact_{fact_name}',
                            'path': output_path,
                            'type': 'fact',
                            'rows': len(df),
                            'columns': df.columns
                        })
                else:
                    errors.append(f"Failed to generate fact {fact_name}: {result.get('error')}")

            except Exception as e:
                errors.append(f"Error generating fact {fact_name}: {str(e)}")

        return {
            'success': len(errors) == 0,
            'template': template_name,
            'display_name': template.get('name', template_name),
            'output_dir': output_dir,
            'scale_factor': scale_factor,
            'files_generated': generated_files,
            'total_files': len(generated_files),
            'total_rows': sum(f['rows'] for f in generated_files),
            'errors': errors if errors else None
        }

    def _generate_custom_dimension(
        self,
        dim_name: str,
        columns: List[str],
        row_count: int
    ) -> Dict[str, Any]:
        """Generate a custom dimension with specified columns"""
        from faker import Faker
        fake = Faker()

        data = {}
        for col in columns:
            col_lower = col.lower()

            # ID column
            if col_lower.endswith('_id'):
                data[col] = list(range(1, row_count + 1))

            # Date columns
            elif 'date' in col_lower:
                start = datetime(2020, 1, 1)
                end = datetime(2025, 12, 31)
                data[col] = [fake.date_between(start_date=start, end_date=end) for _ in range(row_count)]

            # Name columns
            elif 'name' in col_lower:
                if 'first' in col_lower:
                    data[col] = [fake.first_name() for _ in range(row_count)]
                elif 'last' in col_lower:
                    data[col] = [fake.last_name() for _ in range(row_count)]
                else:
                    data[col] = [fake.company() if random.random() > 0.5 else fake.catch_phrase() for _ in range(row_count)]

            # Category/Type columns
            elif 'type' in col_lower or 'category' in col_lower or 'status' in col_lower:
                options = [f'{col.replace("_", " ").title()} {i}' for i in range(1, 6)]
                data[col] = [random.choice(options) for _ in range(row_count)]

            # Location columns
            elif 'city' in col_lower:
                data[col] = [fake.city() for _ in range(row_count)]
            elif 'state' in col_lower:
                data[col] = [fake.state_abbr() for _ in range(row_count)]
            elif 'country' in col_lower:
                data[col] = [fake.country() for _ in range(row_count)]
            elif 'region' in col_lower:
                data[col] = [random.choice(['North', 'South', 'East', 'West', 'Central']) for _ in range(row_count)]

            # Numeric columns
            elif 'amount' in col_lower or 'price' in col_lower or 'cost' in col_lower:
                data[col] = [round(random.uniform(10, 1000), 2) for _ in range(row_count)]
            elif 'rate' in col_lower or 'percent' in col_lower:
                data[col] = [round(random.uniform(0, 0.3), 4) for _ in range(row_count)]
            elif 'minutes' in col_lower or 'duration' in col_lower:
                data[col] = [random.randint(5, 120) for _ in range(row_count)]

            # Email
            elif 'email' in col_lower:
                data[col] = [fake.email() for _ in range(row_count)]

            # Default: random string
            else:
                data[col] = [f'{col}_{i}' for i in range(1, row_count + 1)]

        return {'success': True, 'df': pl.DataFrame(data)}

    def _generate_custom_fact(
        self,
        fact_name: str,
        measures: List[str],
        dimension_keys: List[str],
        dimensions: Dict[str, pl.DataFrame],
        row_count: int
    ) -> Dict[str, Any]:
        """Generate a custom fact table"""
        data = {}

        # Generate fact ID
        data[f'{fact_name}_id'] = list(range(1, row_count + 1))

        # Generate foreign keys from dimensions
        for key in dimension_keys:
            dim_name = key.replace('_id', '').replace('_key', '')
            if dim_name in dimensions:
                dim_df = dimensions[dim_name]
                id_col = dim_df.columns[0]  # First column is typically the ID
                available_ids = dim_df[id_col].to_list()
                data[key] = [random.choice(available_ids) for _ in range(row_count)]
            elif 'date' in dim_name and 'date' in dimensions:
                dim_df = dimensions['date']
                id_col = dim_df.columns[0]
                available_ids = dim_df[id_col].to_list()
                data[key] = [random.choice(available_ids) for _ in range(row_count)]

        # Generate measures
        for measure in measures:
            measure_lower = measure.lower()

            if 'amount' in measure_lower or 'total' in measure_lower or 'charge' in measure_lower:
                data[measure] = [round(random.uniform(10, 5000), 2) for _ in range(row_count)]
            elif 'quantity' in measure_lower or 'count' in measure_lower or 'units' in measure_lower:
                data[measure] = [random.randint(1, 100) for _ in range(row_count)]
            elif 'rate' in measure_lower or 'percent' in measure_lower:
                data[measure] = [round(random.uniform(0, 1), 4) for _ in range(row_count)]
            elif 'minutes' in measure_lower or 'time' in measure_lower or 'duration' in measure_lower:
                data[measure] = [random.randint(1, 480) for _ in range(row_count)]
            elif 'days' in measure_lower:
                data[measure] = [random.randint(1, 30) for _ in range(row_count)]
            elif 'balance' in measure_lower:
                data[measure] = [round(random.uniform(-10000, 100000), 2) for _ in range(row_count)]
            else:
                data[measure] = [round(random.uniform(0, 1000), 2) for _ in range(row_count)]

        return {'success': True, 'df': pl.DataFrame(data)}


def list_industry_templates() -> List[Dict[str, Any]]:
    """List available industry templates"""
    generator = IndustrySchemaGenerator()
    return generator.list_templates()


def generate_industry_schema(
    template_name: str,
    output_dir: str,
    scale_factor: float = 1.0,
    seed: Optional[int] = None
) -> Dict[str, Any]:
    """Generate industry-specific star schema"""
    generator = IndustrySchemaGenerator()
    return generator.generate(template_name, output_dir, scale_factor, seed)
