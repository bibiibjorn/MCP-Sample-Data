"""
Dimension Generator Module
Generates dimension tables with realistic data
"""
import polars as pl
from typing import Dict, Any, List, Optional
from faker import Faker
import random
import logging

logger = logging.getLogger(__name__)


class DimensionGenerator:
    """Generates dimension tables"""

    # Predefined dimension templates
    DIMENSION_TEMPLATES = {
        'customer': [
            {'name': 'customer_id', 'type': 'integer', 'unique': True},
            {'name': 'customer_name', 'generator': 'name'},
            {'name': 'email', 'generator': 'email'},
            {'name': 'phone', 'generator': 'phone'},
            {'name': 'city', 'generator': 'city'},
            {'name': 'state', 'generator': 'state'},
            {'name': 'country', 'generator': 'country'},
            {'name': 'segment', 'generator': 'segment'},
        ],
        'product': [
            {'name': 'product_id', 'type': 'integer', 'unique': True},
            {'name': 'product_name', 'generator': 'product_name'},
            {'name': 'sku', 'generator': 'sku', 'unique': True},
            {'name': 'category', 'generator': 'category'},
            {'name': 'unit_price', 'type': 'decimal'},
            {'name': 'unit_cost', 'type': 'decimal'},
        ],
        'geography': [
            {'name': 'geography_id', 'type': 'integer', 'unique': True},
            {'name': 'city', 'generator': 'city'},
            {'name': 'state', 'generator': 'state'},
            {'name': 'country', 'generator': 'country'},
            {'name': 'region', 'generator': 'region'},
            {'name': 'postal_code', 'generator': 'zipcode'},
        ],
        'employee': [
            {'name': 'employee_id', 'type': 'integer', 'unique': True},
            {'name': 'first_name', 'generator': 'first_name'},
            {'name': 'last_name', 'generator': 'last_name'},
            {'name': 'email', 'generator': 'email'},
            {'name': 'department', 'generator': 'department'},
            {'name': 'job_title', 'generator': 'job'},
            {'name': 'hire_date', 'generator': 'date'},
        ],
        'department': [
            {'name': 'department_id', 'type': 'integer', 'unique': True},
            {'name': 'department_name', 'generator': 'department', 'unique': True},
            {'name': 'cost_center', 'type': 'string'},
        ],
        'account': [
            {'name': 'account_id', 'type': 'integer', 'unique': True},
            {'name': 'account_code', 'type': 'string', 'unique': True},
            {'name': 'account_name', 'type': 'string'},
            {'name': 'account_type', 'values': ['Asset', 'Liability', 'Equity', 'Revenue', 'Expense']},
            {'name': 'is_active', 'type': 'boolean'},
        ],
        'supplier': [
            {'name': 'supplier_id', 'type': 'integer', 'unique': True},
            {'name': 'supplier_name', 'generator': 'company'},
            {'name': 'contact_name', 'generator': 'name'},
            {'name': 'phone', 'generator': 'phone'},
            {'name': 'city', 'generator': 'city'},
            {'name': 'country', 'generator': 'country'},
        ],
        'warehouse': [
            {'name': 'warehouse_id', 'type': 'integer', 'unique': True},
            {'name': 'warehouse_name', 'type': 'string'},
            {'name': 'location', 'generator': 'city'},
            {'name': 'capacity', 'type': 'integer'},
            {'name': 'region', 'generator': 'region'},
        ],
    }

    def __init__(self, locale: str = 'en_US'):
        self.faker = Faker(locale)
        self.locale = locale

    def generate(
        self,
        name: str = None,
        columns: List[Dict[str, Any]] = None,
        row_count: int = 1000,
        output_path: Optional[str] = None,
        output_format: str = 'csv',
        *,  # Force keyword arguments after this
        dimension_type: str = None,
        locale: str = None
    ) -> Dict[str, Any]:
        """
        Generate a dimension table.

        Args:
            name: Table name (optional if dimension_type provided)
            columns: Column definitions (optional if dimension_type provided)
            row_count: Number of rows to generate
            output_path: Optional output file path
            output_format: 'csv' or 'parquet'
            dimension_type: Predefined dimension type (customer, product, etc.)
            locale: Locale for data generation

        Returns:
            Generation result with data
        """
        # Handle locale override
        if locale and locale != self.locale:
            self.faker = Faker(locale)
            self.locale = locale

        # If dimension_type provided, use template
        if dimension_type:
            template = self.DIMENSION_TEMPLATES.get(dimension_type.lower())
            if not template:
                return {
                    'success': False,
                    'error': f"Unknown dimension type: {dimension_type}. "
                             f"Available: {', '.join(self.DIMENSION_TEMPLATES.keys())}"
                }
            columns = template
            name = name or f"dim_{dimension_type}"

        if not columns:
            return {'success': False, 'error': 'Either columns or dimension_type must be provided'}

        try:
            data = {}

            for col_def in columns:
                col_name = col_def['name']
                col_type = col_def.get('type', 'string')
                generator = col_def.get('generator')
                unique = col_def.get('unique', False)
                nullable = col_def.get('nullable', False)
                values = col_def.get('values')

                # Generate column data
                col_data = self._generate_column(
                    col_type, generator, row_count, unique, values
                )

                # Add nulls if nullable
                if nullable and not unique:
                    null_indices = random.sample(
                        range(row_count),
                        int(row_count * random.uniform(0.01, 0.05))
                    )
                    for idx in null_indices:
                        col_data[idx] = None

                data[col_name] = col_data

            # Create DataFrame
            df = pl.DataFrame(data)

            # Save if output path provided
            if output_path:
                if output_format == 'parquet':
                    df.write_parquet(output_path)
                else:
                    df.write_csv(output_path)

            return {
                'success': True,
                'df': df,
                'table_name': name,
                'row_count': len(df),
                'columns': df.columns,
                'output_path': output_path,
                'sample_data': df.head(5).to_dicts()
            }

        except Exception as e:
            logger.error(f"Error generating dimension: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _generate_column(
        self,
        col_type: str,
        generator: Optional[str],
        count: int,
        unique: bool,
        values: Optional[List] = None
    ) -> List:
        """Generate data for a single column"""
        if values:
            # Choose from fixed values
            if unique:
                if len(values) < count:
                    raise ValueError(f"Not enough unique values ({len(values)}) for {count} rows")
                return random.sample(values, count)
            return [random.choice(values) for _ in range(count)]

        if generator:
            return self._generate_with_faker(generator, count, unique)

        # Default generators by type
        type_generators = {
            'string': lambda: self.faker.word(),
            'integer': lambda: random.randint(1, 10000),
            'decimal': lambda: round(random.uniform(0, 10000), 2),
            'date': lambda: self.faker.date_this_decade(),
            'boolean': lambda: random.choice([True, False])
        }

        gen_func = type_generators.get(col_type, type_generators['string'])

        if unique:
            values_set = set()
            values_list = []
            attempts = 0
            while len(values_list) < count and attempts < count * 10:
                val = gen_func()
                if val not in values_set:
                    values_set.add(val)
                    values_list.append(val)
                attempts += 1
            return values_list
        else:
            return [gen_func() for _ in range(count)]

    def _generate_with_faker(self, generator: str, count: int, unique: bool) -> List:
        """Generate data using Faker"""
        # Map common generator names to faker methods
        faker_map = {
            'name': self.faker.name,
            'first_name': self.faker.first_name,
            'last_name': self.faker.last_name,
            'company': self.faker.company,
            'email': self.faker.email,
            'phone': self.faker.phone_number,
            'address': self.faker.address,
            'city': self.faker.city,
            'state': self.faker.state,
            'country': self.faker.country,
            'zipcode': self.faker.zipcode,
            'date': lambda: self.faker.date_this_decade(),
            'date_of_birth': self.faker.date_of_birth,
            'job': self.faker.job,
            'text': lambda: self.faker.text(max_nb_chars=50),
            'sentence': self.faker.sentence,
            'word': self.faker.word,
            'uuid': self.faker.uuid4,
            'sku': lambda: self.faker.bothify(text='???-#####').upper(),
            'product_name': lambda: f"{self.faker.word().title()} {random.choice(['Pro', 'Plus', 'Basic', 'Premium'])}",
            'category': lambda: random.choice(['Electronics', 'Clothing', 'Food', 'Home', 'Office', 'Sports']),
            'department': lambda: random.choice(['Sales', 'Marketing', 'Engineering', 'Finance', 'HR', 'Operations']),
            'region': lambda: random.choice(['North', 'South', 'East', 'West', 'Central']),
            'segment': lambda: random.choice(['Enterprise', 'Mid-Market', 'SMB', 'Consumer'])
        }

        gen_func = faker_map.get(generator)
        if not gen_func:
            # Try to get attribute from faker directly
            if hasattr(self.faker, generator):
                gen_func = getattr(self.faker, generator)
            else:
                gen_func = self.faker.word

        if unique:
            values_set = set()
            values_list = []
            attempts = 0
            while len(values_list) < count and attempts < count * 10:
                val = gen_func()
                if val not in values_set:
                    values_set.add(val)
                    values_list.append(val)
                attempts += 1
            if len(values_list) < count:
                # Pad with numbered values if we couldn't get enough uniques
                for i in range(count - len(values_list)):
                    values_list.append(f"{gen_func()}_{i}")
            return values_list
        else:
            return [gen_func() for _ in range(count)]
