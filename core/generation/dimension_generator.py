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

    def __init__(self, locale: str = 'en_US'):
        self.faker = Faker(locale)
        self.locale = locale

    def generate(
        self,
        name: str,
        columns: List[Dict[str, Any]],
        row_count: int,
        output_path: Optional[str] = None,
        output_format: str = 'csv'
    ) -> Dict[str, Any]:
        """
        Generate a dimension table.

        Args:
            name: Table name
            columns: Column definitions
            row_count: Number of rows to generate
            output_path: Optional output file path
            output_format: 'csv' or 'parquet'

        Returns:
            Generation result with data
        """
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
