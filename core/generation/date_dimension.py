"""
Date Dimension Generator Module
Generates standard date dimension tables for Power BI
"""
import polars as pl
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class DateDimensionGenerator:
    """Generates date dimension tables"""

    def __init__(self):
        self.day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        self.month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                           'July', 'August', 'September', 'October', 'November', 'December']

    def generate(
        self,
        start_date: str,
        end_date: str,
        fiscal_year_start_month: int = 1,
        include_holidays: bool = False,
        holiday_country: str = 'US',
        output_path: Optional[str] = None,
        output_format: str = 'csv'
    ) -> Dict[str, Any]:
        """
        Generate a date dimension table.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            fiscal_year_start_month: Fiscal year start month (1-12)
            include_holidays: Include holiday flags
            holiday_country: Country code for holidays
            output_path: Optional output file path
            output_format: 'csv' or 'parquet'

        Returns:
            Generation result with data
        """
        try:
            # Parse dates
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')

            if end < start:
                return {'success': False, 'error': 'End date must be after start date'}

            # Generate all dates
            dates = []
            current = start
            while current <= end:
                dates.append(current)
                current += timedelta(days=1)

            # Build dimension columns
            data = {
                'date_key': [],
                'full_date': [],
                'year': [],
                'quarter': [],
                'month': [],
                'month_name': [],
                'month_name_short': [],
                'week': [],
                'day_of_month': [],
                'day_of_week': [],
                'day_name': [],
                'day_name_short': [],
                'day_of_year': [],
                'is_weekend': [],
                'is_weekday': [],
                'year_month': [],
                'year_quarter': [],
                'fiscal_year': [],
                'fiscal_quarter': [],
                'fiscal_month': []
            }

            for dt in dates:
                data['date_key'].append(int(dt.strftime('%Y%m%d')))
                data['full_date'].append(dt.date())
                data['year'].append(dt.year)
                data['quarter'].append((dt.month - 1) // 3 + 1)
                data['month'].append(dt.month)
                data['month_name'].append(self.month_names[dt.month - 1])
                data['month_name_short'].append(self.month_names[dt.month - 1][:3])
                data['week'].append(dt.isocalendar()[1])
                data['day_of_month'].append(dt.day)
                data['day_of_week'].append(dt.weekday() + 1)
                data['day_name'].append(self.day_names[dt.weekday()])
                data['day_name_short'].append(self.day_names[dt.weekday()][:3])
                data['day_of_year'].append(dt.timetuple().tm_yday)
                data['is_weekend'].append(dt.weekday() >= 5)
                data['is_weekday'].append(dt.weekday() < 5)
                data['year_month'].append(f"{dt.year}-{dt.month:02d}")
                data['year_quarter'].append(f"{dt.year}-Q{(dt.month - 1) // 3 + 1}")

                # Calculate fiscal year/quarter/month
                fiscal_info = self._calculate_fiscal(dt, fiscal_year_start_month)
                data['fiscal_year'].append(fiscal_info['fiscal_year'])
                data['fiscal_quarter'].append(fiscal_info['fiscal_quarter'])
                data['fiscal_month'].append(fiscal_info['fiscal_month'])

            # Add holiday columns if requested
            if include_holidays:
                holidays = self._get_holidays(dates, holiday_country)
                data['is_holiday'] = holidays['is_holiday']
                data['holiday_name'] = holidays['holiday_name']

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
                'table_name': 'dim_date',
                'row_count': len(df),
                'columns': df.columns,
                'date_range': {
                    'start': start_date,
                    'end': end_date
                },
                'fiscal_year_start_month': fiscal_year_start_month,
                'output_path': output_path,
                'sample_data': df.head(5).to_dicts()
            }

        except Exception as e:
            logger.error(f"Error generating date dimension: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _calculate_fiscal(self, dt: datetime, fiscal_start_month: int) -> Dict[str, int]:
        """Calculate fiscal year, quarter, and month"""
        if fiscal_start_month == 1:
            # Standard calendar year
            return {
                'fiscal_year': dt.year,
                'fiscal_quarter': (dt.month - 1) // 3 + 1,
                'fiscal_month': dt.month
            }

        # Calculate fiscal year offset
        if dt.month >= fiscal_start_month:
            fiscal_year = dt.year + 1  # Fiscal year ends next calendar year
            fiscal_month = dt.month - fiscal_start_month + 1
        else:
            fiscal_year = dt.year
            fiscal_month = dt.month + (12 - fiscal_start_month) + 1

        fiscal_quarter = (fiscal_month - 1) // 3 + 1

        return {
            'fiscal_year': fiscal_year,
            'fiscal_quarter': fiscal_quarter,
            'fiscal_month': fiscal_month
        }

    def _get_holidays(self, dates: list, country: str) -> Dict[str, list]:
        """Get holiday information for dates"""
        # Simple holiday detection - could be enhanced with holidays library
        is_holiday = []
        holiday_name = []

        # Common US holidays (simplified)
        us_holidays = {
            (1, 1): "New Year's Day",
            (7, 4): "Independence Day",
            (12, 25): "Christmas Day",
            (12, 31): "New Year's Eve"
        }

        for dt in dates:
            key = (dt.month, dt.day)
            if country.upper() == 'US' and key in us_holidays:
                is_holiday.append(True)
                holiday_name.append(us_holidays[key])
            else:
                is_holiday.append(False)
                holiday_name.append(None)

        return {
            'is_holiday': is_holiday,
            'holiday_name': holiday_name
        }
