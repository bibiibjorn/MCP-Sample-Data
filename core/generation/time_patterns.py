"""
Time Pattern Engine - Generate time-series data with realistic patterns

Supports:
- Seasonal patterns (monthly, weekly, daily)
- Trend components (linear, exponential)
- Cyclical patterns (business cycles)
- Special events (holidays, promotions)
- Random noise with configurable variance
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import date, datetime, timedelta
from enum import Enum
import math
import random
import yaml
from pathlib import Path

import polars as pl


class TrendType(Enum):
    """Types of trend patterns"""
    NONE = "none"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    LOGARITHMIC = "logarithmic"
    POLYNOMIAL = "polynomial"


class SeasonalityType(Enum):
    """Types of seasonality"""
    NONE = "none"
    MONTHLY = "monthly"
    WEEKLY = "weekly"
    DAILY = "daily"
    QUARTERLY = "quarterly"


@dataclass
class SpecialEvent:
    """Definition of a special event that affects values"""
    name: str
    date_spec: str  # ISO date or rule like 'fourth_thursday_november+1'
    multiplier: float = 1.5
    duration_days: int = 1

    def get_dates(self, year: int) -> List[date]:
        """Get actual dates for this event in a given year"""
        dates = []

        if self.date_spec.startswith('last_'):
            # last_monday_may -> Memorial Day
            parts = self.date_spec.split('_')
            weekday_name = parts[1]
            month_name = parts[2]
            dates.append(self._get_last_weekday_of_month(year, month_name, weekday_name))

        elif self.date_spec.startswith('first_') or self.date_spec.startswith('second_') or \
             self.date_spec.startswith('third_') or self.date_spec.startswith('fourth_'):
            # fourth_thursday_november -> Thanksgiving
            parts = self.date_spec.split('_')
            ordinal = parts[0]
            weekday_name = parts[1]
            month_name = parts[2]

            # Check for offset like +1
            offset = 0
            if '+' in month_name:
                month_name, offset_str = month_name.split('+')
                offset = int(offset_str)
            elif '-' in month_name:
                month_name, offset_str = month_name.split('-')
                offset = -int(offset_str)

            event_date = self._get_nth_weekday_of_month(year, month_name, weekday_name, ordinal)
            if event_date:
                dates.append(event_date + timedelta(days=offset))
        else:
            # Fixed date like 12-25 or 2024-12-25
            try:
                if len(self.date_spec) == 5:  # MM-DD
                    month, day = map(int, self.date_spec.split('-'))
                    dates.append(date(year, month, day))
                else:  # Full date
                    dates.append(datetime.strptime(self.date_spec, '%Y-%m-%d').date())
            except ValueError:
                pass

        # Extend for duration
        all_dates = []
        for d in dates:
            for i in range(self.duration_days):
                all_dates.append(d + timedelta(days=i))

        return all_dates

    def _get_last_weekday_of_month(self, year: int, month_name: str, weekday_name: str) -> date:
        """Get the last occurrence of a weekday in a month"""
        month_map = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        weekday_map = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }

        month = month_map.get(month_name.lower(), 1)
        target_weekday = weekday_map.get(weekday_name.lower(), 0)

        # Start from last day of month
        if month == 12:
            last_day = date(year + 1, 1, 1) - timedelta(days=1)
        else:
            last_day = date(year, month + 1, 1) - timedelta(days=1)

        # Find last occurrence of target weekday
        while last_day.weekday() != target_weekday:
            last_day -= timedelta(days=1)

        return last_day

    def _get_nth_weekday_of_month(self, year: int, month_name: str, weekday_name: str, ordinal: str) -> Optional[date]:
        """Get the nth occurrence of a weekday in a month"""
        month_map = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        weekday_map = {
            'monday': 0, 'tuesday': 1, 'wednesday': 2, 'thursday': 3,
            'friday': 4, 'saturday': 5, 'sunday': 6
        }
        ordinal_map = {'first': 1, 'second': 2, 'third': 3, 'fourth': 4, 'fifth': 5}

        month = month_map.get(month_name.lower(), 1)
        target_weekday = weekday_map.get(weekday_name.lower(), 0)
        n = ordinal_map.get(ordinal.lower(), 1)

        # Start from first day of month
        first_day = date(year, month, 1)

        # Find first occurrence of target weekday
        days_ahead = target_weekday - first_day.weekday()
        if days_ahead < 0:
            days_ahead += 7
        first_occurrence = first_day + timedelta(days=days_ahead)

        # Get nth occurrence
        nth_occurrence = first_occurrence + timedelta(weeks=n-1)

        # Verify it's still in the same month
        if nth_occurrence.month != month:
            return None

        return nth_occurrence


@dataclass
class TimePattern:
    """Complete time pattern configuration"""
    name: str
    description: str = ""

    # Trend configuration
    trend_type: TrendType = TrendType.NONE
    trend_slope: float = 0.0  # For linear: units per day
    trend_rate: float = 0.0   # For exponential: growth rate

    # Seasonality configuration
    monthly_weights: Dict[int, float] = field(default_factory=dict)
    weekly_weights: Dict[str, float] = field(default_factory=dict)
    daily_weights: Dict[int, float] = field(default_factory=dict)  # Hour of day
    quarterly_weights: Dict[int, float] = field(default_factory=dict)

    # Special events
    special_events: List[SpecialEvent] = field(default_factory=list)

    # Noise configuration
    noise_std: float = 0.05  # Standard deviation as fraction of value
    noise_min_factor: float = 0.5  # Minimum multiplier from noise
    noise_max_factor: float = 1.5  # Maximum multiplier from noise

    # Value constraints
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    round_to: Optional[int] = None  # Decimal places

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TimePattern':
        """Create pattern from dictionary"""
        # Parse special events
        events = []
        for event_data in data.get('special_events', []):
            events.append(SpecialEvent(
                name=event_data['name'],
                date_spec=event_data['date'],
                multiplier=event_data.get('multiplier', 1.5),
                duration_days=event_data.get('duration_days', 1)
            ))

        # Parse weekly weights (convert day names to proper format)
        weekly = data.get('day_of_week', data.get('weekly_weights', {}))

        return cls(
            name=data.get('name', 'unnamed'),
            description=data.get('description', ''),
            trend_type=TrendType(data.get('trend_type', 'none')),
            trend_slope=data.get('trend_slope', 0.0),
            trend_rate=data.get('trend_rate', 0.0),
            monthly_weights=data.get('monthly_weights', {}),
            weekly_weights=weekly,
            daily_weights=data.get('daily_weights', {}),
            quarterly_weights=data.get('quarterly_weights', {}),
            special_events=events,
            noise_std=data.get('noise_std', 0.05),
            noise_min_factor=data.get('noise_min_factor', 0.5),
            noise_max_factor=data.get('noise_max_factor', 1.5),
            min_value=data.get('min_value'),
            max_value=data.get('max_value'),
            round_to=data.get('round_to')
        )


class TimePatternEngine:
    """Generate time-series data with realistic patterns"""

    # Default patterns
    DEFAULT_PATTERNS = {
        'retail_seasonal': TimePattern(
            name='retail_seasonal',
            description='Retail sales pattern with holiday peaks',
            monthly_weights={
                1: 0.85, 2: 0.80, 3: 0.90, 4: 0.95,
                5: 1.00, 6: 0.95, 7: 0.90, 8: 0.95,
                9: 1.00, 10: 1.05, 11: 1.40, 12: 1.60
            },
            weekly_weights={
                'monday': 0.85, 'tuesday': 0.90, 'wednesday': 0.95,
                'thursday': 1.00, 'friday': 1.15, 'saturday': 1.20, 'sunday': 0.95
            },
            special_events=[
                SpecialEvent('Black Friday', 'fourth_thursday_november+1', 3.5, 1),
                SpecialEvent('Cyber Monday', 'fourth_thursday_november+4', 2.5, 1),
                SpecialEvent('Christmas Eve', '12-24', 2.0, 1),
                SpecialEvent('Boxing Day', '12-26', 1.8, 1),
                SpecialEvent('New Year Sale', '01-01', 1.5, 3),
                SpecialEvent('Valentines', '02-14', 1.4, 1),
                SpecialEvent('Memorial Day', 'last_monday_may', 1.3, 3),
                SpecialEvent('July 4th', '07-04', 1.3, 1),
                SpecialEvent('Labor Day', 'first_monday_september', 1.3, 3)
            ],
            noise_std=0.10
        ),
        'financial_quarterly': TimePattern(
            name='financial_quarterly',
            description='Financial pattern with quarter-end spikes',
            quarterly_weights={1: 0.90, 2: 0.95, 3: 1.05, 4: 1.10},
            monthly_weights={
                1: 0.85, 2: 0.90, 3: 1.25,  # Q1 end
                4: 0.85, 5: 0.90, 6: 1.25,  # Q2 end
                7: 0.85, 8: 0.90, 9: 1.25,  # Q3 end
                10: 0.90, 11: 0.95, 12: 1.30  # Q4/Year end
            },
            weekly_weights={
                'monday': 1.10, 'tuesday': 1.05, 'wednesday': 1.00,
                'thursday': 0.95, 'friday': 0.90, 'saturday': 0.0, 'sunday': 0.0
            },
            noise_std=0.08
        ),
        'manufacturing_shift': TimePattern(
            name='manufacturing_shift',
            description='Manufacturing with shift patterns',
            daily_weights={
                6: 0.3, 7: 0.8, 8: 1.0, 9: 1.0, 10: 1.0, 11: 0.9,
                12: 0.7, 13: 0.9, 14: 1.0, 15: 1.0, 16: 0.9, 17: 0.8,
                18: 0.6, 19: 0.4, 20: 0.3, 21: 0.2, 22: 0.1
            },
            weekly_weights={
                'monday': 1.0, 'tuesday': 1.0, 'wednesday': 1.0,
                'thursday': 1.0, 'friday': 0.9, 'saturday': 0.3, 'sunday': 0.0
            },
            noise_std=0.05
        ),
        'ecommerce_daily': TimePattern(
            name='ecommerce_daily',
            description='E-commerce with daily patterns',
            daily_weights={
                0: 0.2, 1: 0.1, 2: 0.1, 3: 0.1, 4: 0.1, 5: 0.2,
                6: 0.4, 7: 0.6, 8: 0.8, 9: 1.0, 10: 1.1, 11: 1.2,
                12: 1.3, 13: 1.2, 14: 1.1, 15: 1.0, 16: 0.9, 17: 0.8,
                18: 0.9, 19: 1.0, 20: 1.2, 21: 1.3, 22: 1.0, 23: 0.5
            },
            weekly_weights={
                'monday': 1.1, 'tuesday': 1.0, 'wednesday': 0.95,
                'thursday': 1.0, 'friday': 0.9, 'saturday': 0.8, 'sunday': 1.15
            },
            noise_std=0.12
        ),
        'healthcare_weekly': TimePattern(
            name='healthcare_weekly',
            description='Healthcare visits pattern',
            weekly_weights={
                'monday': 1.3, 'tuesday': 1.1, 'wednesday': 1.0,
                'thursday': 1.0, 'friday': 0.9, 'saturday': 0.5, 'sunday': 0.2
            },
            monthly_weights={
                1: 1.2, 2: 1.1, 3: 1.0, 4: 0.9,  # Flu season
                5: 0.85, 6: 0.80, 7: 0.75, 8: 0.80,
                9: 0.95, 10: 1.0, 11: 1.1, 12: 1.15
            },
            noise_std=0.15
        ),
        'linear_growth': TimePattern(
            name='linear_growth',
            description='Steady linear growth pattern',
            trend_type=TrendType.LINEAR,
            trend_slope=0.001,  # 0.1% daily growth
            noise_std=0.05
        ),
        'exponential_growth': TimePattern(
            name='exponential_growth',
            description='Exponential growth pattern',
            trend_type=TrendType.EXPONENTIAL,
            trend_rate=0.0003,  # ~12% annual growth
            noise_std=0.08
        )
    }

    def __init__(self, patterns_dir: Optional[str] = None):
        """Initialize with optional custom patterns directory"""
        self.patterns = dict(self.DEFAULT_PATTERNS)

        if patterns_dir:
            self._load_patterns_from_dir(patterns_dir)

    def _load_patterns_from_dir(self, patterns_dir: str):
        """Load custom patterns from YAML files"""
        path = Path(patterns_dir)
        if not path.exists():
            return

        for yaml_file in path.glob('*.yaml'):
            try:
                with open(yaml_file, 'r') as f:
                    data = yaml.safe_load(f)

                if isinstance(data, dict):
                    for pattern_name, pattern_data in data.items():
                        if isinstance(pattern_data, dict):
                            pattern_data['name'] = pattern_name
                            self.patterns[pattern_name] = TimePattern.from_dict(pattern_data)
            except Exception:
                pass  # Skip invalid files

    def list_patterns(self) -> List[Dict[str, str]]:
        """List available patterns"""
        return [
            {'name': name, 'description': pattern.description}
            for name, pattern in self.patterns.items()
        ]

    def generate(
        self,
        pattern: Union[str, TimePattern],
        start_date: Union[str, date],
        end_date: Union[str, date],
        base_value: float = 100.0,
        row_count: Optional[int] = None,
        seed: Optional[int] = None
    ) -> Tuple[List[date], List[float]]:
        """
        Generate time series data following the specified pattern.

        Args:
            pattern: Pattern name or TimePattern object
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            base_value: Base value around which to generate
            row_count: If specified, sample this many dates from the range
            seed: Random seed for reproducibility

        Returns:
            Tuple of (dates, values)
        """
        if seed is not None:
            random.seed(seed)

        # Get pattern object
        if isinstance(pattern, str):
            if pattern not in self.patterns:
                raise ValueError(f"Unknown pattern: {pattern}. Available: {list(self.patterns.keys())}")
            pattern_obj = self.patterns[pattern]
        else:
            pattern_obj = pattern

        # Parse dates
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        # Generate date range
        all_dates = []
        current = start_date
        while current <= end_date:
            all_dates.append(current)
            current += timedelta(days=1)

        # Sample if row_count specified
        if row_count and row_count < len(all_dates):
            all_dates = sorted(random.sample(all_dates, row_count))
        elif row_count and row_count > len(all_dates):
            # Allow duplicates for higher row counts
            all_dates = sorted(random.choices(all_dates, k=row_count))

        # Pre-compute special event dates
        years = set(d.year for d in all_dates)
        event_dates = {}  # date -> multiplier
        for event in pattern_obj.special_events:
            for year in years:
                for event_date in event.get_dates(year):
                    if event_date in event_dates:
                        event_dates[event_date] = max(event_dates[event_date], event.multiplier)
                    else:
                        event_dates[event_date] = event.multiplier

        # Generate values
        values = []
        for i, d in enumerate(all_dates):
            value = self._calculate_value(
                d, i, len(all_dates), base_value, pattern_obj, event_dates, start_date
            )
            values.append(value)

        return all_dates, values

    def _calculate_value(
        self,
        d: date,
        index: int,
        total_points: int,
        base_value: float,
        pattern: TimePattern,
        event_dates: Dict[date, float],
        start_date: date
    ) -> float:
        """Calculate value for a specific date"""
        value = base_value

        # Apply trend
        days_from_start = (d - start_date).days
        if pattern.trend_type == TrendType.LINEAR:
            value += pattern.trend_slope * days_from_start * base_value
        elif pattern.trend_type == TrendType.EXPONENTIAL:
            value *= math.exp(pattern.trend_rate * days_from_start)
        elif pattern.trend_type == TrendType.LOGARITHMIC:
            value *= (1 + pattern.trend_rate * math.log(days_from_start + 1))

        # Apply monthly seasonality
        if pattern.monthly_weights:
            month_weight = pattern.monthly_weights.get(d.month, 1.0)
            value *= month_weight

        # Apply quarterly seasonality
        if pattern.quarterly_weights:
            quarter = (d.month - 1) // 3 + 1
            quarter_weight = pattern.quarterly_weights.get(quarter, 1.0)
            value *= quarter_weight

        # Apply weekly seasonality
        if pattern.weekly_weights:
            day_names = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
            day_name = day_names[d.weekday()]
            day_weight = pattern.weekly_weights.get(day_name, 1.0)
            value *= day_weight

        # Apply special events
        if d in event_dates:
            value *= event_dates[d]

        # Apply noise
        if pattern.noise_std > 0:
            noise_factor = random.gauss(1.0, pattern.noise_std)
            noise_factor = max(pattern.noise_min_factor, min(pattern.noise_max_factor, noise_factor))
            value *= noise_factor

        # Apply constraints
        if pattern.min_value is not None:
            value = max(pattern.min_value, value)
        if pattern.max_value is not None:
            value = min(pattern.max_value, value)
        if pattern.round_to is not None:
            value = round(value, pattern.round_to)

        return value

    def generate_dataframe(
        self,
        pattern: Union[str, TimePattern],
        start_date: Union[str, date],
        end_date: Union[str, date],
        base_value: float = 100.0,
        row_count: Optional[int] = None,
        date_column: str = 'date',
        value_column: str = 'value',
        seed: Optional[int] = None
    ) -> pl.DataFrame:
        """Generate time series as a Polars DataFrame"""
        dates, values = self.generate(
            pattern, start_date, end_date, base_value, row_count, seed
        )

        return pl.DataFrame({
            date_column: dates,
            value_column: values
        })

    def apply_pattern_to_dataframe(
        self,
        df: pl.DataFrame,
        date_column: str,
        value_column: str,
        pattern: Union[str, TimePattern],
        base_value: Optional[float] = None,
        seed: Optional[int] = None
    ) -> pl.DataFrame:
        """Apply pattern to existing dataframe's value column based on dates"""
        if seed is not None:
            random.seed(seed)

        # Get pattern object
        if isinstance(pattern, str):
            if pattern not in self.patterns:
                raise ValueError(f"Unknown pattern: {pattern}")
            pattern_obj = self.patterns[pattern]
        else:
            pattern_obj = pattern

        # Get dates and calculate base value if not provided
        dates = df[date_column].to_list()
        if base_value is None:
            base_value = df[value_column].mean()

        # Pre-compute event dates
        years = set(d.year for d in dates if d is not None)
        event_dates = {}
        for event in pattern_obj.special_events:
            for year in years:
                for event_date in event.get_dates(year):
                    if event_date in event_dates:
                        event_dates[event_date] = max(event_dates[event_date], event.multiplier)
                    else:
                        event_dates[event_date] = event.multiplier

        start_date = min(d for d in dates if d is not None)

        # Generate new values
        new_values = []
        for i, d in enumerate(dates):
            if d is None:
                new_values.append(None)
            else:
                value = self._calculate_value(
                    d, i, len(dates), base_value, pattern_obj, event_dates, start_date
                )
                new_values.append(value)

        # Return dataframe with modified value column
        return df.with_columns(pl.Series(name=value_column, values=new_values))


# Convenience function for direct pattern generation
def generate_time_series(
    pattern: str,
    start_date: str,
    end_date: str,
    base_value: float = 100.0,
    row_count: Optional[int] = None,
    seed: Optional[int] = None
) -> Tuple[List[date], List[float]]:
    """Convenience function to generate time series"""
    engine = TimePatternEngine()
    return engine.generate(pattern, start_date, end_date, base_value, row_count, seed)
