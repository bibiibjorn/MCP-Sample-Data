"""
Currency Manager - Multi-currency data generation

Provides:
- ISO 4217 currency dimension generation
- Exchange rate generation with realistic volatility (GBM)
- Multi-currency fact table support
- Currency conversion utilities
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
from datetime import date, datetime, timedelta
from enum import Enum
import math
import random

import polars as pl
import numpy as np


# ISO 4217 Currency Data
ISO_CURRENCIES = {
    'USD': {'name': 'US Dollar', 'symbol': '$', 'decimal_places': 2, 'country': 'United States'},
    'EUR': {'name': 'Euro', 'symbol': '€', 'decimal_places': 2, 'country': 'European Union'},
    'GBP': {'name': 'British Pound', 'symbol': '£', 'decimal_places': 2, 'country': 'United Kingdom'},
    'JPY': {'name': 'Japanese Yen', 'symbol': '¥', 'decimal_places': 0, 'country': 'Japan'},
    'CHF': {'name': 'Swiss Franc', 'symbol': 'CHF', 'decimal_places': 2, 'country': 'Switzerland'},
    'CAD': {'name': 'Canadian Dollar', 'symbol': 'C$', 'decimal_places': 2, 'country': 'Canada'},
    'AUD': {'name': 'Australian Dollar', 'symbol': 'A$', 'decimal_places': 2, 'country': 'Australia'},
    'NZD': {'name': 'New Zealand Dollar', 'symbol': 'NZ$', 'decimal_places': 2, 'country': 'New Zealand'},
    'CNY': {'name': 'Chinese Yuan', 'symbol': '¥', 'decimal_places': 2, 'country': 'China'},
    'INR': {'name': 'Indian Rupee', 'symbol': '₹', 'decimal_places': 2, 'country': 'India'},
    'MXN': {'name': 'Mexican Peso', 'symbol': '$', 'decimal_places': 2, 'country': 'Mexico'},
    'BRL': {'name': 'Brazilian Real', 'symbol': 'R$', 'decimal_places': 2, 'country': 'Brazil'},
    'KRW': {'name': 'South Korean Won', 'symbol': '₩', 'decimal_places': 0, 'country': 'South Korea'},
    'SGD': {'name': 'Singapore Dollar', 'symbol': 'S$', 'decimal_places': 2, 'country': 'Singapore'},
    'HKD': {'name': 'Hong Kong Dollar', 'symbol': 'HK$', 'decimal_places': 2, 'country': 'Hong Kong'},
    'SEK': {'name': 'Swedish Krona', 'symbol': 'kr', 'decimal_places': 2, 'country': 'Sweden'},
    'NOK': {'name': 'Norwegian Krone', 'symbol': 'kr', 'decimal_places': 2, 'country': 'Norway'},
    'DKK': {'name': 'Danish Krone', 'symbol': 'kr', 'decimal_places': 2, 'country': 'Denmark'},
    'ZAR': {'name': 'South African Rand', 'symbol': 'R', 'decimal_places': 2, 'country': 'South Africa'},
    'RUB': {'name': 'Russian Ruble', 'symbol': '₽', 'decimal_places': 2, 'country': 'Russia'},
    'PLN': {'name': 'Polish Zloty', 'symbol': 'zł', 'decimal_places': 2, 'country': 'Poland'},
    'TRY': {'name': 'Turkish Lira', 'symbol': '₺', 'decimal_places': 2, 'country': 'Turkey'},
    'THB': {'name': 'Thai Baht', 'symbol': '฿', 'decimal_places': 2, 'country': 'Thailand'},
    'IDR': {'name': 'Indonesian Rupiah', 'symbol': 'Rp', 'decimal_places': 0, 'country': 'Indonesia'},
    'MYR': {'name': 'Malaysian Ringgit', 'symbol': 'RM', 'decimal_places': 2, 'country': 'Malaysia'},
    'PHP': {'name': 'Philippine Peso', 'symbol': '₱', 'decimal_places': 2, 'country': 'Philippines'},
    'CZK': {'name': 'Czech Koruna', 'symbol': 'Kč', 'decimal_places': 2, 'country': 'Czech Republic'},
    'ILS': {'name': 'Israeli Shekel', 'symbol': '₪', 'decimal_places': 2, 'country': 'Israel'},
    'CLP': {'name': 'Chilean Peso', 'symbol': '$', 'decimal_places': 0, 'country': 'Chile'},
    'AED': {'name': 'UAE Dirham', 'symbol': 'د.إ', 'decimal_places': 2, 'country': 'United Arab Emirates'},
    'SAR': {'name': 'Saudi Riyal', 'symbol': '﷼', 'decimal_places': 2, 'country': 'Saudi Arabia'},
}

# Approximate base rates vs USD (as of a reference point)
BASE_RATES_VS_USD = {
    'USD': 1.0,
    'EUR': 0.92,
    'GBP': 0.79,
    'JPY': 149.5,
    'CHF': 0.88,
    'CAD': 1.36,
    'AUD': 1.53,
    'NZD': 1.64,
    'CNY': 7.24,
    'INR': 83.2,
    'MXN': 17.1,
    'BRL': 4.97,
    'KRW': 1320.0,
    'SGD': 1.34,
    'HKD': 7.82,
    'SEK': 10.5,
    'NOK': 10.7,
    'DKK': 6.88,
    'ZAR': 18.8,
    'RUB': 91.5,
    'PLN': 4.02,
    'TRY': 32.1,
    'THB': 35.5,
    'IDR': 15700.0,
    'MYR': 4.72,
    'PHP': 56.2,
    'CZK': 22.9,
    'ILS': 3.75,
    'CLP': 890.0,
    'AED': 3.67,
    'SAR': 3.75,
}

# Volatility estimates (annual) for currency pairs
CURRENCY_VOLATILITY = {
    'USD': 0.0,    # Base currency
    'EUR': 0.08,
    'GBP': 0.10,
    'JPY': 0.10,
    'CHF': 0.08,
    'CAD': 0.07,
    'AUD': 0.11,
    'NZD': 0.12,
    'CNY': 0.04,   # Managed float
    'INR': 0.06,
    'MXN': 0.13,
    'BRL': 0.16,
    'KRW': 0.09,
    'SGD': 0.05,
    'HKD': 0.01,   # Pegged to USD
    'SEK': 0.10,
    'NOK': 0.11,
    'DKK': 0.02,   # Pegged to EUR
    'ZAR': 0.16,
    'RUB': 0.20,
    'PLN': 0.10,
    'TRY': 0.25,
    'THB': 0.07,
    'IDR': 0.08,
    'MYR': 0.06,
    'PHP': 0.07,
    'CZK': 0.08,
    'ILS': 0.08,
    'CLP': 0.12,
    'AED': 0.01,   # Pegged to USD
    'SAR': 0.01,   # Pegged to USD
}


@dataclass
class CurrencyDimension:
    """Currency dimension record"""
    currency_code: str
    currency_name: str
    symbol: str
    decimal_places: int
    country: str
    is_major: bool = False


class CurrencyDimensionGenerator:
    """Generate currency dimension tables"""

    def __init__(self):
        self.major_currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD']

    def generate(
        self,
        currencies: Optional[List[str]] = None,
        include_all: bool = False
    ) -> pl.DataFrame:
        """
        Generate currency dimension table.

        Args:
            currencies: Specific currencies to include (ISO codes)
            include_all: Include all available currencies

        Returns:
            DataFrame with currency dimension
        """
        if include_all:
            currency_codes = list(ISO_CURRENCIES.keys())
        elif currencies:
            currency_codes = [c.upper() for c in currencies if c.upper() in ISO_CURRENCIES]
        else:
            currency_codes = self.major_currencies

        records = []
        for code in currency_codes:
            info = ISO_CURRENCIES[code]
            records.append({
                'currency_code': code,
                'currency_name': info['name'],
                'symbol': info['symbol'],
                'decimal_places': info['decimal_places'],
                'country': info['country'],
                'is_major': code in self.major_currencies
            })

        return pl.DataFrame(records)


class ExchangeRateGenerator:
    """Generate exchange rate time series with realistic volatility"""

    def __init__(self, seed: Optional[int] = None):
        self.seed = seed
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)

    def generate_rates(
        self,
        base_currency: str,
        target_currencies: List[str],
        start_date: str,
        end_date: str,
        frequency: str = 'daily'
    ) -> pl.DataFrame:
        """
        Generate exchange rates using Geometric Brownian Motion.

        Args:
            base_currency: Base currency code (rates will be X per 1 base)
            target_currencies: List of target currency codes
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            frequency: 'daily', 'weekly', or 'monthly'

        Returns:
            DataFrame with exchange rates
        """
        # Parse dates
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()

        # Generate date range
        dates = []
        current = start
        while current <= end:
            if frequency == 'daily':
                # Skip weekends
                if current.weekday() < 5:
                    dates.append(current)
                current += timedelta(days=1)
            elif frequency == 'weekly':
                dates.append(current)
                current += timedelta(days=7)
            elif frequency == 'monthly':
                dates.append(current)
                # Move to next month
                if current.month == 12:
                    current = date(current.year + 1, 1, 1)
                else:
                    current = date(current.year, current.month + 1, 1)

        n_dates = len(dates)

        # Calculate time step (in years)
        if frequency == 'daily':
            dt = 1 / 252  # Trading days
        elif frequency == 'weekly':
            dt = 1 / 52
        else:
            dt = 1 / 12

        # Generate rates for each currency pair
        all_data = {'date': dates}

        for target in target_currencies:
            if target == base_currency:
                all_data[f'{base_currency}_{target}'] = [1.0] * n_dates
                continue

            # Get base rate
            if base_currency == 'USD':
                initial_rate = BASE_RATES_VS_USD.get(target, 1.0)
            elif target == 'USD':
                initial_rate = 1 / BASE_RATES_VS_USD.get(base_currency, 1.0)
            else:
                # Cross rate
                base_to_usd = BASE_RATES_VS_USD.get(base_currency, 1.0)
                target_to_usd = BASE_RATES_VS_USD.get(target, 1.0)
                initial_rate = target_to_usd / base_to_usd

            # Get volatility (combined for cross rates)
            vol_base = CURRENCY_VOLATILITY.get(base_currency, 0.1)
            vol_target = CURRENCY_VOLATILITY.get(target, 0.1)
            sigma = math.sqrt(vol_base**2 + vol_target**2)

            # Generate GBM path
            # dS = S * (mu * dt + sigma * sqrt(dt) * Z)
            mu = 0  # Assume no drift (random walk)
            rates = [initial_rate]

            for _ in range(n_dates - 1):
                z = np.random.normal(0, 1)
                drift = mu * dt
                diffusion = sigma * math.sqrt(dt) * z
                new_rate = rates[-1] * math.exp(drift + diffusion)
                rates.append(new_rate)

            # Round appropriately
            decimal_places = ISO_CURRENCIES.get(target, {}).get('decimal_places', 2)
            if initial_rate > 100:
                rates = [round(r, 2) for r in rates]
            elif initial_rate > 10:
                rates = [round(r, 3) for r in rates]
            else:
                rates = [round(r, 4) for r in rates]

            all_data[f'{base_currency}_{target}'] = rates

        return pl.DataFrame(all_data)

    def generate_triangular_rates(
        self,
        currencies: List[str],
        start_date: str,
        end_date: str,
        frequency: str = 'daily'
    ) -> pl.DataFrame:
        """
        Generate exchange rate matrix for all currency pairs.

        Args:
            currencies: List of currency codes
            start_date: Start date
            end_date: End date
            frequency: Rate frequency

        Returns:
            DataFrame with all cross rates
        """
        # Use USD as the pivot currency
        usd_rates = self.generate_rates(
            base_currency='USD',
            target_currencies=currencies,
            start_date=start_date,
            end_date=end_date,
            frequency=frequency
        )

        # Calculate cross rates
        result_data = {'date': usd_rates['date'].to_list()}

        for base in currencies:
            for target in currencies:
                if base == target:
                    continue

                col_name = f'{base}_{target}'

                if base == 'USD':
                    result_data[col_name] = usd_rates[f'USD_{target}'].to_list()
                elif target == 'USD':
                    base_rates = usd_rates[f'USD_{base}'].to_list()
                    result_data[col_name] = [1 / r if r != 0 else None for r in base_rates]
                else:
                    base_rates = usd_rates[f'USD_{base}'].to_list()
                    target_rates = usd_rates[f'USD_{target}'].to_list()
                    result_data[col_name] = [
                        round(t / b, 4) if b != 0 else None
                        for b, t in zip(base_rates, target_rates)
                    ]

        return pl.DataFrame(result_data)


class MultiCurrencyFactGenerator:
    """Generate fact tables with multi-currency support"""

    def __init__(self, seed: Optional[int] = None):
        self.seed = seed
        self.rate_generator = ExchangeRateGenerator(seed)
        if seed is not None:
            random.seed(seed)
            np.random.seed(seed)

    def generate(
        self,
        row_count: int,
        transaction_currencies: List[str],
        reporting_currency: str,
        start_date: str,
        end_date: str,
        amount_config: Optional[Dict[str, Any]] = None,
        include_fx_details: bool = True
    ) -> pl.DataFrame:
        """
        Generate fact table with transaction and reporting currency amounts.

        Args:
            row_count: Number of rows
            transaction_currencies: Possible transaction currencies
            reporting_currency: Reporting/functional currency
            start_date: Date range start
            end_date: Date range end
            amount_config: Configuration for amount generation
            include_fx_details: Include exchange rate details

        Returns:
            DataFrame with multi-currency fact data
        """
        # Generate exchange rates for the period
        all_currencies = list(set(transaction_currencies + [reporting_currency]))
        rates_df = self.rate_generator.generate_rates(
            base_currency=reporting_currency,
            target_currencies=all_currencies,
            start_date=start_date,
            end_date=end_date
        )

        # Convert to lookup dict: {date: {currency: rate}}
        rates_lookup = {}
        rate_dates = rates_df['date'].to_list()
        for i, d in enumerate(rate_dates):
            rates_lookup[d] = {}
            for curr in all_currencies:
                col = f'{reporting_currency}_{curr}'
                if col in rates_df.columns:
                    rates_lookup[d][curr] = rates_df[col][i]
                elif curr == reporting_currency:
                    rates_lookup[d][curr] = 1.0

        # Generate transaction dates (randomly within range)
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
        date_range = (end - start).days

        # Amount configuration
        if amount_config is None:
            amount_config = {
                'mean': 1000,
                'std': 500,
                'min': 10,
                'max': 50000
            }

        # Generate data
        data = {
            'transaction_id': list(range(1, row_count + 1)),
            'transaction_date': [],
            'transaction_currency': [],
            'transaction_amount': [],
            'exchange_rate': [],
            'reporting_currency': [reporting_currency] * row_count,
            'reporting_amount': []
        }

        if include_fx_details:
            data['fx_gain_loss'] = []
            data['rate_date'] = []

        for i in range(row_count):
            # Random date
            days_offset = random.randint(0, date_range)
            tx_date = start + timedelta(days=days_offset)
            data['transaction_date'].append(tx_date)

            # Random transaction currency
            tx_currency = random.choice(transaction_currencies)
            data['transaction_currency'].append(tx_currency)

            # Random amount
            amount = random.gauss(amount_config['mean'], amount_config['std'])
            amount = max(amount_config.get('min', 0), min(amount_config.get('max', float('inf')), amount))
            decimal_places = ISO_CURRENCIES.get(tx_currency, {}).get('decimal_places', 2)
            amount = round(amount, decimal_places)
            data['transaction_amount'].append(amount)

            # Get exchange rate (find closest available date)
            rate_date = tx_date
            while rate_date not in rates_lookup and rate_date >= start:
                rate_date -= timedelta(days=1)

            if rate_date in rates_lookup:
                rate = rates_lookup[rate_date].get(tx_currency, 1.0)
            else:
                # Fallback to base rate
                rate = BASE_RATES_VS_USD.get(tx_currency, 1.0) / BASE_RATES_VS_USD.get(reporting_currency, 1.0)

            # For reporting amount, we need the inverse
            # If rate is "reporting per transaction", multiply
            # If rate is "transaction per reporting", divide
            # Our rates are base per target, so:
            if tx_currency == reporting_currency:
                reporting_amount = amount
                rate = 1.0
            else:
                # rate is reporting currency per 1 transaction currency
                # So to convert transaction to reporting: divide by rate
                reporting_amount = amount / rate if rate != 0 else amount

            reporting_decimal = ISO_CURRENCIES.get(reporting_currency, {}).get('decimal_places', 2)
            reporting_amount = round(reporting_amount, reporting_decimal)

            data['exchange_rate'].append(round(rate, 6))
            data['reporting_amount'].append(reporting_amount)

            if include_fx_details:
                data['rate_date'].append(rate_date)
                # Simplified FX gain/loss (would need settlement date for real calculation)
                data['fx_gain_loss'].append(0.0)

        return pl.DataFrame(data)


def generate_currency_dimension(
    currencies: Optional[List[str]] = None,
    include_all: bool = False
) -> Dict[str, Any]:
    """Helper function to generate currency dimension"""
    generator = CurrencyDimensionGenerator()
    df = generator.generate(currencies, include_all)
    return {
        'success': True,
        'df': df,
        'row_count': len(df),
        'columns': df.columns
    }


def generate_exchange_rates(
    base_currency: str,
    target_currencies: List[str],
    start_date: str,
    end_date: str,
    frequency: str = 'daily',
    seed: Optional[int] = None
) -> Dict[str, Any]:
    """Helper function to generate exchange rates"""
    generator = ExchangeRateGenerator(seed)
    df = generator.generate_rates(
        base_currency, target_currencies, start_date, end_date, frequency
    )
    return {
        'success': True,
        'df': df,
        'row_count': len(df),
        'columns': df.columns
    }


def generate_multicurrency_fact(
    row_count: int,
    transaction_currencies: List[str],
    reporting_currency: str,
    start_date: str,
    end_date: str,
    seed: Optional[int] = None
) -> Dict[str, Any]:
    """Helper function to generate multi-currency fact"""
    generator = MultiCurrencyFactGenerator(seed)
    df = generator.generate(
        row_count=row_count,
        transaction_currencies=transaction_currencies,
        reporting_currency=reporting_currency,
        start_date=start_date,
        end_date=end_date
    )
    return {
        'success': True,
        'df': df,
        'row_count': len(df),
        'columns': df.columns
    }
