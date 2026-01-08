# MCP Sample Data Server - Enhancement Plan

> **Document Version**: 1.0
> **Created**: 2026-01-07
> **Status**: Planning

---

## Table of Contents

1. [Time Series with Realistic Patterns](#1-time-series-with-realistic-patterns)
2. [Correlated Data Generation](#2-correlated-data-generation)
3. [Multi-Currency & Exchange Rates](#3-multi-currency--exchange-rates)
4. [PII Detection & Anonymization](#4-pii-detection--anonymization)
5. [Data Subsetting](#5-data-subsetting)
6. [Industry-Specific Star Schemas](#6-industry-specific-star-schemas)
7. [Data Quality Scoring](#7-data-quality-scoring)
8. [Implementation Roadmap](#8-implementation-roadmap)

---

## 1. Time Series with Realistic Patterns

### 1.1 Overview

Add temporal realism to fact table generation by incorporating seasonality, trends, and business-specific patterns that mirror real-world data behavior.

### 1.2 Core Components

#### 1.2.1 Time Pattern Engine

**New file**: `core/generation/time_patterns.py`

```python
class TimePatternEngine:
    """Generates realistic temporal patterns for fact data."""

    def __init__(self, config: TimePatternConfig):
        self.trend = config.trend           # growth, decline, flat, cyclical
        self.seasonality = config.seasonality  # yearly, quarterly, monthly, weekly, daily
        self.noise_level = config.noise_level  # 0.0 - 1.0
        self.holiday_effects = config.holiday_effects
        self.business_hours = config.business_hours
```

#### 1.2.2 Pattern Types

| Pattern | Description | Parameters |
|---------|-------------|------------|
| **Trend** | Long-term direction | `type`: growth/decline/flat, `rate`: % per period |
| **Yearly Seasonality** | Annual cycles | `peaks`: [month_numbers], `amplitude`: 0.0-1.0 |
| **Weekly Seasonality** | Day-of-week patterns | `weights`: {mon: 0.8, tue: 1.0, ...} |
| **Holiday Effects** | Spikes around holidays | `holidays`: list, `effect_days`: before/after |
| **Business Hours** | Intraday patterns | `peak_hours`: [9-12, 14-17], `weekend_factor`: 0.3 |

#### 1.2.3 Predefined Patterns

```yaml
# config/time_patterns/retail_patterns.yaml
retail_sales:
  trend:
    type: growth
    annual_rate: 0.05
  seasonality:
    yearly:
      peaks: [11, 12]  # Nov-Dec holiday season
      amplitude: 0.4
    weekly:
      weights: {mon: 0.7, tue: 0.8, wed: 0.9, thu: 1.0, fri: 1.2, sat: 1.4, sun: 0.6}
  holidays:
    - name: black_friday
      effect: 3.0
      days_before: 0
      days_after: 3
    - name: christmas
      effect: 2.5
      days_before: 14
      days_after: 0
  noise:
    level: 0.15
    distribution: normal
```

### 1.3 Implementation Details

#### 1.3.1 Pattern Application Algorithm

```python
def apply_time_pattern(base_value: float, date: datetime, pattern: TimePattern) -> float:
    """Apply temporal patterns to a base value."""
    multiplier = 1.0

    # Apply trend
    days_from_start = (date - pattern.start_date).days
    if pattern.trend.type == "growth":
        multiplier *= (1 + pattern.trend.daily_rate) ** days_from_start

    # Apply yearly seasonality
    day_of_year = date.timetuple().tm_yday
    yearly_factor = pattern.yearly_seasonality.get_factor(day_of_year)
    multiplier *= yearly_factor

    # Apply weekly seasonality
    day_of_week = date.weekday()
    weekly_factor = pattern.weekly_weights[day_of_week]
    multiplier *= weekly_factor

    # Apply holiday effects
    for holiday in pattern.holidays:
        if holiday.is_in_effect(date):
            multiplier *= holiday.effect

    # Apply noise
    noise = np.random.normal(0, pattern.noise_level)
    multiplier *= (1 + noise)

    return base_value * multiplier
```

#### 1.3.2 Date Distribution Generation

```python
def generate_date_distribution(
    start_date: date,
    end_date: date,
    row_count: int,
    pattern: TimePattern
) -> List[date]:
    """Generate dates with realistic distribution based on pattern."""

    # Calculate daily weights based on pattern
    date_range = pd.date_range(start_date, end_date)
    weights = [pattern.get_weight(d) for d in date_range]

    # Normalize weights to probabilities
    probabilities = np.array(weights) / sum(weights)

    # Sample dates according to probabilities
    date_indices = np.random.choice(
        len(date_range),
        size=row_count,
        p=probabilities
    )

    return [date_range[i] for i in date_indices]
```

### 1.4 New MCP Tools

#### Tool: `02_generate_fact_timeseries`

```json
{
  "name": "02_generate_fact_timeseries",
  "description": "Generate fact table with realistic time patterns",
  "parameters": {
    "fact_type": "sales|finance|inventory|hr|transactions",
    "start_date": "YYYY-MM-DD",
    "end_date": "YYYY-MM-DD",
    "row_count": 100000,
    "time_pattern": "retail_sales|financial_monthly|steady|custom",
    "custom_pattern": {
      "trend": {"type": "growth", "rate": 0.05},
      "seasonality": {"yearly_peaks": [11, 12]},
      "noise_level": 0.1
    },
    "dimensions": {"customer_id": "path/to/customers.csv"}
  }
}
```

### 1.5 File Structure

```
core/generation/
├── time_patterns.py          # TimePatternEngine class
├── pattern_library.py        # Predefined pattern templates
└── date_distributor.py       # Date sampling with patterns

config/time_patterns/
├── retail_patterns.yaml
├── financial_patterns.yaml
├── manufacturing_patterns.yaml
└── healthcare_patterns.yaml
```

### 1.6 Testing Considerations

- Verify seasonal peaks occur at expected times
- Validate trend direction matches configuration
- Ensure holiday effects apply to correct date ranges
- Test noise levels produce expected variance
- Verify date distribution matches pattern weights

---

## 2. Correlated Data Generation

### 2.1 Overview

Generate columns with realistic statistical correlations rather than independent random values, creating more believable test data.

### 2.2 Core Components

#### 2.2.1 Correlation Engine

**New file**: `core/generation/correlation_engine.py`

```python
class CorrelationEngine:
    """Manages inter-column correlations during data generation."""

    def __init__(self):
        self.correlation_rules: List[CorrelationRule] = []
        self.copula_model: Optional[GaussianCopula] = None

    def add_rule(self, rule: CorrelationRule):
        """Add a correlation rule between columns."""
        self.correlation_rules.append(rule)

    def generate_correlated(self, base_data: pl.DataFrame) -> pl.DataFrame:
        """Apply correlation rules to generate dependent columns."""
        pass
```

#### 2.2.2 Correlation Types

| Type | Description | Example |
|------|-------------|---------|
| **Linear** | Direct proportional relationship | quantity → total_amount |
| **Inverse** | Negative correlation | discount_pct ↑ → margin ↓ |
| **Categorical** | Value depends on category | region → shipping_cost range |
| **Conditional** | Rules based on conditions | IF premium_customer THEN higher_discount |
| **Derived** | Calculated from other columns | total = quantity × unit_price |
| **Distribution Shift** | Category affects distribution | product_category → price_range |

#### 2.2.3 Correlation Rule Definition

```python
@dataclass
class CorrelationRule:
    source_column: str
    target_column: str
    correlation_type: str  # linear, inverse, categorical, conditional, derived
    parameters: Dict[str, Any]

# Examples:
rules = [
    # Linear correlation: higher quantity = higher discount
    CorrelationRule(
        source_column="quantity",
        target_column="discount_pct",
        correlation_type="linear",
        parameters={"correlation": 0.6, "noise": 0.2}
    ),

    # Categorical correlation: region affects shipping
    CorrelationRule(
        source_column="region",
        target_column="shipping_cost",
        correlation_type="categorical",
        parameters={
            "mappings": {
                "North": {"mean": 15, "std": 3},
                "South": {"mean": 12, "std": 2},
                "West": {"mean": 20, "std": 5},
                "East": {"mean": 18, "std": 4}
            }
        }
    ),

    # Conditional correlation
    CorrelationRule(
        source_column="customer_segment",
        target_column="payment_terms",
        correlation_type="conditional",
        parameters={
            "conditions": [
                {"if": "Premium", "then": {"choices": ["Net60", "Net90"], "weights": [0.3, 0.7]}},
                {"if": "Standard", "then": {"choices": ["Net30", "Net60"], "weights": [0.7, 0.3]}},
                {"if": "Basic", "then": {"choices": ["Prepaid", "Net30"], "weights": [0.5, 0.5]}}
            ]
        }
    ),

    # Derived calculation
    CorrelationRule(
        source_column=["quantity", "unit_price", "discount_pct"],
        target_column="total_amount",
        correlation_type="derived",
        parameters={"formula": "quantity * unit_price * (1 - discount_pct / 100)"}
    )
]
```

### 2.3 Predefined Correlation Templates

```yaml
# config/correlations/sales_correlations.yaml
sales_fact:
  correlations:
    # Volume discounts
    - source: quantity
      target: discount_pct
      type: tiered
      tiers:
        - range: [1, 10]
          discount_range: [0, 5]
        - range: [11, 50]
          discount_range: [5, 10]
        - range: [51, 100]
          discount_range: [10, 15]
        - range: [101, null]
          discount_range: [15, 25]

    # Premium products = higher unit price
    - source: product_category
      target: unit_price
      type: categorical
      mappings:
        Premium: {mean: 500, std: 100, min: 200}
        Standard: {mean: 100, std: 30, min: 20}
        Budget: {mean: 25, std: 10, min: 5}

    # Customer segment affects order patterns
    - source: customer_segment
      target: quantity
      type: categorical
      mappings:
        Enterprise: {mean: 50, std: 20}
        SMB: {mean: 15, std: 8}
        Consumer: {mean: 3, std: 2}

    # Derived fields
    - sources: [quantity, unit_price, discount_pct]
      target: line_total
      type: formula
      formula: "quantity * unit_price * (1 - discount_pct/100)"

    - sources: [line_total]
      target: tax_amount
      type: formula
      formula: "line_total * 0.21"
```

### 2.4 Implementation Approach

#### 2.4.1 Generation Order Resolution

```python
def resolve_generation_order(rules: List[CorrelationRule]) -> List[str]:
    """Topologically sort columns based on dependencies."""
    # Build dependency graph
    graph = defaultdict(list)
    for rule in rules:
        sources = rule.source_column if isinstance(rule.source_column, list) else [rule.source_column]
        for source in sources:
            graph[rule.target_column].append(source)

    # Topological sort
    return topological_sort(graph)
```

#### 2.4.2 Copula-Based Correlation (Advanced)

For complex multi-variate correlations:

```python
from scipy.stats import norm, multivariate_normal

def generate_correlated_columns(
    correlation_matrix: np.ndarray,
    marginal_distributions: List[Distribution],
    n_samples: int
) -> np.ndarray:
    """Generate correlated samples using Gaussian copula."""

    # Generate correlated uniform samples via copula
    mvn = multivariate_normal(cov=correlation_matrix)
    samples = mvn.rvs(size=n_samples)
    uniform_samples = norm.cdf(samples)

    # Transform to target distributions
    result = np.zeros_like(uniform_samples)
    for i, dist in enumerate(marginal_distributions):
        result[:, i] = dist.ppf(uniform_samples[:, i])

    return result
```

### 2.5 New MCP Tools

#### Tool: `02_generate_correlated_fact`

```json
{
  "name": "02_generate_correlated_fact",
  "description": "Generate fact table with correlated columns",
  "parameters": {
    "fact_type": "sales|finance|custom",
    "row_count": 100000,
    "correlation_template": "sales_standard|finance_standard|custom",
    "custom_correlations": [
      {
        "source": "quantity",
        "target": "discount_pct",
        "type": "linear",
        "correlation": 0.5
      }
    ],
    "dimensions": {}
  }
}
```

### 2.6 File Structure

```
core/generation/
├── correlation_engine.py     # Main correlation logic
├── correlation_rules.py      # Rule definitions and parsing
├── copula_generator.py       # Advanced copula-based generation
└── formula_evaluator.py      # Safe formula evaluation

config/correlations/
├── sales_correlations.yaml
├── finance_correlations.yaml
└── inventory_correlations.yaml
```

---

## 3. Multi-Currency & Exchange Rates

### 3.1 Overview

Support multi-currency financial data generation with realistic exchange rates, currency conversions, and reporting currency consolidation.

### 3.2 Core Components

#### 3.2.1 Currency Manager

**New file**: `core/generation/currency_manager.py`

```python
class CurrencyManager:
    """Manages currency data and exchange rate generation."""

    SUPPORTED_CURRENCIES = [
        "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD",
        "CNY", "INR", "BRL", "MXN", "KRW", "SGD", "HKD"
    ]

    def __init__(self, base_currency: str = "USD"):
        self.base_currency = base_currency
        self.exchange_rates: Dict[str, ExchangeRate] = {}

    def generate_exchange_rate_dimension(
        self,
        currencies: List[str],
        start_date: date,
        end_date: date,
        volatility: str = "medium"  # low, medium, high
    ) -> pl.DataFrame:
        """Generate historical exchange rates with realistic fluctuations."""
        pass

    def convert_amount(
        self,
        amount: float,
        from_currency: str,
        to_currency: str,
        rate_date: date
    ) -> float:
        """Convert amount between currencies using historical rate."""
        pass
```

#### 3.2.2 Exchange Rate Dimension Schema

```python
exchange_rate_schema = {
    "rate_key": "INT",              # Surrogate key
    "rate_date": "DATE",            # Effective date
    "from_currency": "VARCHAR(3)",  # ISO currency code
    "to_currency": "VARCHAR(3)",    # Target currency (usually base)
    "exchange_rate": "DECIMAL(18,6)", # Rate
    "rate_type": "VARCHAR(20)",     # spot, average, closing
    "source": "VARCHAR(50)",        # ECB, Fed, generated
    "is_current": "BOOLEAN"         # Latest rate flag
}
```

#### 3.2.3 Currency Dimension Schema

```python
currency_dimension_schema = {
    "currency_key": "INT",
    "currency_code": "VARCHAR(3)",    # ISO 4217
    "currency_name": "VARCHAR(100)",
    "currency_symbol": "VARCHAR(5)",
    "decimal_places": "INT",
    "country": "VARCHAR(100)",
    "is_active": "BOOLEAN"
}
```

### 3.3 Exchange Rate Generation

#### 3.3.1 Realistic Rate Fluctuation

```python
def generate_exchange_rates(
    base_currency: str,
    target_currencies: List[str],
    start_date: date,
    end_date: date,
    volatility: float = 0.01  # Daily volatility
) -> pl.DataFrame:
    """Generate exchange rates using geometric Brownian motion."""

    # Base rates (approximate real-world rates)
    base_rates = {
        "EUR": 0.92, "GBP": 0.79, "JPY": 149.50, "CHF": 0.88,
        "CAD": 1.36, "AUD": 1.53, "CNY": 7.24, "INR": 83.12
    }

    rates_data = []
    date_range = pd.date_range(start_date, end_date, freq='D')

    for currency in target_currencies:
        if currency == base_currency:
            continue

        current_rate = base_rates.get(currency, 1.0)

        for d in date_range:
            # Geometric Brownian motion for realistic fluctuation
            drift = 0  # No long-term trend
            shock = np.random.normal(0, volatility)
            current_rate *= np.exp(drift + shock)

            rates_data.append({
                "rate_date": d.date(),
                "from_currency": currency,
                "to_currency": base_currency,
                "exchange_rate": round(current_rate, 6),
                "rate_type": "closing"
            })

    return pl.DataFrame(rates_data)
```

### 3.4 Multi-Currency Fact Tables

#### 3.4.1 Enhanced Fact Schema

```python
multicurrency_sales_fact = {
    # Standard columns
    "transaction_id": "INT",
    "date_key": "INT",
    "customer_key": "INT",
    "product_key": "INT",

    # Transaction currency amounts
    "transaction_currency": "VARCHAR(3)",
    "quantity": "INT",
    "unit_price_tc": "DECIMAL(18,2)",      # Transaction currency
    "total_amount_tc": "DECIMAL(18,2)",    # Transaction currency

    # Reporting currency amounts (for consolidation)
    "reporting_currency": "VARCHAR(3)",
    "exchange_rate_used": "DECIMAL(18,6)",
    "unit_price_rc": "DECIMAL(18,2)",      # Reporting currency
    "total_amount_rc": "DECIMAL(18,2)",    # Reporting currency

    # Exchange rate reference
    "exchange_rate_key": "INT"             # FK to exchange rate dimension
}
```

#### 3.4.2 Currency Assignment Logic

```python
def assign_transaction_currency(
    customer_country: str,
    transaction_type: str,
    currency_weights: Dict[str, float] = None
) -> str:
    """Assign realistic transaction currency based on context."""

    country_currency_map = {
        "United States": "USD",
        "Germany": "EUR", "France": "EUR", "Italy": "EUR",
        "United Kingdom": "GBP",
        "Japan": "JPY",
        "China": "CNY",
        "Canada": "CAD",
        "Australia": "AUD"
    }

    # Primary currency from country
    primary = country_currency_map.get(customer_country, "USD")

    # Some transactions may be in different currency
    if currency_weights:
        currencies = list(currency_weights.keys())
        weights = list(currency_weights.values())
        return np.random.choice(currencies, p=weights)

    return primary
```

### 3.5 New MCP Tools

#### Tool: `02_generate_currency_dimension`

```json
{
  "name": "02_generate_currency_dimension",
  "description": "Generate currency reference dimension",
  "parameters": {
    "currencies": ["USD", "EUR", "GBP", "JPY"],
    "output_path": "path/to/currency_dim.csv"
  }
}
```

#### Tool: `02_generate_exchange_rates`

```json
{
  "name": "02_generate_exchange_rates",
  "description": "Generate historical exchange rate dimension",
  "parameters": {
    "base_currency": "USD",
    "target_currencies": ["EUR", "GBP", "JPY", "CAD"],
    "start_date": "2023-01-01",
    "end_date": "2024-12-31",
    "volatility": "medium",
    "rate_types": ["closing", "average"],
    "output_path": "path/to/exchange_rates.csv"
  }
}
```

#### Tool: `02_generate_multicurrency_fact`

```json
{
  "name": "02_generate_multicurrency_fact",
  "description": "Generate fact table with multi-currency support",
  "parameters": {
    "fact_type": "sales|finance",
    "row_count": 100000,
    "transaction_currencies": ["USD", "EUR", "GBP"],
    "reporting_currency": "USD",
    "exchange_rate_file": "path/to/exchange_rates.csv",
    "dimensions": {},
    "output_path": "path/to/sales_fact.csv"
  }
}
```

### 3.6 File Structure

```
core/generation/
├── currency_manager.py       # Currency handling logic
├── exchange_rate_generator.py # Rate generation with GBM
└── multicurrency_fact.py     # Multi-currency fact generation

config/currencies/
├── currency_master.yaml      # Currency definitions
├── country_currency_map.yaml # Country to currency mapping
└── base_rates.yaml           # Starting exchange rates
```

---

## 4. PII Detection & Anonymization

### 4.1 Overview

Detect and anonymize personally identifiable information (PII) in data files, enabling safe use of production-like data for testing and development.

### 4.2 Core Components

#### 4.2.1 PII Detector

**New file**: `core/privacy/pii_detector.py`

```python
class PIIDetector:
    """Detects PII in data columns using patterns and ML."""

    PII_PATTERNS = {
        "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        "phone_us": r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b",
        "phone_intl": r"\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}",
        "ssn": r"\b\d{3}-\d{2}-\d{4}\b",
        "credit_card": r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b",
        "ip_address": r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
        "date_of_birth": r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b",
        "passport": r"\b[A-Z]{1,2}\d{6,9}\b",
        "iban": r"\b[A-Z]{2}\d{2}[A-Z0-9]{4,30}\b",
        "national_id": r"\b\d{2}\.\d{2}\.\d{2}-\d{3}\.\d{2}\b"  # Belgian format
    }

    PII_COLUMN_NAMES = {
        "high_confidence": [
            "ssn", "social_security", "national_id", "passport",
            "credit_card", "card_number", "account_number",
            "date_of_birth", "dob", "birth_date"
        ],
        "medium_confidence": [
            "email", "phone", "mobile", "address", "street",
            "first_name", "last_name", "full_name", "name",
            "ip_address", "salary", "income"
        ],
        "low_confidence": [
            "city", "state", "zip", "postal_code", "country",
            "age", "gender", "department"
        ]
    }

    def detect(self, df: pl.DataFrame) -> PIIReport:
        """Scan dataframe for PII and return detection report."""
        pass
```

#### 4.2.2 PII Report Structure

```python
@dataclass
class PIIReport:
    file_path: str
    scan_timestamp: datetime
    total_columns: int
    pii_columns: List[PIIColumnInfo]
    risk_score: float  # 0.0 - 1.0
    recommendations: List[str]

@dataclass
class PIIColumnInfo:
    column_name: str
    pii_type: str  # email, phone, ssn, name, address, etc.
    confidence: float  # 0.0 - 1.0
    detection_method: str  # pattern, column_name, ml_model
    sample_matches: List[str]  # Redacted samples
    row_count_affected: int
    recommended_action: str  # mask, hash, synthetic, remove
```

### 4.3 Anonymization Techniques

#### 4.3.1 Masking Strategies

```python
class AnonymizationEngine:
    """Applies various anonymization techniques to PII data."""

    STRATEGIES = {
        "mask": MaskingStrategy,           # Replace with ***
        "hash": HashingStrategy,           # SHA-256 hash
        "synthetic": SyntheticStrategy,    # Replace with fake data
        "generalize": GeneralizeStrategy,  # Reduce precision
        "shuffle": ShuffleStrategy,        # Shuffle within column
        "redact": RedactStrategy,          # Remove entirely
        "tokenize": TokenizeStrategy       # Replace with tokens
    }
```

#### 4.3.2 Strategy Details

| Strategy | Description | Preserves | Use Case |
|----------|-------------|-----------|----------|
| **Mask** | `john@email.com` → `j***@e***.com` | Format, length | Display purposes |
| **Hash** | Consistent SHA-256 | Referential integrity | Joining tables |
| **Synthetic** | Replace with Faker data | Data type, patterns | Testing, ML training |
| **Generalize** | `25` → `20-30`, `NYC` → `Northeast` | Analytical value | Statistical analysis |
| **Shuffle** | Randomly reorder values | Distribution | Breaking direct links |
| **Redact** | Remove column entirely | Nothing | Highly sensitive data |
| **Tokenize** | Replace with reversible token | Reversibility (with key) | Controlled access |

#### 4.3.3 Strategy Implementation

```python
class SyntheticStrategy:
    """Replace PII with realistic synthetic data using Faker."""

    def __init__(self, locale: str = "en_US", seed: int = None):
        self.faker = Faker(locale)
        if seed:
            Faker.seed(seed)

    GENERATORS = {
        "email": lambda f: f.email(),
        "phone": lambda f: f.phone_number(),
        "name": lambda f: f.name(),
        "first_name": lambda f: f.first_name(),
        "last_name": lambda f: f.last_name(),
        "address": lambda f: f.address(),
        "ssn": lambda f: f.ssn(),
        "credit_card": lambda f: f.credit_card_number(),
        "date_of_birth": lambda f: f.date_of_birth(),
        "company": lambda f: f.company(),
        "ip_address": lambda f: f.ipv4()
    }

    def anonymize(self, value: str, pii_type: str) -> str:
        generator = self.GENERATORS.get(pii_type, lambda f: f.text(20))
        return generator(self.faker)
```

### 4.4 Referential Integrity Preservation

```python
class ConsistentAnonymizer:
    """Ensures same input produces same output across files."""

    def __init__(self, seed: int = 42):
        self.mapping_cache: Dict[str, Dict[str, str]] = {}
        self.seed = seed

    def anonymize_with_consistency(
        self,
        value: str,
        pii_type: str,
        context: str = "default"
    ) -> str:
        """Return consistent anonymized value for same input."""
        cache_key = f"{context}:{pii_type}"

        if cache_key not in self.mapping_cache:
            self.mapping_cache[cache_key] = {}

        if value not in self.mapping_cache[cache_key]:
            # Generate deterministic fake value
            faker = Faker()
            faker.seed_instance(hash(f"{self.seed}:{value}"))
            self.mapping_cache[cache_key][value] = self._generate(faker, pii_type)

        return self.mapping_cache[cache_key][value]
```

### 4.5 New MCP Tools

#### Tool: `09_detect_pii`

```json
{
  "name": "09_detect_pii",
  "description": "Scan file for personally identifiable information",
  "parameters": {
    "file_path": "path/to/data.csv",
    "scan_depth": "quick|standard|deep",
    "pii_types": ["email", "phone", "ssn", "name", "all"],
    "sample_size": 1000
  },
  "returns": {
    "pii_columns": [],
    "risk_score": 0.0,
    "recommendations": []
  }
}
```

#### Tool: `09_anonymize_file`

```json
{
  "name": "09_anonymize_file",
  "description": "Anonymize PII in a data file",
  "parameters": {
    "input_path": "path/to/data.csv",
    "output_path": "path/to/anonymized.csv",
    "strategy": "synthetic|mask|hash|redact",
    "columns": {
      "email": "synthetic",
      "phone": "mask",
      "ssn": "redact"
    },
    "preserve_referential_integrity": true,
    "consistency_seed": 42
  }
}
```

#### Tool: `09_generate_anonymization_report`

```json
{
  "name": "09_generate_anonymization_report",
  "description": "Generate detailed PII detection and anonymization report",
  "parameters": {
    "file_paths": ["file1.csv", "file2.csv"],
    "output_format": "json|markdown|html"
  }
}
```

### 4.6 File Structure

```
core/privacy/
├── __init__.py
├── pii_detector.py           # PII detection logic
├── pii_patterns.py           # Regex patterns for PII types
├── anonymization_engine.py   # Main anonymization orchestrator
├── strategies/
│   ├── __init__.py
│   ├── masking.py
│   ├── hashing.py
│   ├── synthetic.py
│   ├── generalization.py
│   └── tokenization.py
└── consistency_manager.py    # Cross-file consistency

server/handlers/
└── privacy_handlers.py       # MCP tool handlers

config/privacy/
├── pii_patterns.yaml         # Customizable PII patterns
├── column_classifications.yaml
└── anonymization_defaults.yaml
```

---

## 5. Data Subsetting

### 5.1 Overview

Create smaller, representative subsets of large datasets while maintaining statistical properties, referential integrity, and data distributions.

### 5.2 Core Components

#### 5.2.1 Subset Engine

**New file**: `core/subsetting/subset_engine.py`

```python
class SubsetEngine:
    """Creates representative data subsets with various strategies."""

    def __init__(self, config: SubsetConfig):
        self.config = config
        self.referential_graph: Dict[str, List[ForeignKey]] = {}

    def create_subset(
        self,
        source_files: Dict[str, str],  # table_name -> file_path
        target_percentage: float,
        strategy: str,
        output_dir: str
    ) -> SubsetResult:
        """Create subset of related tables maintaining integrity."""
        pass
```

#### 5.2.2 Subsetting Strategies

| Strategy | Description | Best For |
|----------|-------------|----------|
| **Random** | Pure random sampling | Quick testing |
| **Stratified** | Maintain distribution of key columns | Preserving proportions |
| **Time-Window** | Filter by date range | Recent data testing |
| **Top-N** | Take first/last N records per group | Representative samples |
| **Referential** | Start from facts, pull related dims | Star schema integrity |
| **Cohort** | Select complete customer/entity groups | Journey analysis |

### 5.3 Implementation Details

#### 5.3.1 Stratified Sampling

```python
def stratified_sample(
    df: pl.DataFrame,
    stratify_columns: List[str],
    sample_fraction: float,
    min_per_stratum: int = 1
) -> pl.DataFrame:
    """Sample while maintaining distribution of stratification columns."""

    # Calculate current distribution
    strata_counts = df.group_by(stratify_columns).count()

    # Calculate target counts per stratum
    target_counts = strata_counts.with_columns([
        pl.max([
            (pl.col("count") * sample_fraction).round().cast(pl.Int64),
            pl.lit(min_per_stratum)
        ]).alias("target")
    ])

    # Sample from each stratum
    sampled_dfs = []
    for stratum in df.group_by(stratify_columns):
        stratum_key = stratum[0]
        stratum_df = stratum[1]
        target = target_counts.filter(
            # match stratum key
        )["target"][0]

        sampled_dfs.append(
            stratum_df.sample(n=min(target, len(stratum_df)))
        )

    return pl.concat(sampled_dfs)
```

#### 5.3.2 Referential Subsetting

```python
def referential_subset(
    fact_table: pl.DataFrame,
    dimensions: Dict[str, pl.DataFrame],
    foreign_keys: Dict[str, str],  # fact_column -> dim_table
    fact_sample_fraction: float
) -> Tuple[pl.DataFrame, Dict[str, pl.DataFrame]]:
    """Subset fact table and pull only referenced dimension records."""

    # Sample fact table
    fact_subset = fact_table.sample(fraction=fact_sample_fraction)

    # For each dimension, keep only referenced records
    dim_subsets = {}
    for fact_col, dim_name in foreign_keys.items():
        dim_df = dimensions[dim_name]
        dim_key = f"{dim_name}_key"  # Convention

        # Get unique FK values from fact subset
        referenced_keys = fact_subset[fact_col].unique()

        # Filter dimension to only referenced records
        dim_subsets[dim_name] = dim_df.filter(
            pl.col(dim_key).is_in(referenced_keys)
        )

    return fact_subset, dim_subsets
```

#### 5.3.3 Time-Window Subsetting

```python
def time_window_subset(
    df: pl.DataFrame,
    date_column: str,
    start_date: date = None,
    end_date: date = None,
    last_n_days: int = None,
    last_n_months: int = None
) -> pl.DataFrame:
    """Filter data to specific time window."""

    if last_n_days:
        start_date = date.today() - timedelta(days=last_n_days)
        end_date = date.today()
    elif last_n_months:
        end_date = date.today()
        start_date = end_date - relativedelta(months=last_n_months)

    return df.filter(
        (pl.col(date_column) >= start_date) &
        (pl.col(date_column) <= end_date)
    )
```

### 5.4 New MCP Tools

#### Tool: `10_create_subset`

```json
{
  "name": "10_create_subset",
  "description": "Create representative subset of data files",
  "parameters": {
    "source_files": {
      "sales_fact": "path/to/sales.csv",
      "customer_dim": "path/to/customers.csv",
      "product_dim": "path/to/products.csv"
    },
    "strategy": "stratified|random|time_window|referential",
    "target_percentage": 10,
    "stratify_columns": ["region", "product_category"],
    "time_window": {
      "column": "order_date",
      "last_n_months": 6
    },
    "foreign_keys": {
      "customer_key": "customer_dim",
      "product_key": "product_dim"
    },
    "output_dir": "path/to/subset/"
  }
}
```

#### Tool: `10_analyze_subset`

```json
{
  "name": "10_analyze_subset",
  "description": "Compare subset statistics to original data",
  "parameters": {
    "original_path": "path/to/original.csv",
    "subset_path": "path/to/subset.csv",
    "compare_columns": ["region", "product_category", "amount"]
  },
  "returns": {
    "original_row_count": 1000000,
    "subset_row_count": 100000,
    "reduction_percentage": 90,
    "distribution_comparison": {},
    "integrity_check": "passed"
  }
}
```

### 5.5 File Structure

```
core/subsetting/
├── __init__.py
├── subset_engine.py          # Main orchestration
├── strategies/
│   ├── __init__.py
│   ├── random_sampler.py
│   ├── stratified_sampler.py
│   ├── time_window.py
│   ├── referential_sampler.py
│   └── cohort_sampler.py
├── integrity_checker.py      # Verify FK integrity after subset
└── distribution_analyzer.py  # Compare distributions

server/handlers/
└── subsetting_handlers.py
```

---

## 6. Industry-Specific Star Schemas

### 6.1 Overview

Provide complete, ready-to-use star schema templates for common industries with realistic data generation, proper relationships, and domain-specific attributes.

### 6.2 Industry Templates

### 6.2.1 Retail Industry

```yaml
# config/industry_templates/retail.yaml
retail:
  name: "Retail Star Schema"
  description: "Complete retail analytics data model"

  dimensions:
    customer:
      columns:
        - {name: customer_key, type: int, role: surrogate_key}
        - {name: customer_id, type: string, role: business_key}
        - {name: first_name, type: string, generator: faker.first_name}
        - {name: last_name, type: string, generator: faker.last_name}
        - {name: email, type: string, generator: faker.email}
        - {name: phone, type: string, generator: faker.phone_number}
        - {name: customer_segment, type: string, values: [Premium, Standard, Basic], weights: [0.1, 0.5, 0.4]}
        - {name: acquisition_channel, type: string, values: [Online, Store, Referral, Social], weights: [0.4, 0.3, 0.2, 0.1]}
        - {name: loyalty_tier, type: string, values: [Gold, Silver, Bronze, None], weights: [0.05, 0.15, 0.30, 0.50]}
        - {name: registration_date, type: date, generator: date_range, params: {start: -1825, end: 0}}
        - {name: lifetime_value_bucket, type: string, values: [High, Medium, Low]}
      row_count: 50000

    product:
      columns:
        - {name: product_key, type: int, role: surrogate_key}
        - {name: product_sku, type: string, generator: sku}
        - {name: product_name, type: string, generator: product_name}
        - {name: category, type: string, values: [Electronics, Clothing, Home, Sports, Beauty]}
        - {name: subcategory, type: string, depends_on: category}
        - {name: brand, type: string, generator: faker.company}
        - {name: unit_cost, type: decimal, generator: price_range, params: {min: 5, max: 500}}
        - {name: list_price, type: decimal, derived: "unit_cost * uniform(1.3, 2.5)"}
        - {name: weight_kg, type: decimal, generator: uniform, params: {min: 0.1, max: 50}}
        - {name: is_active, type: boolean, values: [true, false], weights: [0.9, 0.1]}
        - {name: launch_date, type: date}
      row_count: 10000

    store:
      columns:
        - {name: store_key, type: int, role: surrogate_key}
        - {name: store_id, type: string}
        - {name: store_name, type: string}
        - {name: store_type, type: string, values: [Flagship, Standard, Express, Online]}
        - {name: address, type: string, generator: faker.street_address}
        - {name: city, type: string, generator: faker.city}
        - {name: state, type: string}
        - {name: country, type: string}
        - {name: region, type: string, values: [North, South, East, West]}
        - {name: square_footage, type: int, generator: uniform, params: {min: 5000, max: 100000}}
        - {name: open_date, type: date}
        - {name: manager_name, type: string, generator: faker.name}
      row_count: 500

    promotion:
      columns:
        - {name: promotion_key, type: int, role: surrogate_key}
        - {name: promotion_id, type: string}
        - {name: promotion_name, type: string}
        - {name: promotion_type, type: string, values: [Discount, BOGO, Bundle, Loyalty]}
        - {name: discount_percentage, type: decimal}
        - {name: start_date, type: date}
        - {name: end_date, type: date}
        - {name: min_purchase_amount, type: decimal}
        - {name: is_active, type: boolean}
      row_count: 200

    date:
      type: standard_date_dimension
      start_date: "2020-01-01"
      end_date: "2025-12-31"
      fiscal_year_start_month: 2

  facts:
    sales:
      grain: "One row per transaction line item"
      columns:
        - {name: sales_key, type: int, role: surrogate_key}
        - {name: date_key, type: int, role: fk, dimension: date}
        - {name: customer_key, type: int, role: fk, dimension: customer}
        - {name: product_key, type: int, role: fk, dimension: product}
        - {name: store_key, type: int, role: fk, dimension: store}
        - {name: promotion_key, type: int, role: fk, dimension: promotion}
        - {name: transaction_id, type: string}
        - {name: quantity, type: int, generator: poisson, params: {lambda: 2}}
        - {name: unit_price, type: decimal, derived_from: product.list_price}
        - {name: discount_amount, type: decimal}
        - {name: net_amount, type: decimal, formula: "quantity * unit_price - discount_amount"}
        - {name: tax_amount, type: decimal, formula: "net_amount * 0.08"}
        - {name: total_amount, type: decimal, formula: "net_amount + tax_amount"}
        - {name: cost_amount, type: decimal, derived_from: product.unit_cost}
        - {name: profit_amount, type: decimal, formula: "net_amount - (quantity * cost_amount)"}
        - {name: payment_method, type: string, values: [Credit, Debit, Cash, Mobile]}
        - {name: sales_channel, type: string, values: [In-Store, Online, Mobile App]}
      row_count: 5000000
      time_pattern: retail_sales

    inventory:
      grain: "One row per product per store per day"
      columns:
        - {name: inventory_key, type: int, role: surrogate_key}
        - {name: date_key, type: int, role: fk, dimension: date}
        - {name: product_key, type: int, role: fk, dimension: product}
        - {name: store_key, type: int, role: fk, dimension: store}
        - {name: quantity_on_hand, type: int}
        - {name: quantity_on_order, type: int}
        - {name: reorder_point, type: int}
        - {name: days_of_supply, type: int}
      row_count: 1000000

    returns:
      grain: "One row per returned item"
      columns:
        - {name: return_key, type: int, role: surrogate_key}
        - {name: original_sales_key, type: int, role: fk, dimension: sales}
        - {name: return_date_key, type: int, role: fk, dimension: date}
        - {name: return_reason, type: string, values: [Defective, Wrong Size, Changed Mind, Damaged]}
        - {name: return_amount, type: decimal}
        - {name: refund_method, type: string, values: [Original Payment, Store Credit, Exchange]}
      row_count: 250000

  relationships:
    - {from: sales.customer_key, to: customer.customer_key, type: many_to_one}
    - {from: sales.product_key, to: product.product_key, type: many_to_one}
    - {from: sales.store_key, to: store.store_key, type: many_to_one}
    - {from: sales.date_key, to: date.date_key, type: many_to_one}
    - {from: sales.promotion_key, to: promotion.promotion_key, type: many_to_one}
    - {from: inventory.product_key, to: product.product_key, type: many_to_one}
    - {from: inventory.store_key, to: store.store_key, type: many_to_one}
    - {from: returns.original_sales_key, to: sales.sales_key, type: many_to_one}
```

### 6.2.2 Healthcare Industry

```yaml
healthcare:
  name: "Healthcare Analytics Star Schema"

  dimensions:
    patient:
      columns:
        - {name: patient_key, type: int, role: surrogate_key}
        - {name: patient_id, type: string, role: business_key}
        - {name: gender, type: string, values: [Male, Female, Other]}
        - {name: birth_date, type: date}
        - {name: age_group, type: string, values: [0-17, 18-34, 35-54, 55-64, 65+]}
        - {name: blood_type, type: string, values: [A+, A-, B+, B-, AB+, AB-, O+, O-]}
        - {name: insurance_type, type: string, values: [Private, Medicare, Medicaid, Self-Pay]}
        - {name: zip_code, type: string}
        - {name: registration_date, type: date}
      row_count: 100000

    provider:
      columns:
        - {name: provider_key, type: int, role: surrogate_key}
        - {name: provider_npi, type: string}
        - {name: provider_name, type: string}
        - {name: specialty, type: string, values: [Primary Care, Cardiology, Orthopedics, Oncology, Neurology, Pediatrics]}
        - {name: facility_type, type: string, values: [Hospital, Clinic, Specialist Office, Urgent Care]}
        - {name: city, type: string}
        - {name: state, type: string}
      row_count: 5000

    diagnosis:
      columns:
        - {name: diagnosis_key, type: int, role: surrogate_key}
        - {name: icd10_code, type: string}
        - {name: diagnosis_description, type: string}
        - {name: diagnosis_category, type: string}
        - {name: is_chronic, type: boolean}
        - {name: severity_level, type: string, values: [Low, Medium, High, Critical]}
      row_count: 2000

    procedure:
      columns:
        - {name: procedure_key, type: int, role: surrogate_key}
        - {name: cpt_code, type: string}
        - {name: procedure_description, type: string}
        - {name: procedure_category, type: string}
        - {name: average_duration_minutes, type: int}
        - {name: requires_anesthesia, type: boolean}
      row_count: 3000

  facts:
    encounters:
      grain: "One row per patient encounter"
      columns:
        - {name: encounter_key, type: int, role: surrogate_key}
        - {name: date_key, type: int, role: fk, dimension: date}
        - {name: patient_key, type: int, role: fk, dimension: patient}
        - {name: provider_key, type: int, role: fk, dimension: provider}
        - {name: primary_diagnosis_key, type: int, role: fk, dimension: diagnosis}
        - {name: encounter_type, type: string, values: [Outpatient, Inpatient, Emergency, Telehealth]}
        - {name: admission_date, type: datetime}
        - {name: discharge_date, type: datetime}
        - {name: length_of_stay_days, type: int}
        - {name: total_charges, type: decimal}
        - {name: total_payments, type: decimal}
        - {name: patient_responsibility, type: decimal}
      row_count: 2000000

    claims:
      grain: "One row per claim line"
      columns:
        - {name: claim_key, type: int, role: surrogate_key}
        - {name: encounter_key, type: int, role: fk, fact: encounters}
        - {name: procedure_key, type: int, role: fk, dimension: procedure}
        - {name: service_date_key, type: int, role: fk, dimension: date}
        - {name: billed_amount, type: decimal}
        - {name: allowed_amount, type: decimal}
        - {name: paid_amount, type: decimal}
        - {name: claim_status, type: string, values: [Paid, Denied, Pending, Appealed]}
        - {name: denial_reason, type: string}
      row_count: 5000000
```

### 6.2.3 Manufacturing Industry

```yaml
manufacturing:
  name: "Manufacturing Analytics Star Schema"

  dimensions:
    machine:
      columns:
        - {name: machine_key, type: int}
        - {name: machine_id, type: string}
        - {name: machine_name, type: string}
        - {name: machine_type, type: string, values: [CNC, Assembly, Packaging, QA, Welding]}
        - {name: manufacturer, type: string}
        - {name: install_date, type: date}
        - {name: production_line, type: string}
        - {name: maintenance_schedule, type: string, values: [Daily, Weekly, Monthly]}
      row_count: 500

    shift:
      columns:
        - {name: shift_key, type: int}
        - {name: shift_name, type: string, values: [Morning, Afternoon, Night]}
        - {name: start_time, type: time}
        - {name: end_time, type: time}
        - {name: is_weekend, type: boolean}
      row_count: 6

    operator:
      columns:
        - {name: operator_key, type: int}
        - {name: employee_id, type: string}
        - {name: operator_name, type: string}
        - {name: skill_level, type: string, values: [Junior, Mid, Senior, Expert]}
        - {name: certification_level, type: int}
        - {name: hire_date, type: date}
      row_count: 1000

    work_order:
      columns:
        - {name: work_order_key, type: int}
        - {name: work_order_id, type: string}
        - {name: product_key, type: int}
        - {name: target_quantity, type: int}
        - {name: priority, type: string, values: [Low, Normal, High, Urgent]}
        - {name: due_date, type: date}
      row_count: 10000

  facts:
    production:
      grain: "One row per production run"
      columns:
        - {name: production_key, type: int}
        - {name: date_key, type: int, role: fk, dimension: date}
        - {name: shift_key, type: int, role: fk, dimension: shift}
        - {name: machine_key, type: int, role: fk, dimension: machine}
        - {name: operator_key, type: int, role: fk, dimension: operator}
        - {name: work_order_key, type: int, role: fk, dimension: work_order}
        - {name: quantity_produced, type: int}
        - {name: quantity_defective, type: int}
        - {name: scrap_quantity, type: int}
        - {name: cycle_time_seconds, type: decimal}
        - {name: downtime_minutes, type: int}
        - {name: setup_time_minutes, type: int}
        - {name: oee_score, type: decimal}  # Overall Equipment Effectiveness
      row_count: 2000000

    quality:
      grain: "One row per quality inspection"
      columns:
        - {name: quality_key, type: int}
        - {name: production_key, type: int, role: fk, fact: production}
        - {name: inspection_date_key, type: int, role: fk, dimension: date}
        - {name: inspection_type, type: string, values: [Visual, Dimensional, Functional, Material]}
        - {name: result, type: string, values: [Pass, Fail, Rework]}
        - {name: defect_type, type: string}
        - {name: defect_severity, type: string, values: [Minor, Major, Critical]}
        - {name: inspector_key, type: int, role: fk, dimension: operator}
      row_count: 500000

    maintenance:
      grain: "One row per maintenance event"
      columns:
        - {name: maintenance_key, type: int}
        - {name: machine_key, type: int, role: fk, dimension: machine}
        - {name: date_key, type: int, role: fk, dimension: date}
        - {name: maintenance_type, type: string, values: [Preventive, Corrective, Emergency, Predictive]}
        - {name: duration_minutes, type: int}
        - {name: parts_cost, type: decimal}
        - {name: labor_cost, type: decimal}
        - {name: root_cause, type: string}
      row_count: 100000
```

### 6.2.4 Banking/Financial Services

```yaml
banking:
  name: "Banking Analytics Star Schema"

  dimensions:
    customer:
      columns:
        - {name: customer_key, type: int}
        - {name: customer_id, type: string}
        - {name: customer_type, type: string, values: [Individual, Business, Institution]}
        - {name: segment, type: string, values: [Mass Market, Affluent, High Net Worth, Ultra HNW]}
        - {name: relationship_start_date, type: date}
        - {name: relationship_manager_id, type: string}
        - {name: risk_rating, type: string, values: [Low, Medium, High]}
        - {name: kyc_status, type: string, values: [Verified, Pending, Expired]}
      row_count: 200000

    account:
      columns:
        - {name: account_key, type: int}
        - {name: account_number, type: string}
        - {name: account_type, type: string, values: [Checking, Savings, CD, Money Market, Investment]}
        - {name: currency, type: string, values: [USD, EUR, GBP]}
        - {name: open_date, type: date}
        - {name: status, type: string, values: [Active, Dormant, Closed, Frozen]}
        - {name: interest_rate, type: decimal}
        - {name: credit_limit, type: decimal}
      row_count: 500000

    branch:
      columns:
        - {name: branch_key, type: int}
        - {name: branch_code, type: string}
        - {name: branch_name, type: string}
        - {name: branch_type, type: string, values: [Full Service, Express, Digital Only]}
        - {name: region, type: string}
        - {name: city, type: string}
      row_count: 1000

    product:
      columns:
        - {name: product_key, type: int}
        - {name: product_code, type: string}
        - {name: product_name, type: string}
        - {name: product_category, type: string, values: [Deposits, Loans, Cards, Insurance, Investments]}
        - {name: risk_weight, type: decimal}
      row_count: 200

  facts:
    transactions:
      grain: "One row per transaction"
      columns:
        - {name: transaction_key, type: int}
        - {name: date_key, type: int, role: fk, dimension: date}
        - {name: account_key, type: int, role: fk, dimension: account}
        - {name: customer_key, type: int, role: fk, dimension: customer}
        - {name: branch_key, type: int, role: fk, dimension: branch}
        - {name: transaction_type, type: string, values: [Deposit, Withdrawal, Transfer, Payment, Fee]}
        - {name: channel, type: string, values: [Branch, ATM, Online, Mobile, Wire]}
        - {name: amount, type: decimal}
        - {name: running_balance, type: decimal}
        - {name: is_international, type: boolean}
        - {name: fraud_flag, type: boolean}
      row_count: 10000000

    balances:
      grain: "One row per account per day"
      columns:
        - {name: balance_key, type: int}
        - {name: date_key, type: int, role: fk, dimension: date}
        - {name: account_key, type: int, role: fk, dimension: account}
        - {name: opening_balance, type: decimal}
        - {name: closing_balance, type: decimal}
        - {name: average_balance, type: decimal}
        - {name: minimum_balance, type: decimal}
        - {name: transaction_count, type: int}
      row_count: 5000000

    loans:
      grain: "One row per loan"
      columns:
        - {name: loan_key, type: int}
        - {name: customer_key, type: int, role: fk, dimension: customer}
        - {name: origination_date_key, type: int, role: fk, dimension: date}
        - {name: loan_type, type: string, values: [Mortgage, Auto, Personal, Business, Student]}
        - {name: principal_amount, type: decimal}
        - {name: interest_rate, type: decimal}
        - {name: term_months, type: int}
        - {name: outstanding_balance, type: decimal}
        - {name: payment_status, type: string, values: [Current, 30DPD, 60DPD, 90DPD, Default]}
        - {name: collateral_value, type: decimal}
      row_count: 500000
```

### 6.3 New MCP Tools

#### Tool: `02_generate_industry_schema`

```json
{
  "name": "02_generate_industry_schema",
  "description": "Generate complete industry-specific star schema",
  "parameters": {
    "industry": "retail|healthcare|manufacturing|banking|telecom",
    "scale_factor": 1.0,
    "date_range": {
      "start": "2020-01-01",
      "end": "2025-12-31"
    },
    "include_facts": ["sales", "inventory", "returns"],
    "exclude_dimensions": [],
    "time_patterns": true,
    "output_dir": "path/to/output/"
  }
}
```

#### Tool: `02_list_industry_templates`

```json
{
  "name": "02_list_industry_templates",
  "description": "List available industry templates with details",
  "parameters": {
    "industry": "all|retail|healthcare|manufacturing|banking"
  },
  "returns": {
    "templates": [
      {
        "name": "retail",
        "dimensions": ["customer", "product", "store", "promotion", "date"],
        "facts": ["sales", "inventory", "returns"],
        "total_tables": 8
      }
    ]
  }
}
```

### 6.4 File Structure

```
config/industry_templates/
├── retail.yaml
├── healthcare.yaml
├── manufacturing.yaml
├── banking.yaml
├── telecom.yaml
├── insurance.yaml
└── logistics.yaml

core/generation/
├── industry_generator.py     # Orchestrates industry schema generation
├── template_parser.py        # Parses YAML templates
└── schema_builder.py         # Builds complete schemas from templates
```

---

## 7. Data Quality Scoring

### 7.1 Overview

Implement a comprehensive data quality scoring system that quantifies data quality across multiple dimensions, providing actionable metrics for data governance.

### 7.2 Quality Dimensions

Based on industry standards (DAMA, ISO 8000):

| Dimension | Description | Metrics |
|-----------|-------------|---------|
| **Completeness** | Presence of required data | Null rate, missing value % |
| **Uniqueness** | Absence of duplicates | Duplicate rate, unique value ratio |
| **Validity** | Conformance to rules | Format compliance, range compliance |
| **Accuracy** | Correctness of values | Pattern match rate, referential match |
| **Consistency** | Agreement across sources | Cross-field logic, temporal consistency |
| **Timeliness** | Currency of data | Data age, freshness score |

### 7.3 Core Components

#### 7.3.1 Quality Scorer

**New file**: `core/quality/quality_scorer.py`

```python
class DataQualityScorer:
    """Calculates comprehensive data quality scores."""

    def __init__(self, config: QualityConfig = None):
        self.config = config or QualityConfig()
        self.dimension_weights = {
            "completeness": 0.20,
            "uniqueness": 0.15,
            "validity": 0.25,
            "accuracy": 0.20,
            "consistency": 0.15,
            "timeliness": 0.05
        }

    def score(self, df: pl.DataFrame, rules: QualityRules = None) -> QualityReport:
        """Calculate quality scores for a dataframe."""

        scores = {
            "completeness": self._score_completeness(df),
            "uniqueness": self._score_uniqueness(df),
            "validity": self._score_validity(df, rules),
            "accuracy": self._score_accuracy(df, rules),
            "consistency": self._score_consistency(df, rules),
            "timeliness": self._score_timeliness(df)
        }

        # Calculate weighted overall score
        overall = sum(
            scores[dim] * self.dimension_weights[dim]
            for dim in scores
        )

        return QualityReport(
            overall_score=overall,
            dimension_scores=scores,
            column_scores=self._score_columns(df, rules),
            issues=self._identify_issues(df, rules),
            recommendations=self._generate_recommendations(scores)
        )
```

#### 7.3.2 Dimension Scoring Logic

```python
def _score_completeness(self, df: pl.DataFrame) -> DimensionScore:
    """Score data completeness (non-null values)."""

    column_scores = {}
    for col in df.columns:
        null_count = df[col].null_count()
        total = len(df)
        column_scores[col] = 1.0 - (null_count / total)

    return DimensionScore(
        dimension="completeness",
        score=sum(column_scores.values()) / len(column_scores),
        column_scores=column_scores,
        details={
            "total_cells": len(df) * len(df.columns),
            "null_cells": sum(df[col].null_count() for col in df.columns),
            "null_percentage": ...
        }
    )

def _score_uniqueness(self, df: pl.DataFrame) -> DimensionScore:
    """Score data uniqueness (duplicate detection)."""

    # Overall row uniqueness
    total_rows = len(df)
    unique_rows = df.unique().shape[0]
    row_uniqueness = unique_rows / total_rows

    # Column-level uniqueness (for key columns)
    column_scores = {}
    for col in df.columns:
        unique_values = df[col].n_unique()
        column_scores[col] = unique_values / total_rows

    return DimensionScore(
        dimension="uniqueness",
        score=row_uniqueness,
        column_scores=column_scores,
        details={
            "total_rows": total_rows,
            "unique_rows": unique_rows,
            "duplicate_rows": total_rows - unique_rows
        }
    )

def _score_validity(self, df: pl.DataFrame, rules: QualityRules) -> DimensionScore:
    """Score conformance to validation rules."""

    column_scores = {}
    violations = []

    for rule in rules.validity_rules:
        col = rule.column

        if rule.type == "regex":
            matches = df[col].str.contains(rule.pattern).sum()
            score = matches / len(df)

        elif rule.type == "range":
            in_range = df.filter(
                (pl.col(col) >= rule.min) & (pl.col(col) <= rule.max)
            ).shape[0]
            score = in_range / len(df)

        elif rule.type == "enum":
            valid = df.filter(pl.col(col).is_in(rule.allowed_values)).shape[0]
            score = valid / len(df)

        column_scores[col] = score
        if score < 1.0:
            violations.append(RuleViolation(rule, score))

    return DimensionScore(
        dimension="validity",
        score=sum(column_scores.values()) / len(column_scores) if column_scores else 1.0,
        column_scores=column_scores,
        violations=violations
    )

def _score_consistency(self, df: pl.DataFrame, rules: QualityRules) -> DimensionScore:
    """Score cross-field and temporal consistency."""

    consistency_checks = []

    for rule in rules.consistency_rules:
        if rule.type == "cross_field":
            # e.g., end_date >= start_date
            valid = df.filter(eval(rule.expression)).shape[0]
            score = valid / len(df)
            consistency_checks.append(("cross_field", rule.name, score))

        elif rule.type == "sum_check":
            # e.g., line_items should sum to total
            calculated = df[rule.columns].sum(axis=1)
            expected = df[rule.total_column]
            matches = (abs(calculated - expected) < rule.tolerance).sum()
            score = matches / len(df)
            consistency_checks.append(("sum_check", rule.name, score))

    avg_score = sum(c[2] for c in consistency_checks) / len(consistency_checks) if consistency_checks else 1.0

    return DimensionScore(
        dimension="consistency",
        score=avg_score,
        details={"checks": consistency_checks}
    )
```

#### 7.3.3 Quality Report Structure

```python
@dataclass
class QualityReport:
    """Comprehensive data quality report."""

    # Overall scores
    overall_score: float  # 0.0 - 1.0
    quality_grade: str    # A, B, C, D, F

    # Dimension scores
    dimension_scores: Dict[str, DimensionScore]

    # Column-level detail
    column_scores: Dict[str, ColumnQualityScore]

    # Issues found
    issues: List[QualityIssue]
    critical_issues: int
    warning_issues: int
    info_issues: int

    # Recommendations
    recommendations: List[Recommendation]

    # Metadata
    file_path: str
    row_count: int
    column_count: int
    scan_timestamp: datetime
    scan_duration_seconds: float

@dataclass
class ColumnQualityScore:
    column_name: str
    data_type: str
    overall_score: float
    completeness: float
    uniqueness: float
    validity: float
    issues: List[str]

@dataclass
class QualityIssue:
    severity: str  # critical, warning, info
    dimension: str
    column: str
    issue_type: str
    description: str
    affected_rows: int
    sample_values: List[str]
    suggested_fix: str
```

### 7.4 Quality Rules Configuration

```yaml
# config/quality/default_rules.yaml
quality_rules:
  completeness:
    required_columns:
      - column: customer_id
        max_null_pct: 0
      - column: order_date
        max_null_pct: 0
      - column: email
        max_null_pct: 5

  validity:
    - column: email
      type: regex
      pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"

    - column: quantity
      type: range
      min: 1
      max: 10000

    - column: status
      type: enum
      allowed_values: [Active, Inactive, Pending]

  consistency:
    - name: date_order
      type: cross_field
      expression: "pl.col('end_date') >= pl.col('start_date')"

    - name: total_check
      type: sum_check
      columns: [subtotal, tax, shipping]
      total_column: grand_total
      tolerance: 0.01

  uniqueness:
    primary_keys:
      - column: order_id
        must_be_unique: true

    natural_keys:
      - columns: [customer_id, order_date, product_id]
        must_be_unique: true

  accuracy:
    referential:
      - column: customer_id
        reference_file: customers.csv
        reference_column: customer_id
```

### 7.5 New MCP Tools

#### Tool: `04_score_data_quality`

```json
{
  "name": "04_score_data_quality",
  "description": "Calculate comprehensive data quality scores",
  "parameters": {
    "file_path": "path/to/data.csv",
    "rules_file": "path/to/rules.yaml",
    "dimensions": ["completeness", "uniqueness", "validity", "consistency", "accuracy", "timeliness"],
    "reference_files": {
      "customer_id": "path/to/customers.csv"
    }
  },
  "returns": {
    "overall_score": 0.87,
    "quality_grade": "B",
    "dimension_scores": {},
    "critical_issues": 2,
    "recommendations": []
  }
}
```

#### Tool: `04_compare_quality`

```json
{
  "name": "04_compare_quality",
  "description": "Compare quality scores between files or over time",
  "parameters": {
    "file_a": "path/to/current.csv",
    "file_b": "path/to/previous.csv",
    "dimensions": ["all"]
  },
  "returns": {
    "score_delta": 0.05,
    "improved_dimensions": ["completeness"],
    "degraded_dimensions": ["validity"],
    "new_issues": [],
    "resolved_issues": []
  }
}
```

#### Tool: `04_generate_quality_report`

```json
{
  "name": "04_generate_quality_report",
  "description": "Generate detailed quality report in various formats",
  "parameters": {
    "file_paths": ["file1.csv", "file2.csv"],
    "output_format": "markdown|json|html",
    "output_path": "path/to/report.md",
    "include_visualizations": true
  }
}
```

### 7.6 File Structure

```
core/quality/
├── __init__.py
├── quality_scorer.py         # Main scoring engine
├── dimension_scorers/
│   ├── __init__.py
│   ├── completeness.py
│   ├── uniqueness.py
│   ├── validity.py
│   ├── accuracy.py
│   ├── consistency.py
│   └── timeliness.py
├── quality_rules.py          # Rule parsing and validation
├── quality_report.py         # Report generation
└── quality_comparator.py     # Compare quality over time

server/handlers/
└── quality_handlers.py

config/quality/
├── default_rules.yaml
├── financial_rules.yaml
├── retail_rules.yaml
└── healthcare_rules.yaml
```

---

## 8. Implementation Roadmap

### 8.1 Phase 1: Foundation (Core Infrastructure)

**Duration**: Initial implementation phase

| Enhancement | Priority | Dependencies | New Files |
|-------------|----------|--------------|-----------|
| Data Quality Scoring | High | None | 8 files |
| Time Series Patterns | High | None | 4 files |

**Deliverables**:
- Quality scoring engine with 6 dimensions
- Time pattern library for 4 domains
- 4 new MCP tools

---

### 8.2 Phase 2: Data Generation Enhancements

**Duration**: Second implementation phase

| Enhancement | Priority | Dependencies | New Files |
|-------------|----------|--------------|-----------|
| Correlated Data Generation | High | None | 4 files |
| Multi-Currency Support | Medium | Time Series | 4 files |

**Deliverables**:
- Correlation engine with 6 correlation types
- Currency manager with exchange rate generation
- 4 new MCP tools

---

### 8.3 Phase 3: Privacy & Subsetting

**Duration**: Third implementation phase

| Enhancement | Priority | Dependencies | New Files |
|-------------|----------|--------------|-----------|
| PII Detection & Anonymization | High | None | 8 files |
| Data Subsetting | Medium | None | 6 files |

**Deliverables**:
- PII detector with 10+ patterns
- 5 anonymization strategies
- 5 subsetting strategies
- 5 new MCP tools

---

### 8.4 Phase 4: Industry Templates

**Duration**: Fourth implementation phase

| Enhancement | Priority | Dependencies | New Files |
|-------------|----------|--------------|-----------|
| Industry Star Schemas | Medium | Time Series, Correlations | 10 files |

**Deliverables**:
- 5 industry templates (Retail, Healthcare, Manufacturing, Banking, Telecom)
- Template parser and generator
- 2 new MCP tools

---

### 8.5 Summary

| Phase | New Tools | New Core Files | Config Files |
|-------|-----------|----------------|--------------|
| Phase 1 | 4 | 12 | 8 |
| Phase 2 | 4 | 8 | 6 |
| Phase 3 | 5 | 14 | 4 |
| Phase 4 | 2 | 10 | 7 |
| **Total** | **15** | **44** | **25** |

---

## Appendix A: Dependencies

### New Python Packages

```txt
# requirements.txt additions
scipy>=1.11.0          # For copula-based correlations
scikit-learn>=1.3.0    # For anomaly detection in quality scoring
python-dateutil>=2.8.0 # For date handling
rapidfuzz>=3.0.0       # For PII column name matching (already present)
```

### Existing Dependencies (Already Available)

- `polars` - Data manipulation
- `faker` - Synthetic data generation
- `numpy` - Numerical operations
- `pyyaml` - Configuration parsing

---

## Appendix B: Configuration Schema

### Master Configuration Extension

```yaml
# config/default_config.yaml additions
generation:
  time_patterns:
    enabled: true
    default_pattern: "steady"

  correlations:
    enabled: true
    default_template: null

  currency:
    base_currency: "USD"
    default_volatility: "medium"

privacy:
  pii_detection:
    enabled: true
    scan_depth: "standard"
    confidence_threshold: 0.7

  anonymization:
    default_strategy: "synthetic"
    preserve_referential_integrity: true

quality:
  enabled: true
  default_dimensions: ["completeness", "uniqueness", "validity"]
  score_thresholds:
    A: 0.95
    B: 0.85
    C: 0.70
    D: 0.50

subsetting:
  default_strategy: "stratified"
  preserve_distributions: true
  min_rows_per_stratum: 1
```

---

*Document maintained by: MCP Sample Data Server Development Team*
