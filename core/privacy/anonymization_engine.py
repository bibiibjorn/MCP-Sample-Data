"""
Anonymization Engine - Applies various anonymization strategies to PII.

Supports multiple strategies:
1. Masking: j***@e***.com
2. Hashing: SHA-256 (preserves joins)
3. Synthetic: Replace with fake data
4. Generalization: 25 → "20-30"
5. Redaction: Complete removal
6. Tokenization: Reversible replacement
"""

import re
import hashlib
import random
import string
from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
from datetime import date, datetime

import polars as pl

from .pii_patterns import PIIType, PIISensitivity, PII_BY_TYPE
from .consistency_manager import ConsistencyManager


class AnonymizationStrategy(Enum):
    """Available anonymization strategies"""
    MASK = "mask"              # Partial masking: j***@e***.com
    HASH = "hash"              # SHA-256 hash (deterministic)
    SYNTHETIC = "synthetic"    # Replace with fake data
    GENERALIZE = "generalize"  # Reduce precision: 25 → "20-30"
    REDACT = "redact"          # Complete removal
    TOKENIZE = "tokenize"      # Reversible token replacement
    SHUFFLE = "shuffle"        # Shuffle values within column
    NOISE = "noise"            # Add statistical noise (numeric)


@dataclass
class ColumnAnonymizationConfig:
    """Configuration for anonymizing a single column"""
    column_name: str
    strategy: AnonymizationStrategy
    pii_type: Optional[PIIType] = None
    preserve_format: bool = True
    preserve_nulls: bool = True
    seed: Optional[int] = None
    custom_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AnonymizationResult:
    """Result of anonymization operation"""
    success: bool
    file_path: Optional[str] = None
    rows_processed: int = 0
    columns_anonymized: int = 0
    column_details: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class AnonymizationEngine:
    """
    Applies anonymization strategies to datasets.

    Supports both automatic (PII-type based) and manual configuration.
    Uses ConsistencyManager to ensure same input → same output across runs.
    """

    # Default strategies by PII type
    DEFAULT_STRATEGIES: Dict[PIIType, AnonymizationStrategy] = {
        # Critical - use strongest methods
        PIIType.SSN: AnonymizationStrategy.REDACT,
        PIIType.CREDIT_CARD: AnonymizationStrategy.MASK,
        PIIType.BANK_ACCOUNT: AnonymizationStrategy.HASH,
        PIIType.IBAN: AnonymizationStrategy.HASH,
        PIIType.PASSPORT: AnonymizationStrategy.REDACT,
        PIIType.DRIVERS_LICENSE: AnonymizationStrategy.REDACT,
        PIIType.MEDICAL_ID: AnonymizationStrategy.HASH,
        PIIType.NATIONAL_ID: AnonymizationStrategy.REDACT,
        PIIType.PASSWORD: AnonymizationStrategy.REDACT,
        PIIType.BIOMETRIC: AnonymizationStrategy.REDACT,
        PIIType.GENETIC: AnonymizationStrategy.REDACT,

        # High - use masking or synthetic
        PIIType.EMAIL: AnonymizationStrategy.MASK,
        PIIType.PHONE: AnonymizationStrategy.MASK,
        PIIType.IP_ADDRESS: AnonymizationStrategy.GENERALIZE,
        PIIType.STREET_ADDRESS: AnonymizationStrategy.SYNTHETIC,
        PIIType.USERNAME: AnonymizationStrategy.HASH,

        # Medium - use generalization or synthetic
        PIIType.FIRST_NAME: AnonymizationStrategy.SYNTHETIC,
        PIIType.LAST_NAME: AnonymizationStrategy.SYNTHETIC,
        PIIType.FULL_NAME: AnonymizationStrategy.SYNTHETIC,
        PIIType.DATE_OF_BIRTH: AnonymizationStrategy.GENERALIZE,
        PIIType.AGE: AnonymizationStrategy.GENERALIZE,
        PIIType.GENDER: AnonymizationStrategy.SHUFFLE,
        PIIType.SALARY: AnonymizationStrategy.NOISE,

        # Low - use generalization
        PIIType.ZIP_CODE: AnonymizationStrategy.GENERALIZE,
        PIIType.CITY: AnonymizationStrategy.SHUFFLE,
        PIIType.STATE: AnonymizationStrategy.SHUFFLE,
        PIIType.COUNTRY: AnonymizationStrategy.SHUFFLE,

        # Special categories
        PIIType.ETHNICITY: AnonymizationStrategy.REDACT,
        PIIType.RELIGION: AnonymizationStrategy.REDACT,
        PIIType.POLITICAL: AnonymizationStrategy.REDACT,
    }

    def __init__(
        self,
        seed: Optional[int] = None,
        consistency_manager: Optional[ConsistencyManager] = None
    ):
        """
        Initialize the anonymization engine.

        Args:
            seed: Random seed for reproducibility
            consistency_manager: For consistent anonymization across runs
        """
        self.seed = seed or random.randint(0, 2**32 - 1)
        self.consistency_manager = consistency_manager or ConsistencyManager(seed=self.seed)
        self._init_faker()

    def _init_faker(self):
        """Initialize fake data generators"""
        # Simple fake data generators (avoiding external dependency)
        self._first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael",
            "Linda", "William", "Elizabeth", "David", "Barbara", "Richard", "Susan",
            "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Christopher",
            "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony", "Margaret"
        ]
        self._last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
            "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
            "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"
        ]
        self._street_types = ["Street", "Avenue", "Boulevard", "Drive", "Lane", "Court", "Way"]
        self._cities = [
            "Springfield", "Franklin", "Greenville", "Bristol", "Clinton", "Madison",
            "Georgetown", "Salem", "Fairview", "Manchester", "Milton", "Newport"
        ]
        self._email_domains = [
            "example.com", "mail.example.org", "test.example.net", "demo.example.io"
        ]

    def anonymize_dataframe(
        self,
        df: pl.DataFrame,
        configs: List[ColumnAnonymizationConfig]
    ) -> tuple[pl.DataFrame, AnonymizationResult]:
        """
        Anonymize a DataFrame according to configurations.

        Args:
            df: Source DataFrame
            configs: List of column configurations

        Returns:
            Tuple of (anonymized DataFrame, result)
        """
        result = AnonymizationResult(
            success=True,
            rows_processed=len(df)
        )

        # Work with a copy
        anonymized_df = df.clone()

        for config in configs:
            if config.column_name not in df.columns:
                result.warnings.append(f"Column '{config.column_name}' not found, skipping")
                continue

            try:
                anonymized_df = self._anonymize_column(anonymized_df, config)
                result.columns_anonymized += 1
                result.column_details[config.column_name] = {
                    'strategy': config.strategy.value,
                    'pii_type': config.pii_type.value if config.pii_type else None,
                    'status': 'success'
                }
            except Exception as e:
                result.errors.append(f"Error anonymizing '{config.column_name}': {str(e)}")
                result.column_details[config.column_name] = {
                    'strategy': config.strategy.value,
                    'status': 'error',
                    'error': str(e)
                }

        if result.errors:
            result.success = len(result.errors) < len(configs)  # Partial success

        return anonymized_df, result

    def anonymize_auto(
        self,
        df: pl.DataFrame,
        pii_columns: Dict[str, PIIType],
        strategy_overrides: Optional[Dict[str, AnonymizationStrategy]] = None
    ) -> tuple[pl.DataFrame, AnonymizationResult]:
        """
        Automatically anonymize based on detected PII types.

        Args:
            df: Source DataFrame
            pii_columns: Dict of column_name → PIIType
            strategy_overrides: Optional per-column strategy overrides

        Returns:
            Tuple of (anonymized DataFrame, result)
        """
        configs = []

        for col_name, pii_type in pii_columns.items():
            # Get strategy (override or default)
            if strategy_overrides and col_name in strategy_overrides:
                strategy = strategy_overrides[col_name]
            else:
                strategy = self.DEFAULT_STRATEGIES.get(pii_type, AnonymizationStrategy.MASK)

            configs.append(ColumnAnonymizationConfig(
                column_name=col_name,
                strategy=strategy,
                pii_type=pii_type,
                seed=self.seed
            ))

        return self.anonymize_dataframe(df, configs)

    def _anonymize_column(
        self,
        df: pl.DataFrame,
        config: ColumnAnonymizationConfig
    ) -> pl.DataFrame:
        """Apply anonymization strategy to a single column"""
        strategy_methods = {
            AnonymizationStrategy.MASK: self._apply_masking,
            AnonymizationStrategy.HASH: self._apply_hashing,
            AnonymizationStrategy.SYNTHETIC: self._apply_synthetic,
            AnonymizationStrategy.GENERALIZE: self._apply_generalization,
            AnonymizationStrategy.REDACT: self._apply_redaction,
            AnonymizationStrategy.TOKENIZE: self._apply_tokenization,
            AnonymizationStrategy.SHUFFLE: self._apply_shuffle,
            AnonymizationStrategy.NOISE: self._apply_noise,
        }

        method = strategy_methods.get(config.strategy)
        if not method:
            raise ValueError(f"Unknown strategy: {config.strategy}")

        return method(df, config)

    def _apply_masking(
        self,
        df: pl.DataFrame,
        config: ColumnAnonymizationConfig
    ) -> pl.DataFrame:
        """Apply partial masking strategy"""
        col = config.column_name
        pii_type = config.pii_type

        def mask_value(val: str) -> str:
            if val is None:
                return None

            if pii_type == PIIType.EMAIL:
                # j***@e***.com
                parts = val.split('@')
                if len(parts) == 2:
                    local, domain = parts
                    domain_parts = domain.split('.')
                    masked_local = local[0] + '***' if local else '***'
                    masked_domain = domain_parts[0][0] + '***' if domain_parts else '***'
                    return f"{masked_local}@{masked_domain}.com"

            elif pii_type == PIIType.PHONE:
                # ***-***-1234
                digits = re.sub(r'\D', '', val)
                if len(digits) >= 4:
                    return f"***-***-{digits[-4:]}"

            elif pii_type == PIIType.CREDIT_CARD:
                # ****-****-****-1234
                digits = re.sub(r'\D', '', val)
                if len(digits) >= 4:
                    return f"****-****-****-{digits[-4:]}"

            elif pii_type == PIIType.SSN:
                # ***-**-1234
                digits = re.sub(r'\D', '', val)
                if len(digits) >= 4:
                    return f"***-**-{digits[-4:]}"

            # Default: mask middle portion
            if len(val) > 4:
                visible = max(1, len(val) // 4)
                return val[:visible] + '*' * (len(val) - visible * 2) + val[-visible:]
            return '*' * len(val)

        return df.with_columns(
            pl.col(col).map_elements(mask_value, return_dtype=pl.Utf8).alias(col)
        )

    def _apply_hashing(
        self,
        df: pl.DataFrame,
        config: ColumnAnonymizationConfig
    ) -> pl.DataFrame:
        """Apply SHA-256 hashing (deterministic, preserves joins)"""
        col = config.column_name
        salt = str(config.seed or self.seed)

        def hash_value(val: str) -> str:
            if val is None:
                return None
            salted = f"{salt}:{val}"
            return hashlib.sha256(salted.encode()).hexdigest()[:16]

        return df.with_columns(
            pl.col(col).map_elements(hash_value, return_dtype=pl.Utf8).alias(col)
        )

    def _apply_synthetic(
        self,
        df: pl.DataFrame,
        config: ColumnAnonymizationConfig
    ) -> pl.DataFrame:
        """Replace with synthetic (fake) data"""
        col = config.column_name
        pii_type = config.pii_type
        rng = random.Random(config.seed or self.seed)

        # Get values to replace
        n_rows = len(df)

        if pii_type == PIIType.FIRST_NAME:
            new_values = [rng.choice(self._first_names) for _ in range(n_rows)]
        elif pii_type == PIIType.LAST_NAME:
            new_values = [rng.choice(self._last_names) for _ in range(n_rows)]
        elif pii_type == PIIType.FULL_NAME:
            new_values = [
                f"{rng.choice(self._first_names)} {rng.choice(self._last_names)}"
                for _ in range(n_rows)
            ]
        elif pii_type == PIIType.EMAIL:
            new_values = [
                f"user{rng.randint(1000, 9999)}@{rng.choice(self._email_domains)}"
                for _ in range(n_rows)
            ]
        elif pii_type == PIIType.PHONE:
            new_values = [
                f"555-{rng.randint(100, 999)}-{rng.randint(1000, 9999)}"
                for _ in range(n_rows)
            ]
        elif pii_type == PIIType.STREET_ADDRESS:
            new_values = [
                f"{rng.randint(100, 9999)} {rng.choice(self._first_names)} {rng.choice(self._street_types)}"
                for _ in range(n_rows)
            ]
        else:
            # Generic replacement
            new_values = [
                f"ANON_{i:06d}" for i in range(n_rows)
            ]

        # Preserve nulls if configured
        if config.preserve_nulls:
            original = df[col].to_list()
            new_values = [
                new_values[i] if original[i] is not None else None
                for i in range(n_rows)
            ]

        return df.with_columns(pl.Series(name=col, values=new_values))

    def _apply_generalization(
        self,
        df: pl.DataFrame,
        config: ColumnAnonymizationConfig
    ) -> pl.DataFrame:
        """Reduce precision through generalization"""
        col = config.column_name
        pii_type = config.pii_type

        if pii_type == PIIType.AGE:
            # Age ranges: 25 → "20-29"
            def generalize_age(val):
                if val is None:
                    return None
                try:
                    age = int(val)
                    decade = (age // 10) * 10
                    return f"{decade}-{decade + 9}"
                except (ValueError, TypeError):
                    return str(val)

            return df.with_columns(
                pl.col(col).map_elements(generalize_age, return_dtype=pl.Utf8).alias(col)
            )

        elif pii_type == PIIType.DATE_OF_BIRTH:
            # DOB → year only or year range
            def generalize_dob(val):
                if val is None:
                    return None
                try:
                    if isinstance(val, (date, datetime)):
                        year = val.year
                    else:
                        # Try to parse
                        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']:
                            try:
                                year = datetime.strptime(str(val), fmt).year
                                break
                            except ValueError:
                                continue
                        else:
                            return str(val)[:4] + "-XX-XX"
                    decade = (year // 10) * 10
                    return f"{decade}s"
                except (ValueError, TypeError):
                    return "Unknown"

            return df.with_columns(
                pl.col(col).map_elements(generalize_dob, return_dtype=pl.Utf8).alias(col)
            )

        elif pii_type == PIIType.ZIP_CODE:
            # ZIP → first 3 digits only
            def generalize_zip(val):
                if val is None:
                    return None
                val_str = str(val).replace('-', '')[:3]
                return f"{val_str}XX" if len(val_str) >= 3 else "XXXXX"

            return df.with_columns(
                pl.col(col).map_elements(generalize_zip, return_dtype=pl.Utf8).alias(col)
            )

        elif pii_type == PIIType.IP_ADDRESS:
            # IP → first two octets
            def generalize_ip(val):
                if val is None:
                    return None
                parts = str(val).split('.')
                if len(parts) >= 2:
                    return f"{parts[0]}.{parts[1]}.0.0/16"
                return "0.0.0.0/0"

            return df.with_columns(
                pl.col(col).map_elements(generalize_ip, return_dtype=pl.Utf8).alias(col)
            )

        elif pii_type == PIIType.SALARY:
            # Salary → ranges
            def generalize_salary(val):
                if val is None:
                    return None
                try:
                    salary = float(val)
                    if salary < 30000:
                        return "<$30K"
                    elif salary < 50000:
                        return "$30K-$50K"
                    elif salary < 75000:
                        return "$50K-$75K"
                    elif salary < 100000:
                        return "$75K-$100K"
                    elif salary < 150000:
                        return "$100K-$150K"
                    else:
                        return "$150K+"
                except (ValueError, TypeError):
                    return str(val)

            return df.with_columns(
                pl.col(col).map_elements(generalize_salary, return_dtype=pl.Utf8).alias(col)
            )

        # Default: truncate to first few characters
        return df.with_columns(
            pl.col(col).cast(pl.Utf8).str.slice(0, 3).alias(col)
        )

    def _apply_redaction(
        self,
        df: pl.DataFrame,
        config: ColumnAnonymizationConfig
    ) -> pl.DataFrame:
        """Complete removal/redaction"""
        col = config.column_name
        return df.with_columns(
            pl.lit("[REDACTED]").alias(col)
        )

    def _apply_tokenization(
        self,
        df: pl.DataFrame,
        config: ColumnAnonymizationConfig
    ) -> pl.DataFrame:
        """Replace with consistent tokens (reversible with lookup)"""
        col = config.column_name

        # Build token mapping using consistency manager
        unique_values = df[col].unique().drop_nulls().to_list()
        token_map = {}

        for val in unique_values:
            token = self.consistency_manager.get_consistent_token(col, str(val))
            token_map[val] = token

        def tokenize(val):
            if val is None:
                return None
            return token_map.get(val, f"TOKEN_{hash(val) % 10000:04d}")

        return df.with_columns(
            pl.col(col).map_elements(tokenize, return_dtype=pl.Utf8).alias(col)
        )

    def _apply_shuffle(
        self,
        df: pl.DataFrame,
        config: ColumnAnonymizationConfig
    ) -> pl.DataFrame:
        """Shuffle values within column (breaks correlation)"""
        col = config.column_name
        rng = random.Random(config.seed or self.seed)

        values = df[col].to_list()
        non_null_indices = [i for i, v in enumerate(values) if v is not None]
        non_null_values = [values[i] for i in non_null_indices]

        rng.shuffle(non_null_values)

        # Put shuffled values back
        shuffled = values.copy()
        for i, idx in enumerate(non_null_indices):
            shuffled[idx] = non_null_values[i]

        return df.with_columns(pl.Series(name=col, values=shuffled))

    def _apply_noise(
        self,
        df: pl.DataFrame,
        config: ColumnAnonymizationConfig
    ) -> pl.DataFrame:
        """Add statistical noise to numeric columns"""
        col = config.column_name
        rng = random.Random(config.seed or self.seed)

        # Get noise parameters
        noise_pct = config.custom_params.get('noise_percentage', 10)

        values = df[col].to_list()
        noisy_values = []

        for val in values:
            if val is None:
                noisy_values.append(None)
            else:
                try:
                    num_val = float(val)
                    noise = num_val * (noise_pct / 100) * (rng.random() * 2 - 1)
                    noisy_values.append(num_val + noise)
                except (ValueError, TypeError):
                    noisy_values.append(val)

        return df.with_columns(pl.Series(name=col, values=noisy_values))
