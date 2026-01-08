"""
Consistency Manager - Ensures deterministic anonymization across runs.

Maintains mappings so that:
1. Same input value → same output value (within a session)
2. Reproducible results with the same seed
3. Consistent tokenization for joins to work after anonymization
"""

import hashlib
import random
import string
import json
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Set


@dataclass
class TokenMapping:
    """Mapping from original to tokenized values"""
    column_name: str
    mappings: Dict[str, str] = field(default_factory=dict)
    reverse_mappings: Dict[str, str] = field(default_factory=dict)
    token_counter: int = 0


class ConsistencyManager:
    """
    Manages consistent anonymization mappings.

    Ensures that the same input value always produces the same
    output value within a session or across sessions with the same seed.
    """

    def __init__(
        self,
        seed: Optional[int] = None,
        persistence_path: Optional[str] = None
    ):
        """
        Initialize the consistency manager.

        Args:
            seed: Random seed for reproducibility
            persistence_path: Optional path to persist mappings
        """
        self.seed = seed or random.randint(0, 2**32 - 1)
        self.persistence_path = Path(persistence_path) if persistence_path else None
        self.rng = random.Random(self.seed)

        # Token mappings per column
        self._token_mappings: Dict[str, TokenMapping] = {}

        # Hash salt for deterministic hashing
        self._hash_salt = self._generate_salt()

        # Load persisted mappings if available
        if self.persistence_path and self.persistence_path.exists():
            self._load_mappings()

    def _generate_salt(self) -> str:
        """Generate deterministic salt from seed"""
        return hashlib.sha256(str(self.seed).encode()).hexdigest()[:16]

    def get_consistent_hash(self, value: str, length: int = 16) -> str:
        """
        Get a consistent hash for a value.

        Args:
            value: Value to hash
            length: Desired output length

        Returns:
            Consistent hash string
        """
        salted = f"{self._hash_salt}:{value}"
        return hashlib.sha256(salted.encode()).hexdigest()[:length]

    def get_consistent_token(
        self,
        column_name: str,
        value: str,
        prefix: str = "TKN"
    ) -> str:
        """
        Get a consistent token for a value in a column.

        Args:
            column_name: Name of the column
            value: Original value
            prefix: Token prefix

        Returns:
            Consistent token string
        """
        if column_name not in self._token_mappings:
            self._token_mappings[column_name] = TokenMapping(column_name=column_name)

        mapping = self._token_mappings[column_name]

        if value in mapping.mappings:
            return mapping.mappings[value]

        # Generate new token
        mapping.token_counter += 1
        token = f"{prefix}_{mapping.token_counter:06d}"

        # Store both directions
        mapping.mappings[value] = token
        mapping.reverse_mappings[token] = value

        return token

    def get_original_value(
        self,
        column_name: str,
        token: str
    ) -> Optional[str]:
        """
        Reverse lookup: get original value from token.

        Args:
            column_name: Name of the column
            token: Tokenized value

        Returns:
            Original value if found, None otherwise
        """
        if column_name not in self._token_mappings:
            return None

        return self._token_mappings[column_name].reverse_mappings.get(token)

    def get_consistent_synthetic(
        self,
        value: str,
        synthetic_values: list,
        column_name: Optional[str] = None
    ) -> str:
        """
        Get a consistent synthetic replacement.

        Uses the hash of the value to select from available options,
        ensuring the same input always gets the same synthetic value.

        Args:
            value: Original value
            synthetic_values: List of possible synthetic replacements
            column_name: Optional column name for additional entropy

        Returns:
            Consistent synthetic value
        """
        if not synthetic_values:
            return "SYNTHETIC"

        # Use hash to deterministically select
        hash_input = f"{self._hash_salt}:{column_name or ''}:{value}"
        hash_val = int(hashlib.sha256(hash_input.encode()).hexdigest(), 16)
        index = hash_val % len(synthetic_values)

        return synthetic_values[index]

    def get_consistent_noise(
        self,
        value: float,
        noise_percentage: float = 10.0
    ) -> float:
        """
        Add consistent noise to a numeric value.

        Args:
            value: Original numeric value
            noise_percentage: Maximum percentage noise

        Returns:
            Value with consistent noise added
        """
        # Use value itself as part of the seed for consistency
        value_hash = hashlib.sha256(f"{self._hash_salt}:{value}".encode()).hexdigest()
        local_rng = random.Random(int(value_hash, 16) % (2**32))

        noise_factor = (local_rng.random() * 2 - 1) * (noise_percentage / 100)
        return value * (1 + noise_factor)

    def reset_column(self, column_name: str):
        """Reset mappings for a specific column"""
        if column_name in self._token_mappings:
            del self._token_mappings[column_name]

    def reset_all(self):
        """Reset all mappings"""
        self._token_mappings.clear()

    def get_mapping_stats(self) -> Dict[str, Any]:
        """Get statistics about current mappings"""
        return {
            'seed': self.seed,
            'columns_mapped': len(self._token_mappings),
            'column_details': {
                col: {
                    'unique_values': len(mapping.mappings),
                    'token_count': mapping.token_counter
                }
                for col, mapping in self._token_mappings.items()
            }
        }

    def save_mappings(self, path: Optional[str] = None):
        """
        Save current mappings to file.

        Args:
            path: Path to save to (uses persistence_path if None)
        """
        save_path = Path(path) if path else self.persistence_path
        if not save_path:
            raise ValueError("No path specified for saving mappings")

        data = {
            'seed': self.seed,
            'hash_salt': self._hash_salt,
            'mappings': {
                col: {
                    'mappings': mapping.mappings,
                    'reverse_mappings': mapping.reverse_mappings,
                    'token_counter': mapping.token_counter
                }
                for col, mapping in self._token_mappings.items()
            }
        }

        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'w') as f:
            json.dump(data, f, indent=2)

    def _load_mappings(self):
        """Load mappings from persistence path"""
        if not self.persistence_path or not self.persistence_path.exists():
            return

        try:
            with open(self.persistence_path, 'r') as f:
                data = json.load(f)

            self.seed = data.get('seed', self.seed)
            self._hash_salt = data.get('hash_salt', self._hash_salt)

            for col, mapping_data in data.get('mappings', {}).items():
                self._token_mappings[col] = TokenMapping(
                    column_name=col,
                    mappings=mapping_data.get('mappings', {}),
                    reverse_mappings=mapping_data.get('reverse_mappings', {}),
                    token_counter=mapping_data.get('token_counter', 0)
                )
        except (json.JSONDecodeError, KeyError) as e:
            # Invalid file, start fresh
            pass

    def export_token_lookup(self, column_name: str) -> Dict[str, str]:
        """
        Export token lookup table for a column.

        Useful for creating a separate lookup file.

        Args:
            column_name: Column to export

        Returns:
            Dict mapping tokens to original values
        """
        if column_name not in self._token_mappings:
            return {}

        return dict(self._token_mappings[column_name].reverse_mappings)

    def import_token_lookup(
        self,
        column_name: str,
        lookup: Dict[str, str]
    ):
        """
        Import token lookup table for a column.

        Args:
            column_name: Column to import for
            lookup: Dict mapping tokens to original values
        """
        if column_name not in self._token_mappings:
            self._token_mappings[column_name] = TokenMapping(column_name=column_name)

        mapping = self._token_mappings[column_name]

        for token, original in lookup.items():
            mapping.mappings[original] = token
            mapping.reverse_mappings[token] = original
            # Update counter if needed
            try:
                token_num = int(token.split('_')[-1])
                mapping.token_counter = max(mapping.token_counter, token_num)
            except (ValueError, IndexError):
                pass
