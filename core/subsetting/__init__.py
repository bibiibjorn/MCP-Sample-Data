"""
Data Subsetting module for creating representative test datasets.

Provides tools to create smaller, representative subsets of data while:
1. Maintaining referential integrity across related tables
2. Preserving statistical distributions
3. Supporting various sampling strategies
"""

from .subset_engine import (
    SubsetEngine,
    SubsetConfig,
    SubsetResult,
    SamplingStrategy
)

from .distribution_analyzer import (
    DistributionAnalyzer,
    DistributionComparison
)

__all__ = [
    # Subset Engine
    'SubsetEngine',
    'SubsetConfig',
    'SubsetResult',
    'SamplingStrategy',

    # Distribution Analysis
    'DistributionAnalyzer',
    'DistributionComparison'
]
