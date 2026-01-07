"""
Distribution Sampler Module
Provides various statistical distributions for data generation
"""
import numpy as np
from typing import List, Optional, Union
import logging

logger = logging.getLogger(__name__)


class DistributionSampler:
    """Samples values from various statistical distributions"""

    def __init__(self, seed: Optional[int] = None):
        if seed:
            np.random.seed(seed)

    def uniform(
        self,
        count: int,
        low: float = 0.0,
        high: float = 1.0,
        as_int: bool = False
    ) -> List[Union[int, float]]:
        """Sample from uniform distribution"""
        values = np.random.uniform(low, high, count)
        if as_int:
            return [int(round(v)) for v in values]
        return [round(float(v), 2) for v in values]

    def normal(
        self,
        count: int,
        mean: float = 0.0,
        std: float = 1.0,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        as_int: bool = False
    ) -> List[Union[int, float]]:
        """Sample from normal (Gaussian) distribution"""
        values = np.random.normal(mean, std, count)

        if min_val is not None or max_val is not None:
            values = np.clip(values,
                           min_val if min_val is not None else -np.inf,
                           max_val if max_val is not None else np.inf)

        if as_int:
            return [int(round(v)) for v in values]
        return [round(float(v), 2) for v in values]

    def lognormal(
        self,
        count: int,
        mean: float = 0.0,
        sigma: float = 1.0,
        scale: float = 1.0,
        as_int: bool = False
    ) -> List[Union[int, float]]:
        """Sample from lognormal distribution (good for amounts: many small, few large)"""
        values = np.random.lognormal(mean, sigma, count) * scale

        if as_int:
            return [int(round(v)) for v in values]
        return [round(float(v), 2) for v in values]

    def exponential(
        self,
        count: int,
        scale: float = 1.0,
        min_val: float = 0.0,
        as_int: bool = False
    ) -> List[Union[int, float]]:
        """Sample from exponential distribution"""
        values = np.random.exponential(scale, count) + min_val

        if as_int:
            return [int(round(v)) for v in values]
        return [round(float(v), 2) for v in values]

    def poisson(
        self,
        count: int,
        lam: float = 1.0
    ) -> List[int]:
        """Sample from Poisson distribution (good for counts)"""
        values = np.random.poisson(lam, count)
        return [int(v) for v in values]

    def binomial(
        self,
        count: int,
        n: int,
        p: float
    ) -> List[int]:
        """Sample from binomial distribution"""
        values = np.random.binomial(n, p, count)
        return [int(v) for v in values]

    def pareto(
        self,
        count: int,
        alpha: float = 1.0,
        scale: float = 1.0,
        as_int: bool = False
    ) -> List[Union[int, float]]:
        """Sample from Pareto distribution (80/20 rule)"""
        values = (np.random.pareto(alpha, count) + 1) * scale

        if as_int:
            return [int(round(v)) for v in values]
        return [round(float(v), 2) for v in values]

    def beta(
        self,
        count: int,
        a: float = 2.0,
        b: float = 5.0,
        min_val: float = 0.0,
        max_val: float = 1.0
    ) -> List[float]:
        """Sample from beta distribution (good for proportions/percentages)"""
        values = np.random.beta(a, b, count)
        # Scale to range
        values = values * (max_val - min_val) + min_val
        return [round(float(v), 4) for v in values]

    def weighted_choice(
        self,
        count: int,
        values: List,
        weights: Optional[List[float]] = None
    ) -> List:
        """Sample from discrete values with optional weights"""
        if weights:
            # Normalize weights
            total = sum(weights)
            probs = [w / total for w in weights]
            indices = np.random.choice(len(values), count, p=probs)
        else:
            indices = np.random.choice(len(values), count)

        return [values[i] for i in indices]

    def sequential(
        self,
        count: int,
        start: int = 1,
        step: int = 1
    ) -> List[int]:
        """Generate sequential integers"""
        return list(range(start, start + count * step, step))

    def cyclic(
        self,
        count: int,
        values: List
    ) -> List:
        """Cycle through values repeatedly"""
        result = []
        for i in range(count):
            result.append(values[i % len(values)])
        return result
