"""
PII Detector - Identifies personally identifiable information in datasets.

Uses a combination of:
1. Column name heuristics
2. Regex pattern matching
3. Statistical sampling for performance
4. Validation functions for accuracy
"""

import re
import polars as pl
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from pathlib import Path

from .pii_patterns import (
    PIIType,
    PIIPattern,
    PIISensitivity,
    PII_PATTERNS,
    PII_BY_TYPE,
    detect_pii_in_value,
    get_pii_column_candidates
)


@dataclass
class ColumnPIIInfo:
    """PII detection results for a single column"""
    column_name: str
    detected_pii_types: List[PIIType] = field(default_factory=list)
    confidence_scores: Dict[PIIType, float] = field(default_factory=dict)
    sample_matches: Dict[PIIType, List[str]] = field(default_factory=dict)
    detection_method: str = ""  # 'column_name', 'pattern_match', 'both'
    max_sensitivity: Optional[PIISensitivity] = None
    recommendations: List[str] = field(default_factory=list)
    match_percentage: float = 0.0


@dataclass
class PIIDetectionResult:
    """Complete PII detection results for a file/dataframe"""
    file_path: Optional[str] = None
    total_columns: int = 0
    columns_with_pii: int = 0
    pii_summary: Dict[PIIType, int] = field(default_factory=dict)
    column_details: List[ColumnPIIInfo] = field(default_factory=list)
    sensitivity_summary: Dict[str, int] = field(default_factory=dict)
    gdpr_categories: Dict[str, List[str]] = field(default_factory=dict)
    overall_risk_score: float = 0.0
    recommendations: List[str] = field(default_factory=list)


class PIIDetector:
    """
    Detects personally identifiable information in datasets.

    Uses a multi-layered approach:
    1. Column name analysis (fast, high precision for common patterns)
    2. Content sampling (statistical sampling for pattern matching)
    3. Validation (confirmation using validation functions)
    """

    def __init__(
        self,
        sample_size: int = 1000,
        confidence_threshold: float = 0.5,
        custom_patterns: Optional[List[PIIPattern]] = None
    ):
        """
        Initialize the PII detector.

        Args:
            sample_size: Number of rows to sample for pattern detection
            confidence_threshold: Minimum confidence to report PII
            custom_patterns: Additional custom patterns to use
        """
        self.sample_size = sample_size
        self.confidence_threshold = confidence_threshold
        self.patterns = PII_PATTERNS + (custom_patterns or [])

    def detect_in_file(
        self,
        file_path: str,
        columns: Optional[List[str]] = None,
        deep_scan: bool = False
    ) -> PIIDetectionResult:
        """
        Detect PII in a file.

        Args:
            file_path: Path to CSV/Parquet file
            columns: Specific columns to scan (all if None)
            deep_scan: If True, scan all rows instead of sampling

        Returns:
            PIIDetectionResult with findings
        """
        path = Path(file_path)

        if path.suffix.lower() == '.parquet':
            df = pl.read_parquet(path)
        else:
            df = pl.read_csv(path, infer_schema_length=10000)

        return self.detect_in_dataframe(df, columns, deep_scan, str(path))

    def detect_in_dataframe(
        self,
        df: pl.DataFrame,
        columns: Optional[List[str]] = None,
        deep_scan: bool = False,
        source_path: Optional[str] = None
    ) -> PIIDetectionResult:
        """
        Detect PII in a DataFrame.

        Args:
            df: Polars DataFrame to scan
            columns: Specific columns to scan (all if None)
            deep_scan: If True, scan all rows instead of sampling
            source_path: Optional source file path for reporting

        Returns:
            PIIDetectionResult with findings
        """
        result = PIIDetectionResult(
            file_path=source_path,
            total_columns=len(df.columns)
        )

        # Determine columns to scan
        cols_to_scan = columns if columns else df.columns

        # Get sample for pattern matching
        if deep_scan or len(df) <= self.sample_size:
            sample_df = df
        else:
            sample_df = df.sample(n=self.sample_size, seed=42)

        # Analyze each column
        for col_name in cols_to_scan:
            col_info = self._analyze_column(col_name, sample_df, deep_scan)

            if col_info.detected_pii_types:
                result.column_details.append(col_info)
                result.columns_with_pii += 1

                # Update summary counts
                for pii_type in col_info.detected_pii_types:
                    result.pii_summary[pii_type] = result.pii_summary.get(pii_type, 0) + 1

                    # Track GDPR categories
                    pattern = PII_BY_TYPE.get(pii_type)
                    if pattern and pattern.gdpr_category:
                        if pattern.gdpr_category not in result.gdpr_categories:
                            result.gdpr_categories[pattern.gdpr_category] = []
                        result.gdpr_categories[pattern.gdpr_category].append(col_name)

                # Track sensitivity
                if col_info.max_sensitivity:
                    sens_name = col_info.max_sensitivity.value
                    result.sensitivity_summary[sens_name] = result.sensitivity_summary.get(sens_name, 0) + 1

        # Calculate overall risk score
        result.overall_risk_score = self._calculate_risk_score(result)

        # Generate recommendations
        result.recommendations = self._generate_recommendations(result)

        return result

    def _analyze_column(
        self,
        col_name: str,
        df: pl.DataFrame,
        deep_scan: bool
    ) -> ColumnPIIInfo:
        """Analyze a single column for PII"""
        info = ColumnPIIInfo(column_name=col_name)

        # Step 1: Check column name
        name_candidates = get_pii_column_candidates(col_name)
        name_detected = {pii_type: conf for pii_type, conf in name_candidates}

        # Step 2: Check content patterns (for string columns)
        content_detected: Dict[PIIType, float] = {}
        sample_matches: Dict[PIIType, List[str]] = {}

        col_dtype = df[col_name].dtype
        if col_dtype == pl.Utf8 or col_dtype == pl.String:
            content_detected, sample_matches = self._scan_column_content(df, col_name)

        # Step 3: Combine results
        all_types: Set[PIIType] = set(name_detected.keys()) | set(content_detected.keys())

        for pii_type in all_types:
            name_conf = name_detected.get(pii_type, 0)
            content_conf = content_detected.get(pii_type, 0)

            # Calculate combined confidence
            if name_conf > 0 and content_conf > 0:
                # Both methods agree - high confidence
                combined_conf = min(0.98, name_conf * 0.4 + content_conf * 0.6)
                info.detection_method = "both"
            elif name_conf > 0:
                combined_conf = name_conf * 0.7  # Name only - moderate confidence
                info.detection_method = "column_name"
            else:
                combined_conf = content_conf  # Content only
                info.detection_method = "pattern_match"

            if combined_conf >= self.confidence_threshold:
                info.detected_pii_types.append(pii_type)
                info.confidence_scores[pii_type] = combined_conf
                if pii_type in sample_matches:
                    info.sample_matches[pii_type] = sample_matches[pii_type][:5]  # Keep top 5

        # Determine max sensitivity
        if info.detected_pii_types:
            sensitivities = []
            for pii_type in info.detected_pii_types:
                pattern = PII_BY_TYPE.get(pii_type)
                if pattern:
                    sensitivities.append(pattern.sensitivity)

            # Order: CRITICAL > HIGH > MEDIUM > LOW
            sensitivity_order = [
                PIISensitivity.CRITICAL,
                PIISensitivity.HIGH,
                PIISensitivity.MEDIUM,
                PIISensitivity.LOW
            ]
            for sens in sensitivity_order:
                if sens in sensitivities:
                    info.max_sensitivity = sens
                    break

            # Generate column-specific recommendations
            info.recommendations = self._column_recommendations(info)

        return info

    def _scan_column_content(
        self,
        df: pl.DataFrame,
        col_name: str
    ) -> tuple[Dict[PIIType, float], Dict[PIIType, List[str]]]:
        """Scan column content for PII patterns"""
        detected: Dict[PIIType, float] = {}
        samples: Dict[PIIType, List[str]] = {}

        # Get non-null values
        values = df[col_name].drop_nulls().to_list()
        if not values:
            return detected, samples

        total_count = len(values)
        match_counts: Dict[PIIType, int] = {}
        match_samples: Dict[PIIType, List[str]] = {}

        # Check each value against patterns
        for value in values:
            if not isinstance(value, str) or not value.strip():
                continue

            matches = detect_pii_in_value(value)
            for pii_type, conf in matches:
                match_counts[pii_type] = match_counts.get(pii_type, 0) + 1
                if pii_type not in match_samples:
                    match_samples[pii_type] = []
                if len(match_samples[pii_type]) < 10:
                    match_samples[pii_type].append(value)

        # Calculate confidence based on match rate
        for pii_type, count in match_counts.items():
            match_rate = count / total_count

            # Adjust confidence based on match rate
            if match_rate >= 0.8:
                detected[pii_type] = 0.95
            elif match_rate >= 0.5:
                detected[pii_type] = 0.8
            elif match_rate >= 0.2:
                detected[pii_type] = 0.6
            elif match_rate >= 0.05:
                detected[pii_type] = 0.4
            # Below 5% match rate is likely false positives

            if pii_type in match_samples:
                samples[pii_type] = match_samples[pii_type]

        return detected, samples

    def _calculate_risk_score(self, result: PIIDetectionResult) -> float:
        """Calculate overall risk score (0-100)"""
        if not result.column_details:
            return 0.0

        score = 0.0

        # Weight by sensitivity
        sensitivity_weights = {
            'critical': 40,
            'high': 25,
            'medium': 10,
            'low': 5
        }

        for sens, count in result.sensitivity_summary.items():
            weight = sensitivity_weights.get(sens, 5)
            score += count * weight

        # Cap at 100
        return min(100.0, score)

    def _column_recommendations(self, col_info: ColumnPIIInfo) -> List[str]:
        """Generate recommendations for a column"""
        recommendations = []

        for pii_type in col_info.detected_pii_types:
            pattern = PII_BY_TYPE.get(pii_type)
            if not pattern:
                continue

            if pattern.sensitivity == PIISensitivity.CRITICAL:
                recommendations.append(
                    f"CRITICAL: {pattern.description} detected - must be encrypted or removed"
                )
            elif pattern.sensitivity == PIISensitivity.HIGH:
                recommendations.append(
                    f"HIGH: {pattern.description} detected - recommend anonymization or masking"
                )
            elif pattern.sensitivity == PIISensitivity.MEDIUM:
                recommendations.append(
                    f"MEDIUM: {pattern.description} detected - consider pseudonymization"
                )

        return recommendations

    def _generate_recommendations(self, result: PIIDetectionResult) -> List[str]:
        """Generate overall recommendations"""
        recommendations = []

        # Critical findings
        critical_count = result.sensitivity_summary.get('critical', 0)
        if critical_count > 0:
            recommendations.append(
                f"URGENT: {critical_count} column(s) contain CRITICAL PII (SSN, credit cards, etc.). "
                "These must be removed or encrypted before any data sharing."
            )

        # High sensitivity
        high_count = result.sensitivity_summary.get('high', 0)
        if high_count > 0:
            recommendations.append(
                f"ACTION REQUIRED: {high_count} column(s) contain HIGH sensitivity PII. "
                "Apply anonymization before use in development/testing."
            )

        # GDPR categories
        if 'special_category' in result.gdpr_categories:
            recommendations.append(
                "WARNING: GDPR Article 9 special category data detected "
                f"in columns: {', '.join(result.gdpr_categories['special_category'])}. "
                "Extra legal basis required for processing."
            )

        # General recommendation
        if result.columns_with_pii > 0:
            pii_cols = [c.column_name for c in result.column_details]
            recommendations.append(
                f"Use 09_anonymize_file to anonymize these columns: {', '.join(pii_cols)}"
            )

        return recommendations

    def quick_scan(self, df: pl.DataFrame) -> Dict[str, List[PIIType]]:
        """
        Quick scan using only column names (no content analysis).
        Fast but less accurate.

        Args:
            df: DataFrame to scan

        Returns:
            Dict mapping column names to detected PII types
        """
        results = {}

        for col_name in df.columns:
            candidates = get_pii_column_candidates(col_name)
            if candidates:
                results[col_name] = [pii_type for pii_type, _ in candidates]

        return results
