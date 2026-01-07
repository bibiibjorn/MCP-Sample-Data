"""
Mapping Discovery Module
Fuzzy/semantic column matching between files
"""
import polars as pl
from typing import Dict, Any, List, Optional, Set
from rapidfuzz import fuzz, process
import logging

logger = logging.getLogger(__name__)


class MappingDiscovery:
    """Discovers mappings between columns across files"""

    def __init__(self, match_threshold: float = 0.7):
        self.match_threshold = match_threshold

    def discover(
        self,
        files: List[str],
        source_file: str,
        detect_hierarchies: bool = True
    ) -> Dict[str, Any]:
        """
        Discover mappings between files.

        Args:
            files: List of file paths to analyze
            source_file: Primary source data file
            detect_hierarchies: Look for hierarchical structures

        Returns:
            Discovered mappings and hierarchies
        """
        try:
            # Load all files
            loaded_files = {}
            for path in files:
                df = self._load_file(path)
                loaded_files[path] = df

            source_df = loaded_files.get(source_file)
            if source_df is None:
                return {'success': False, 'error': f'Source file not found: {source_file}'}

            # Discover column mappings
            discovered_mappings = []

            for target_path, target_df in loaded_files.items():
                if target_path == source_file:
                    continue

                mappings = self._find_column_mappings(
                    source_file, source_df,
                    target_path, target_df
                )
                discovered_mappings.extend(mappings)

            # Detect hierarchies if requested
            hierarchies_found = []
            if detect_hierarchies:
                for path, df in loaded_files.items():
                    hierarchy = self._detect_hierarchy(path, df)
                    if hierarchy:
                        hierarchies_found.append(hierarchy)

            # Suggest join paths
            suggested_paths = self._suggest_join_paths(discovered_mappings, loaded_files)

            return {
                'success': True,
                'discovered_mappings': discovered_mappings,
                'hierarchies_found': hierarchies_found,
                'suggested_join_paths': suggested_paths
            }

        except Exception as e:
            logger.error(f"Error discovering mappings: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _find_column_mappings(
        self,
        source_path: str,
        source_df: pl.DataFrame,
        target_path: str,
        target_df: pl.DataFrame
    ) -> List[Dict[str, Any]]:
        """Find potential column mappings between two DataFrames"""
        mappings = []

        for source_col in source_df.columns:
            source_values = self._get_unique_values(source_df[source_col])
            if not source_values:
                continue

            for target_col in target_df.columns:
                target_values = self._get_unique_values(target_df[target_col])
                if not target_values:
                    continue

                # Try exact matching first
                exact_matches = source_values & target_values
                if exact_matches:
                    exact_ratio = len(exact_matches) / len(source_values)
                    if exact_ratio > self.match_threshold:
                        mappings.append({
                            'source_file': source_path,
                            'source_column': source_col,
                            'target_file': target_path,
                            'target_column': target_col,
                            'match_type': 'exact',
                            'match_confidence': round(exact_ratio, 2),
                            'sample_matches': [
                                {'source_value': v, 'target_value': v, 'score': 1.0}
                                for v in list(exact_matches)[:5]
                            ]
                        })
                        continue

                # Try fuzzy matching
                fuzzy_matches = self._fuzzy_match_values(source_values, target_values)
                if fuzzy_matches['match_ratio'] > self.match_threshold:
                    mappings.append({
                        'source_file': source_path,
                        'source_column': source_col,
                        'target_file': target_path,
                        'target_column': target_col,
                        'match_type': 'fuzzy',
                        'match_confidence': round(fuzzy_matches['match_ratio'], 2),
                        'sample_matches': fuzzy_matches['sample_matches']
                    })

        return mappings

    def _fuzzy_match_values(
        self,
        source_values: Set[str],
        target_values: Set[str]
    ) -> Dict[str, Any]:
        """Fuzzy match values between two sets"""
        target_list = list(target_values)
        matches = []
        match_count = 0

        for source_val in list(source_values)[:100]:  # Limit for performance
            result = process.extractOne(
                str(source_val),
                target_list,
                scorer=fuzz.ratio
            )

            if result and result[1] >= self.match_threshold * 100:
                matches.append({
                    'source_value': source_val,
                    'target_value': result[0],
                    'score': round(result[1] / 100, 2)
                })
                match_count += 1

        match_ratio = match_count / len(source_values) if source_values else 0

        return {
            'match_ratio': match_ratio,
            'sample_matches': matches[:5]
        }

    def _detect_hierarchy(self, path: str, df: pl.DataFrame) -> Optional[Dict[str, Any]]:
        """Detect if a DataFrame contains hierarchical structure"""
        # Look for common hierarchy indicators
        hierarchy_indicators = [
            'level', 'parent', 'child', 'row_number', 'sequence',
            'rollup', 'total', 'subtotal', 'aggregation'
        ]

        hierarchy_cols = []
        for col in df.columns:
            col_lower = col.lower()
            if any(ind in col_lower for ind in hierarchy_indicators):
                hierarchy_cols.append(col)

        if not hierarchy_cols:
            return None

        # Look for aggregation lines
        aggregation_lines = []
        for col in df.columns:
            unique_vals = df[col].unique().to_list()
            for val in unique_vals:
                if val and isinstance(val, str):
                    val_lower = val.lower()
                    if any(x in val_lower for x in ['total', 'sum', 'subtotal']):
                        aggregation_lines.append(val)

        return {
            'file': path,
            'hierarchy_columns': hierarchy_cols,
            'levels': len(hierarchy_cols),
            'aggregation_lines': list(set(aggregation_lines))[:10]
        }

    def _suggest_join_paths(
        self,
        mappings: List[Dict[str, Any]],
        files: Dict[str, pl.DataFrame]
    ) -> List[Dict[str, Any]]:
        """Suggest join paths based on discovered mappings"""
        paths = []

        # Group mappings by confidence
        high_conf_mappings = [m for m in mappings if m['match_confidence'] >= 0.9]

        for mapping in high_conf_mappings:
            path = {
                'path': [f"{mapping['source_column']} -> {mapping['target_column']}"],
                'files_involved': [mapping['source_file'], mapping['target_file']]
            }
            paths.append(path)

        return paths

    def _load_file(self, path: str) -> pl.DataFrame:
        """Load a file into a DataFrame"""
        if path.endswith('.csv'):
            return pl.read_csv(path)
        elif path.endswith('.parquet'):
            return pl.read_parquet(path)
        elif path.endswith('.xlsx') or path.endswith('.xls'):
            return pl.read_excel(path)
        else:
            raise ValueError(f"Unsupported file format: {path}")

    def _get_unique_values(self, series: pl.Series, max_values: int = 1000) -> Set[str]:
        """Get unique string values from a series"""
        try:
            unique = series.drop_nulls().unique()
            if len(unique) > max_values:
                unique = unique.head(max_values)
            return set(str(v) for v in unique.to_list() if v is not None)
        except:
            return set()
