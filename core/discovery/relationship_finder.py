"""
Relationship Finder Module
Detects potential relationships between multiple files
"""
import polars as pl
from typing import Dict, Any, List, Optional
import logging

logger = logging.getLogger(__name__)

# Token optimization constants
MAX_UNIQUE_SAMPLE = 10000       # Max unique values to compare (prevents memory issues on large columns)
SAMPLE_MATCH_LIMIT = 5          # Sample matches to return in response
RELATIONSHIP_LIMIT = 50         # Max relationships to return per query


class RelationshipFinder:
    """Finds relationships between tables"""

    def __init__(self, confidence_threshold: float = 0.3):
        # Lower threshold (0.3) to detect potential relationships even with
        # partial value overlap - useful for data quality analysis and
        # finding relationships where column names don't match
        self.confidence_threshold = confidence_threshold

    def find(self, tables: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Find potential relationships between tables.

        Args:
            tables: Dict of table_name -> {'path': str, 'df': DataFrame}

        Returns:
            List of detected relationships (limited for token efficiency)
        """
        relationships = []
        table_names = list(tables.keys())

        # Compare each pair of tables
        for i, table1_name in enumerate(table_names):
            for table2_name in table_names[i + 1:]:
                table1 = tables[table1_name]
                table2 = tables[table2_name]

                # Find potential join columns
                matches = self._find_column_matches(
                    table1_name, table1['df'],
                    table2_name, table2['df']
                )
                relationships.extend(matches)

        # Sort by confidence and limit results
        relationships.sort(key=lambda x: -x.get('confidence', 0))

        return relationships[:RELATIONSHIP_LIMIT]

    def _find_column_matches(
        self,
        table1_name: str,
        df1: pl.DataFrame,
        table2_name: str,
        df2: pl.DataFrame
    ) -> List[Dict[str, Any]]:
        """Find matching columns between two tables"""
        matches = []

        # Minimum unique values to consider a column for relationships
        # Avoids false positives from empty/near-empty columns
        MIN_UNIQUE_VALUES = 3

        for col1 in df1.columns:
            # Skip columns with too few unique values
            unique1 = df1[col1].drop_nulls().n_unique()
            if unique1 < MIN_UNIQUE_VALUES:
                continue

            for col2 in df2.columns:
                # Skip columns with too few unique values
                unique2 = df2[col2].drop_nulls().n_unique()
                if unique2 < MIN_UNIQUE_VALUES:
                    continue

                # Skip if different types
                if not self._compatible_types(df1[col1].dtype, df2[col2].dtype):
                    continue

                # Calculate match score
                match_info = self._calculate_match(
                    col1, df1[col1],
                    col2, df2[col2]
                )

                if match_info['confidence'] >= self.confidence_threshold:
                    matches.append({
                        'source_table': table1_name,
                        'source_column': col1,
                        'target_table': table2_name,
                        'target_column': col2,
                        'relationship_type': match_info['type'],
                        'confidence': match_info['confidence'],
                        'sample_matches': match_info.get('sample_matches', []),
                        'reasoning': match_info['reasoning']
                    })

        return matches

    def _compatible_types(self, dtype1, dtype2) -> bool:
        """Check if two data types are compatible for joining"""
        numeric_types = {pl.Int64, pl.Int32, pl.Int16, pl.Int8,
                        pl.UInt64, pl.UInt32, pl.UInt16, pl.UInt8,
                        pl.Float64, pl.Float32}
        string_types = {pl.Utf8, pl.String}

        if dtype1 in numeric_types and dtype2 in numeric_types:
            return True
        if dtype1 in string_types and dtype2 in string_types:
            return True
        if dtype1 == dtype2:
            return True

        return False

    def _calculate_match(
        self,
        col1_name: str,
        col1_data: pl.Series,
        col2_name: str,
        col2_data: pl.Series
    ) -> Dict[str, Any]:
        """Calculate match score between two columns with memory-efficient sampling."""
        reasoning = []
        confidence = 0.0

        # Check column name similarity
        name_similarity = self._name_similarity(col1_name, col2_name)
        if name_similarity > 0.8:
            confidence += 0.3
            reasoning.append(f"Similar names ({name_similarity:.0%})")
        elif name_similarity > 0.5:
            confidence += 0.15
            reasoning.append(f"Some name similarity ({name_similarity:.0%})")

        # Check value overlap with memory-efficient sampling
        try:
            # Get unique counts first (cheap operation)
            unique1 = col1_data.n_unique()
            unique2 = col2_data.n_unique()

            # Use sampling for high-cardinality columns to save memory
            if unique1 > MAX_UNIQUE_SAMPLE or unique2 > MAX_UNIQUE_SAMPLE:
                # Sample-based estimation for large columns
                sample1 = col1_data.drop_nulls().unique().head(MAX_UNIQUE_SAMPLE)
                sample2 = col2_data.drop_nulls().unique().head(MAX_UNIQUE_SAMPLE)
                vals1 = set(sample1.to_list())
                vals2 = set(sample2.to_list())
                is_sampled = True
            else:
                vals1 = set(col1_data.unique().drop_nulls().to_list())
                vals2 = set(col2_data.unique().drop_nulls().to_list())
                is_sampled = False

            if vals1 and vals2:
                overlap = len(vals1 & vals2)
                max_overlap = min(len(vals1), len(vals2))
                overlap_ratio = overlap / max_overlap if max_overlap > 0 else 0

                # Value overlap is a strong signal - increase confidence boosts
                if overlap_ratio > 0.9:
                    confidence += 0.7
                    reasoning.append(f"High overlap ({overlap_ratio:.0%})")
                elif overlap_ratio > 0.5:
                    confidence += 0.5
                    reasoning.append(f"Moderate overlap ({overlap_ratio:.0%})")
                elif overlap_ratio > 0.2:
                    confidence += 0.3
                    reasoning.append(f"Some overlap ({overlap_ratio:.0%})")
                elif overlap_ratio > 0.05:
                    confidence += 0.15
                    reasoning.append(f"Minimal overlap ({overlap_ratio:.0%})")

                if is_sampled:
                    reasoning.append("(sampled)")

                # Determine relationship type
                len1 = len(col1_data)
                len2 = len(col2_data)

                if unique1 == len1 and unique2 < len2:
                    rel_type = '1:N'
                    reasoning.append(f"{col1_name}: PK candidate")
                elif unique2 == len2 and unique1 < len1:
                    rel_type = 'N:1'
                    reasoning.append(f"{col2_name}: PK candidate")
                elif unique1 == len1 and unique2 == len2:
                    rel_type = '1:1'
                else:
                    rel_type = 'N:N'

                # Get sample matches (truncate long values)
                sample_matches = []
                for val in list(vals1 & vals2)[:SAMPLE_MATCH_LIMIT]:
                    if isinstance(val, str) and len(val) > 50:
                        sample_matches.append(val[:47] + '...')
                    else:
                        sample_matches.append(val)

                return {
                    'confidence': round(min(confidence, 1.0), 2),
                    'type': rel_type,
                    'reasoning': reasoning,
                    'sample_matches': sample_matches
                }

        except Exception as e:
            logger.debug(f"Error comparing columns: {e}")

        return {
            'confidence': round(confidence, 2),
            'type': 'unknown',
            'reasoning': reasoning
        }

    def _name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two column names"""
        # Normalize names
        n1 = name1.lower().replace('_', '').replace('-', '')
        n2 = name2.lower().replace('_', '').replace('-', '')

        if n1 == n2:
            return 1.0

        # Check if one contains the other
        if n1 in n2 or n2 in n1:
            return 0.8

        # Check common suffixes/prefixes
        common_suffixes = ['id', 'key', 'code', 'name', 'date']
        for suffix in common_suffixes:
            if n1.endswith(suffix) and n2.endswith(suffix):
                prefix1 = n1[:-len(suffix)]
                prefix2 = n2[:-len(suffix)]
                if prefix1 == prefix2:
                    return 0.9

        # Simple character overlap
        chars1 = set(n1)
        chars2 = set(n2)
        overlap = len(chars1 & chars2) / max(len(chars1), len(chars2))

        return overlap

    def find_relationships(
        self,
        dataframes: Dict[str, pl.DataFrame],
        primary_file: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Find potential relationships between multiple files.

        Args:
            dataframes: Dict of file_path -> DataFrame
            primary_file: Optional primary file to focus on

        Returns:
            Relationship discovery results
        """
        try:
            # Convert dataframes dict to the format expected by find()
            tables = {
                path: {'path': path, 'df': df}
                for path, df in dataframes.items()
            }

            relationships = self.find(tables)

            # If primary file specified, prioritize relationships involving it
            if primary_file:
                primary_rels = [
                    r for r in relationships
                    if r.get('source_table') == primary_file or r.get('target_table') == primary_file
                ]
                other_rels = [
                    r for r in relationships
                    if r not in primary_rels
                ]
                relationships = primary_rels + other_rels

            return {
                'success': True,
                'file_count': len(dataframes),
                'relationships_found': len(relationships),
                'relationships': relationships,
                'primary_file': primary_file
            }

        except Exception as e:
            logger.error(f"Error finding relationships: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def suggest_star_schema(self, tables: Dict[str, Dict[str, Any]], relationships: List[Dict]) -> Dict[str, Any]:
        """Suggest a star schema design based on detected relationships"""
        # Identify potential fact and dimension tables
        table_info = {}
        for name, info in tables.items():
            df = info['df']
            row_count = len(df)
            avg_unique_ratio = sum(df[col].n_unique() / row_count for col in df.columns) / len(df.columns)

            table_info[name] = {
                'row_count': row_count,
                'column_count': len(df.columns),
                'avg_unique_ratio': avg_unique_ratio
            }

        # Tables with more rows and lower unique ratios are likely facts
        sorted_tables = sorted(table_info.items(),
                               key=lambda x: (-x[1]['row_count'], x[1]['avg_unique_ratio']))

        fact_candidates = [t[0] for t in sorted_tables[:len(sorted_tables) // 2 + 1]]
        dim_candidates = [t[0] for t in sorted_tables[len(sorted_tables) // 2 + 1:]]

        return {
            'suggested_facts': fact_candidates,
            'suggested_dimensions': dim_candidates,
            'relationships': relationships,
            'table_analysis': table_info
        }
