"""
MCP Handlers for PII detection and anonymization tools.

Tools:
- 09_detect_pii: Scan files for personally identifiable information
- 09_anonymize_file: Apply anonymization strategies to PII columns
- 09_generate_anonymization_report: Create compliance documentation
"""

import os
from typing import Any, Dict, List, Optional
from pathlib import Path

import polars as pl

from core.privacy import (
    PIIDetector,
    PIIDetectionResult,
    AnonymizationEngine,
    AnonymizationStrategy,
    ColumnAnonymizationConfig,
    ConsistencyManager,
    PIIType
)
from server.tool_schemas import TOOL_SCHEMAS


def register_privacy_handlers(registry):
    """Register all privacy-related tool handlers"""

    # =========================================================================
    # 09_detect_pii
    # =========================================================================
    def detect_pii(
        file_path: str,
        columns: Optional[List[str]] = None,
        deep_scan: bool = False,
        sample_size: int = 1000,
        confidence_threshold: float = 0.5
    ) -> Dict[str, Any]:
        """
        Detect personally identifiable information in a file.

        Args:
            file_path: Path to CSV or Parquet file
            columns: Specific columns to scan (all if not specified)
            deep_scan: If True, scan all rows instead of sampling
            sample_size: Number of rows to sample for detection
            confidence_threshold: Minimum confidence to report (0.0-1.0)

        Returns:
            Detection results with PII findings
        """
        try:
            path = Path(file_path)
            if not path.exists():
                return {
                    'success': False,
                    'error': f"File not found: {file_path}"
                }

            # Initialize detector
            detector = PIIDetector(
                sample_size=sample_size,
                confidence_threshold=confidence_threshold
            )

            # Run detection
            result = detector.detect_in_file(
                str(path),
                columns=columns,
                deep_scan=deep_scan
            )

            # Format output
            columns_with_pii = []
            for col_info in result.column_details:
                col_data = {
                    'column': col_info.column_name,
                    'pii_types': [t.value for t in col_info.detected_pii_types],
                    'confidence': {
                        t.value: round(c, 3)
                        for t, c in col_info.confidence_scores.items()
                    },
                    'sensitivity': col_info.max_sensitivity.value if col_info.max_sensitivity else None,
                    'detection_method': col_info.detection_method,
                    'sample_matches': {
                        t.value: matches
                        for t, matches in col_info.sample_matches.items()
                    },
                    'recommendations': col_info.recommendations
                }
                columns_with_pii.append(col_data)

            return {
                'success': True,
                'file_path': str(path),
                'total_columns': result.total_columns,
                'columns_with_pii': result.columns_with_pii,
                'pii_columns': columns_with_pii,
                'pii_summary': {t.value: c for t, c in result.pii_summary.items()},
                'sensitivity_summary': result.sensitivity_summary,
                'gdpr_categories': result.gdpr_categories,
                'risk_score': round(result.overall_risk_score, 1),
                'risk_level': _get_risk_level(result.overall_risk_score),
                'recommendations': result.recommendations
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    schema = TOOL_SCHEMAS['09_detect_pii']
    registry.register(
        '09_detect_pii',
        detect_pii,
        'privacy',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # =========================================================================
    # 09_anonymize_file
    # =========================================================================
    def anonymize_file(
        file_path: str,
        output_path: Optional[str] = None,
        columns: Optional[Dict[str, str]] = None,
        auto_detect: bool = True,
        strategy_overrides: Optional[Dict[str, str]] = None,
        preserve_nulls: bool = True,
        seed: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Anonymize PII in a file.

        Args:
            file_path: Path to source file
            output_path: Path for anonymized output (default: adds _anonymized suffix)
            columns: Manual column→strategy mapping (e.g., {"email": "mask", "ssn": "redact"})
            auto_detect: If True, auto-detect PII and apply default strategies
            strategy_overrides: Override default strategies for auto-detected columns
            preserve_nulls: Keep null values as null (not anonymize them)
            seed: Random seed for reproducibility

        Returns:
            Anonymization results
        """
        try:
            path = Path(file_path)
            if not path.exists():
                return {
                    'success': False,
                    'error': f"File not found: {file_path}"
                }

            # Load file
            if path.suffix.lower() == '.parquet':
                df = pl.read_parquet(path)
            else:
                df = pl.read_csv(path, infer_schema_length=10000)

            # Determine output path
            if output_path:
                out_path = Path(output_path)
            else:
                out_path = path.parent / f"{path.stem}_anonymized{path.suffix}"

            # Initialize engine
            consistency_mgr = ConsistencyManager(seed=seed)
            engine = AnonymizationEngine(seed=seed, consistency_manager=consistency_mgr)

            # Build column configurations
            pii_columns: Dict[str, PIIType] = {}
            manual_configs: List[ColumnAnonymizationConfig] = []

            # Auto-detect if requested
            if auto_detect:
                detector = PIIDetector()
                detection = detector.detect_in_dataframe(df)

                for col_info in detection.column_details:
                    if col_info.detected_pii_types:
                        # Use highest confidence PII type
                        best_type = max(
                            col_info.confidence_scores.items(),
                            key=lambda x: x[1]
                        )[0]
                        pii_columns[col_info.column_name] = best_type

            # Add manual column configurations
            if columns:
                for col_name, strategy_name in columns.items():
                    try:
                        strategy = AnonymizationStrategy(strategy_name.lower())
                    except ValueError:
                        return {
                            'success': False,
                            'error': f"Unknown strategy: {strategy_name}. Valid: {[s.value for s in AnonymizationStrategy]}"
                        }

                    manual_configs.append(ColumnAnonymizationConfig(
                        column_name=col_name,
                        strategy=strategy,
                        preserve_nulls=preserve_nulls,
                        seed=seed
                    ))

                    # Remove from auto-detect if manually specified
                    if col_name in pii_columns:
                        del pii_columns[col_name]

            # Parse strategy overrides
            parsed_overrides = None
            if strategy_overrides:
                parsed_overrides = {}
                for col, strat in strategy_overrides.items():
                    try:
                        parsed_overrides[col] = AnonymizationStrategy(strat.lower())
                    except ValueError:
                        pass

            # Apply anonymization
            if pii_columns:
                anonymized_df, auto_result = engine.anonymize_auto(
                    df,
                    pii_columns,
                    strategy_overrides=parsed_overrides
                )
            else:
                anonymized_df = df
                auto_result = None

            # Apply manual configs
            if manual_configs:
                anonymized_df, manual_result = engine.anonymize_dataframe(
                    anonymized_df,
                    manual_configs
                )
            else:
                manual_result = None

            # Write output
            if out_path.suffix.lower() == '.parquet':
                anonymized_df.write_parquet(out_path)
            else:
                anonymized_df.write_csv(out_path)

            # Combine results
            all_columns = {}
            if auto_result:
                all_columns.update(auto_result.column_details)
            if manual_result:
                all_columns.update(manual_result.column_details)

            return {
                'success': True,
                'input_file': str(path),
                'output_file': str(out_path),
                'rows_processed': len(df),
                'columns_anonymized': len(all_columns),
                'column_details': all_columns,
                'seed_used': seed or engine.seed,
                'errors': (auto_result.errors if auto_result else []) +
                         (manual_result.errors if manual_result else []),
                'warnings': (auto_result.warnings if auto_result else []) +
                           (manual_result.warnings if manual_result else [])
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    schema = TOOL_SCHEMAS['09_anonymize_file']
    registry.register(
        '09_anonymize_file',
        anonymize_file,
        'privacy',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )

    # =========================================================================
    # 09_generate_anonymization_report
    # =========================================================================
    def generate_anonymization_report(
        file_path: str,
        output_path: Optional[str] = None,
        format: str = "markdown",
        include_samples: bool = False
    ) -> Dict[str, Any]:
        """
        Generate a compliance report for PII detection and anonymization.

        Args:
            file_path: Path to the file to analyze
            output_path: Path for the report (default: adds _pii_report suffix)
            format: Report format - "markdown", "html", or "json"
            include_samples: Include sample values in report (use with caution)

        Returns:
            Report generation results
        """
        try:
            path = Path(file_path)
            if not path.exists():
                return {
                    'success': False,
                    'error': f"File not found: {file_path}"
                }

            # Detect PII
            detector = PIIDetector()
            result = detector.detect_in_file(str(path), deep_scan=False)

            # Determine output path
            if output_path:
                out_path = Path(output_path)
            else:
                ext = '.md' if format == 'markdown' else f'.{format}'
                out_path = path.parent / f"{path.stem}_pii_report{ext}"

            # Generate report content
            if format == 'markdown':
                content = _generate_markdown_report(result, path, include_samples)
            elif format == 'html':
                content = _generate_html_report(result, path, include_samples)
            elif format == 'json':
                import json
                content = json.dumps(_generate_json_report(result, path), indent=2)
            else:
                return {
                    'success': False,
                    'error': f"Unknown format: {format}. Valid: markdown, html, json"
                }

            # Write report
            with open(out_path, 'w', encoding='utf-8') as f:
                f.write(content)

            return {
                'success': True,
                'report_path': str(out_path),
                'format': format,
                'file_analyzed': str(path),
                'columns_with_pii': result.columns_with_pii,
                'risk_score': round(result.overall_risk_score, 1),
                'risk_level': _get_risk_level(result.overall_risk_score)
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    schema = TOOL_SCHEMAS['09_generate_anonymization_report']
    registry.register(
        '09_generate_anonymization_report',
        generate_anonymization_report,
        'privacy',
        schema['description'],
        schema['parameters'],
        schema.get('required', [])
    )


# =============================================================================
# Helper Functions
# =============================================================================

def _get_risk_level(score: float) -> str:
    """Convert risk score to level"""
    if score >= 80:
        return "CRITICAL"
    elif score >= 60:
        return "HIGH"
    elif score >= 40:
        return "MEDIUM"
    elif score >= 20:
        return "LOW"
    else:
        return "MINIMAL"


def _generate_markdown_report(
    result: PIIDetectionResult,
    file_path: Path,
    include_samples: bool
) -> str:
    """Generate markdown format report"""
    lines = [
        "# PII Detection Report",
        "",
        f"**File:** `{file_path.name}`  ",
        f"**Generated:** {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ",
        "",
        "---",
        "",
        "## Summary",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total Columns | {result.total_columns} |",
        f"| Columns with PII | {result.columns_with_pii} |",
        f"| Risk Score | {result.overall_risk_score:.1f}/100 |",
        f"| Risk Level | **{_get_risk_level(result.overall_risk_score)}** |",
        "",
    ]

    # Sensitivity breakdown
    if result.sensitivity_summary:
        lines.extend([
            "## Sensitivity Breakdown",
            "",
            "| Level | Count |",
            "|-------|-------|",
        ])
        for level in ['critical', 'high', 'medium', 'low']:
            count = result.sensitivity_summary.get(level, 0)
            if count > 0:
                lines.append(f"| {level.upper()} | {count} |")
        lines.append("")

    # GDPR Categories
    if result.gdpr_categories:
        lines.extend([
            "## GDPR Data Categories",
            "",
        ])
        for category, columns in result.gdpr_categories.items():
            lines.append(f"- **{category}**: {', '.join(columns)}")
        lines.append("")

    # Detailed findings
    if result.column_details:
        lines.extend([
            "## Detailed Findings",
            "",
        ])

        for col in result.column_details:
            sensitivity = col.max_sensitivity.value.upper() if col.max_sensitivity else "UNKNOWN"
            lines.extend([
                f"### Column: `{col.column_name}`",
                "",
                f"- **Sensitivity:** {sensitivity}",
                f"- **PII Types:** {', '.join(t.value for t in col.detected_pii_types)}",
                f"- **Detection Method:** {col.detection_method}",
                "",
            ])

            if col.confidence_scores:
                lines.append("**Confidence Scores:**")
                for pii_type, conf in col.confidence_scores.items():
                    lines.append(f"- {pii_type.value}: {conf:.1%}")
                lines.append("")

            if include_samples and col.sample_matches:
                lines.append("**Sample Matches:**")
                for pii_type, samples in col.sample_matches.items():
                    lines.append(f"- {pii_type.value}: `{samples[0]}`...")
                lines.append("")

            if col.recommendations:
                lines.append("**Recommendations:**")
                for rec in col.recommendations:
                    lines.append(f"- {rec}")
                lines.append("")

    # Overall recommendations
    if result.recommendations:
        lines.extend([
            "## Recommendations",
            "",
        ])
        for i, rec in enumerate(result.recommendations, 1):
            lines.append(f"{i}. {rec}")
        lines.append("")

    # Anonymization strategies
    lines.extend([
        "## Suggested Anonymization Strategies",
        "",
        "| Column | PII Type | Suggested Strategy |",
        "|--------|----------|-------------------|",
    ])

    strategy_map = {
        'critical': 'REDACT or HASH',
        'high': 'MASK or SYNTHETIC',
        'medium': 'GENERALIZE or SHUFFLE',
        'low': 'SHUFFLE or keep'
    }

    for col in result.column_details:
        sens = col.max_sensitivity.value if col.max_sensitivity else 'low'
        pii_types = ', '.join(t.value for t in col.detected_pii_types)
        strategy = strategy_map.get(sens, 'Review')
        lines.append(f"| {col.column_name} | {pii_types} | {strategy} |")

    lines.extend(["", "---", "", "*Report generated by MCP Sample Data Server*"])

    return '\n'.join(lines)


def _generate_html_report(
    result: PIIDetectionResult,
    file_path: Path,
    include_samples: bool
) -> str:
    """Generate HTML format report"""
    risk_level = _get_risk_level(result.overall_risk_score)
    risk_color = {
        'CRITICAL': '#dc3545',
        'HIGH': '#fd7e14',
        'MEDIUM': '#ffc107',
        'LOW': '#28a745',
        'MINIMAL': '#6c757d'
    }.get(risk_level, '#6c757d')

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>PII Detection Report - {file_path.name}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        h1 {{ color: #333; }}
        .summary {{ background: #f8f9fa; padding: 20px; border-radius: 8px; margin: 20px 0; }}
        .risk-badge {{ display: inline-block; padding: 8px 16px; border-radius: 4px;
                      color: white; font-weight: bold; background: {risk_color}; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background: #007bff; color: white; }}
        tr:nth-child(even) {{ background: #f8f9fa; }}
        .critical {{ color: #dc3545; font-weight: bold; }}
        .high {{ color: #fd7e14; font-weight: bold; }}
        .medium {{ color: #ffc107; }}
        .low {{ color: #28a745; }}
    </style>
</head>
<body>
    <h1>PII Detection Report</h1>
    <p><strong>File:</strong> <code>{file_path.name}</code></p>

    <div class="summary">
        <h2>Summary</h2>
        <p><strong>Total Columns:</strong> {result.total_columns}</p>
        <p><strong>Columns with PII:</strong> {result.columns_with_pii}</p>
        <p><strong>Risk Score:</strong> {result.overall_risk_score:.1f}/100</p>
        <p><strong>Risk Level:</strong> <span class="risk-badge">{risk_level}</span></p>
    </div>

    <h2>Detailed Findings</h2>
    <table>
        <tr>
            <th>Column</th>
            <th>PII Types</th>
            <th>Sensitivity</th>
            <th>Confidence</th>
        </tr>
"""

    for col in result.column_details:
        sens = col.max_sensitivity.value if col.max_sensitivity else 'unknown'
        pii_types = ', '.join(t.value for t in col.detected_pii_types)
        conf = max(col.confidence_scores.values()) if col.confidence_scores else 0

        html += f"""        <tr>
            <td><code>{col.column_name}</code></td>
            <td>{pii_types}</td>
            <td class="{sens}">{sens.upper()}</td>
            <td>{conf:.0%}</td>
        </tr>
"""

    html += """    </table>

    <h2>Recommendations</h2>
    <ul>
"""

    for rec in result.recommendations:
        html += f"        <li>{rec}</li>\n"

    html += """    </ul>

    <footer style="margin-top: 40px; color: #6c757d;">
        <p><em>Report generated by MCP Sample Data Server</em></p>
    </footer>
</body>
</html>"""

    return html


def _generate_json_report(
    result: PIIDetectionResult,
    file_path: Path
) -> Dict[str, Any]:
    """Generate JSON format report"""
    return {
        'file': str(file_path),
        'summary': {
            'total_columns': result.total_columns,
            'columns_with_pii': result.columns_with_pii,
            'risk_score': round(result.overall_risk_score, 1),
            'risk_level': _get_risk_level(result.overall_risk_score)
        },
        'sensitivity_summary': result.sensitivity_summary,
        'gdpr_categories': result.gdpr_categories,
        'columns': [
            {
                'name': col.column_name,
                'pii_types': [t.value for t in col.detected_pii_types],
                'sensitivity': col.max_sensitivity.value if col.max_sensitivity else None,
                'confidence': {t.value: c for t, c in col.confidence_scores.items()},
                'detection_method': col.detection_method,
                'recommendations': col.recommendations
            }
            for col in result.column_details
        ],
        'recommendations': result.recommendations
    }
