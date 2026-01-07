"""
Template Engine Module
Generates data from predefined domain templates
"""
import polars as pl
from typing import Dict, Any, List, Optional
import yaml
import os
import logging
from core.generation.dimension_generator import DimensionGenerator
from core.generation.fact_generator import FactGenerator

logger = logging.getLogger(__name__)


class TemplateEngine:
    """Generates data from templates"""

    def __init__(self, templates_dir: Optional[str] = None):
        if templates_dir:
            self.templates_dir = templates_dir
        else:
            # Default templates directory
            script_dir = os.path.dirname(os.path.abspath(__file__))
            self.templates_dir = os.path.join(
                os.path.dirname(os.path.dirname(script_dir)),
                'templates'
            )

        self.dimension_generator = DimensionGenerator()
        self.fact_generator = FactGenerator()

    def list_templates(self, domain: str = 'all') -> Dict[str, Any]:
        """List available templates"""
        templates = []

        if not os.path.exists(self.templates_dir):
            return {'success': True, 'templates': [], 'message': 'Templates directory not found'}

        for root, dirs, files in os.walk(self.templates_dir):
            for file in files:
                if file.endswith('.yaml') or file.endswith('.yml'):
                    template_path = os.path.join(root, file)
                    relative_path = os.path.relpath(template_path, self.templates_dir)
                    template_domain = relative_path.split(os.sep)[0] if os.sep in relative_path else 'generic'

                    if domain != 'all' and template_domain != domain:
                        continue

                    # Load template metadata
                    try:
                        with open(template_path, 'r', encoding='utf-8') as f:
                            template = yaml.safe_load(f)
                            templates.append({
                                'name': relative_path.replace(os.sep, '/').replace('.yaml', '').replace('.yml', ''),
                                'domain': template_domain,
                                'description': template.get('description', ''),
                                'type': template.get('type', 'dimension'),
                                'columns': len(template.get('columns', []))
                            })
                    except Exception as e:
                        logger.warning(f"Error loading template {template_path}: {e}")

        return {
            'success': True,
            'templates': templates,
            'count': len(templates)
        }

    def generate(
        self,
        template: str,
        row_count: int,
        overrides: Optional[Dict[str, Any]] = None,
        output_path: Optional[str] = None,
        output_format: str = 'csv'
    ) -> Dict[str, Any]:
        """
        Generate data from a template.

        Args:
            template: Template name (e.g., 'financial/trial_balance')
            row_count: Number of rows to generate
            overrides: Override template defaults
            output_path: Optional output file path
            output_format: 'csv' or 'parquet'

        Returns:
            Generation result with data
        """
        try:
            # Find and load template
            template_path = self._find_template(template)
            if not template_path:
                return {'success': False, 'error': f'Template not found: {template}'}

            with open(template_path, 'r', encoding='utf-8') as f:
                template_def = yaml.safe_load(f)

            # Apply overrides
            if overrides:
                template_def = self._apply_overrides(template_def, overrides)

            # Generate based on template type
            template_type = template_def.get('type', 'dimension')

            if template_type == 'dimension':
                return self.dimension_generator.generate(
                    name=template_def.get('name', template),
                    columns=template_def.get('columns', []),
                    row_count=row_count,
                    output_path=output_path,
                    output_format=output_format
                )
            elif template_type == 'fact':
                return self.fact_generator.generate(
                    name=template_def.get('name', template),
                    grain=template_def.get('grain', []),
                    measures=template_def.get('measures', []),
                    dimensions=template_def.get('dimensions', []),
                    row_count=row_count,
                    output_path=output_path,
                    output_format=output_format
                )
            else:
                return {'success': False, 'error': f'Unknown template type: {template_type}'}

        except Exception as e:
            logger.error(f"Error generating from template: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def _find_template(self, template: str) -> Optional[str]:
        """Find template file path"""
        # Normalize template name
        template = template.replace('/', os.sep).replace('\\', os.sep)

        # Try different extensions
        for ext in ['.yaml', '.yml']:
            path = os.path.join(self.templates_dir, template + ext)
            if os.path.exists(path):
                return path

        return None

    def _apply_overrides(self, template: Dict, overrides: Dict) -> Dict:
        """Apply overrides to template definition"""
        result = template.copy()

        for key, value in overrides.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = {**result[key], **value}
            else:
                result[key] = value

        return result
