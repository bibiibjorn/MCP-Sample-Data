"""
Mapping Manager Module
Manages mapping definitions between files
"""
import json
import os
from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class MappingManager:
    """Manages mapping definitions"""

    def __init__(self, storage_path: Optional[str] = None):
        if storage_path:
            self.storage_path = storage_path
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            self.storage_path = os.path.join(
                os.path.dirname(os.path.dirname(script_dir)),
                'projects', 'mappings'
            )

        os.makedirs(self.storage_path, exist_ok=True)
        self.mappings: Dict[str, Dict[str, Any]] = {}
        self._load_mappings()

    def _load_mappings(self):
        """Load existing mappings from storage"""
        mappings_file = os.path.join(self.storage_path, 'mappings.json')
        if os.path.exists(mappings_file):
            try:
                with open(mappings_file, 'r', encoding='utf-8') as f:
                    self.mappings = json.load(f)
            except Exception as e:
                logger.warning(f"Error loading mappings: {e}")
                self.mappings = {}

    def _save_mappings(self):
        """Save mappings to storage"""
        mappings_file = os.path.join(self.storage_path, 'mappings.json')
        try:
            with open(mappings_file, 'w', encoding='utf-8') as f:
                json.dump(self.mappings, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Error saving mappings: {e}")

    def define_mapping(
        self,
        mapping_name: str,
        source_file: str,
        source_column: str,
        target_file: str,
        target_column: str,
        explicit_mappings: Optional[Dict[str, str]] = None,
        hierarchy_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Define a mapping between files.

        Args:
            mapping_name: Unique name for this mapping
            source_file: Path to source data file
            source_column: Column name in source file
            target_file: Path to target/mapping file
            target_column: Column name in target file
            explicit_mappings: Manual source->target value mappings
            hierarchy_config: Hierarchy configuration for rollup calculations

        Returns:
            Mapping definition result
        """
        try:
            mapping_def = {
                'name': mapping_name,
                'source_file': source_file,
                'source_column': source_column,
                'target_file': target_file,
                'target_column': target_column,
                'explicit_mappings': explicit_mappings or {},
                'hierarchy_config': hierarchy_config or {},
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }

            self.mappings[mapping_name] = mapping_def
            self._save_mappings()

            return {
                'success': True,
                'mapping_name': mapping_name,
                'mapping': mapping_def
            }

        except Exception as e:
            logger.error(f"Error defining mapping: {e}", exc_info=True)
            return {'success': False, 'error': str(e)}

    def get_mapping(self, mapping_name: str) -> Optional[Dict[str, Any]]:
        """Get a mapping definition by name"""
        return self.mappings.get(mapping_name)

    def list_mappings(self) -> List[Dict[str, Any]]:
        """List all mapping definitions"""
        return [
            {
                'name': name,
                'source_file': m['source_file'],
                'source_column': m['source_column'],
                'target_file': m['target_file'],
                'target_column': m['target_column'],
                'created_at': m.get('created_at')
            }
            for name, m in self.mappings.items()
        ]

    def delete_mapping(self, mapping_name: str) -> Dict[str, Any]:
        """Delete a mapping definition"""
        if mapping_name not in self.mappings:
            return {'success': False, 'error': f'Mapping not found: {mapping_name}'}

        del self.mappings[mapping_name]
        self._save_mappings()

        return {'success': True, 'deleted': mapping_name}

    def update_explicit_mappings(
        self,
        mapping_name: str,
        new_mappings: Dict[str, str]
    ) -> Dict[str, Any]:
        """Update explicit value mappings"""
        if mapping_name not in self.mappings:
            return {'success': False, 'error': f'Mapping not found: {mapping_name}'}

        self.mappings[mapping_name]['explicit_mappings'].update(new_mappings)
        self.mappings[mapping_name]['updated_at'] = datetime.now().isoformat()
        self._save_mappings()

        return {
            'success': True,
            'mapping_name': mapping_name,
            'explicit_mappings': self.mappings[mapping_name]['explicit_mappings']
        }

    def apply_mapping(
        self,
        mapping_name: str,
        source_value: str
    ) -> Optional[str]:
        """Apply a mapping to get the target value"""
        mapping = self.mappings.get(mapping_name)
        if not mapping:
            return None

        explicit = mapping.get('explicit_mappings', {})
        return explicit.get(source_value)
