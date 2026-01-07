"""
Configuration Manager
Handles loading and merging configuration from default and local config files
"""
import json
import os
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manages configuration for the MCP-Sample-Data server"""

    def __init__(self):
        self._config: Dict[str, Any] = {}
        self._load_config()

    def _load_config(self):
        """Load configuration from files"""
        # Find config directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_dir = os.path.join(os.path.dirname(os.path.dirname(script_dir)), 'config')

        # Load default config
        default_config_path = os.path.join(config_dir, 'default_config.json')
        if os.path.exists(default_config_path):
            try:
                with open(default_config_path, 'r', encoding='utf-8') as f:
                    self._config = json.load(f)
                logger.debug(f"Loaded default config from {default_config_path}")
            except Exception as e:
                logger.warning(f"Failed to load default config: {e}")
                self._config = self._get_default_config()
        else:
            logger.debug("No default config file found, using built-in defaults")
            self._config = self._get_default_config()

        # Load local config (overrides)
        local_config_path = os.path.join(config_dir, 'local_config.json')
        if os.path.exists(local_config_path):
            try:
                with open(local_config_path, 'r', encoding='utf-8') as f:
                    local_config = json.load(f)
                self._merge_config(local_config)
                logger.debug(f"Merged local config from {local_config_path}")
            except Exception as e:
                logger.warning(f"Failed to load local config: {e}")

    def _get_default_config(self) -> Dict[str, Any]:
        """Get built-in default configuration"""
        return {
            "server": {
                "name": "MCP-Sample-Data",
                "version": "1.0.0"
            },
            "cache": {
                "enabled": True,
                "max_size": 100,
                "ttl_seconds": 300
            },
            "generation": {
                "default_locale": "en_US",
                "max_rows_per_generation": 1000000,
                "default_output_format": "csv"
            },
            "editing": {
                "default_query_limit": 1000,
                "max_query_limit": 100000,
                "dry_run_default": True
            },
            "validation": {
                "balance_tolerance": 0.01,
                "referential_integrity_sample_size": 10000
            },
            "export": {
                "default_format": "csv",
                "parquet_compression": "snappy",
                "csv_encoding": "utf-8"
            },
            "projects": {
                "storage_path": "projects"
            },
            "logging": {
                "level": "WARNING",
                "file": "logs/sample_data.log"
            }
        }

    def _merge_config(self, override: Dict[str, Any]):
        """Recursively merge override config into current config"""
        for key, value in override.items():
            if key in self._config and isinstance(self._config[key], dict) and isinstance(value, dict):
                self._merge_config_recursive(self._config[key], value)
            else:
                self._config[key] = value

    def _merge_config_recursive(self, base: Dict, override: Dict):
        """Recursively merge dictionaries"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config_recursive(base[key], value)
            else:
                base[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value by key (supports dot notation)"""
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

    def get_section(self, section: str) -> Dict[str, Any]:
        """Get an entire configuration section"""
        return self._config.get(section, {})

    @property
    def all(self) -> Dict[str, Any]:
        """Get the entire configuration"""
        return self._config.copy()


# Global config instance
config = ConfigManager()
