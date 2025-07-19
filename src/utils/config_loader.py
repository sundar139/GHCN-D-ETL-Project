import yaml
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class ConfigLoader:
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self._config = None
    
    def load_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_path, 'r') as file:
                self._config = yaml.safe_load(file)
            logger.info(f"Configuration loaded from {self.config_path}")
            return self._config
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    def get(self, key: str, default: Any = None) -> Any:
        if self._config is None:
            self.load_config()
        
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def get_spark_config(self) -> Dict[str, str]:
        return self.get('spark_conf', {})
