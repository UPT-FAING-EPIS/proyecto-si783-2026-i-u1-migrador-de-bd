import structlog
import logging
import json
from datetime import datetime
from pathlib import Path

class ConfiguradorLogging:
    def __init__(self, config_logging: dict = None):
        self.config = config_logging or {}
        self.nivel_logging = self._obtener_nivel()
        self._configurar_structlog()
    
    def _obtener_nivel(self) -> int:
        niveles = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        nivel = self.config.get('nivel', 'INFO')
        return niveles.get(nivel, logging.INFO)
    
    def _configurar_structlog(self):
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.stdlib.render_to_log_kwargs,
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
        
        formato = self.config.get('formato', 'json')
        if formato == 'json':
            logging.basicConfig(
                format='%(message)s',
                level=self.nivel_logging
            )
    
    @staticmethod
    def obtener_logger(nombre: str) -> structlog.BoundLogger:
        return structlog.get_logger(nombre)