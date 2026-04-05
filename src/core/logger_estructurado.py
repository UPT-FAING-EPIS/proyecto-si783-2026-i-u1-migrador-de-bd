"""Issue #4: Sistema de logging estructurado con JSON y trazabilidad."""

import structlog
import logging.config
import yaml
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

def configurar_logging(ruta_config: str = "config/logging.yaml"):
    """Configura el logging estructurado desde archivo YAML."""
    
    # Configurar logging estándar
    with open(ruta_config, 'r') as f:
        config = yaml.safe_load(f)
        logging.config.dictConfig(config)
    
    # Configurar structlog
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
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    return structlog.get_logger()

class LoggerMigracion:
    """Logger personalizado para operaciones de migración con trazabilidad."""
    
    def __init__(self, id_migracion: Optional[str] = None):
        self.logger = structlog.get_logger()
        self.id_migracion = id_migracion or datetime.now().strftime("%Y%m%d_%H%M%S")
        self._contexto_actual = {}
    
    def bind(self, **kwargs):
        """Agrega contexto al logger."""
        self._contexto_actual.update(kwargs)
        return self
    
    def _log(self, nivel: str, mensaje: str, **kwargs):
        """Método interno para logging."""
        contexto = {
            'id_migracion': self.id_migracion,
            **self._contexto_actual,
            **kwargs
        }
        getattr(self.logger, nivel)(mensaje, **contexto)
    
    def debug(self, mensaje: str, **kwargs):
        self._log('debug', mensaje, **kwargs)
    
    def info(self, mensaje: str, **kwargs):
        self._log('info', mensaje, **kwargs)
    
    def warning(self, mensaje: str, **kwargs):
        self._log('warning', mensaje, **kwargs)
    
    def error(self, mensaje: str, **kwargs):
        self._log('error', mensaje, **kwargs)
    
    def exception(self, mensaje: str, **kwargs):
        self._log('exception', mensaje, **kwargs)
    
    def log_extraccion(self, tabla: str, filas_extraidas: int, tamano_lote: int):
        """Registra evento de extracción."""
        self.info(
            "Extracción completada",
            etapa="extraccion",
            tabla=tabla,
            filas_extraidas=filas_extraidas,
            tamano_lote=tamano_lote
        )
    
    def log_transformacion(self, tabla: str, filas_entrada: int, filas_salida: int):
        """Registra evento de transformación."""
        self.info(
            "Transformación completada",
            etapa="transformacion",
            tabla=tabla,
            filas_entrada=filas_entrada,
            filas_salida=filas_salida
        )
    
    def log_carga(self, tabla: str, filas_cargadas: int, duracion_segundos: float):
        """Registra evento de carga."""
        self.info(
            "Carga completada",
            etapa="carga",
            tabla=tabla,
            filas_cargadas=filas_cargadas,
            duracion_segundos=duracion_segundos
        )
    
    def log_error(self, etapa: str, tabla: str, error: Exception, contexto: Dict = None):
        """Registra un error con contexto detallado."""
        self.error(
            f"Error en {etapa}",
            etapa=etapa,
            tabla=tabla,
            error=str(error),
            tipo_error=type(error).__name__,
            contexto=contexto or {},
            exc_info=True
        )

# Inicializar configuración al importar
configurar_logging()