"""Issue #9: Manejo de errores con reintentos y backoff."""

from typing import Callable, Type, Tuple, Any, Optional
from functools import wraps
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    RetryError
)
import structlog
from sqlalchemy.exc import OperationalError, TimeoutError, DatabaseError
from datetime import datetime
import traceback

logger = structlog.get_logger()

class ErrorExtraccion(Exception):
    """Excepción personalizada para errores de extracción."""
    pass

class ErrorTransformacion(Exception):
    """Excepción personalizada para errores de transformación."""
    pass

class ErrorCarga(Exception):
    """Excepción personalizada para errores de carga."""
    pass

def con_reintento(
    max_intentos: int = 3,
    espera_min: int = 1,
    espera_max: int = 10,
    excepciones_reintentar: Tuple[Type[Exception]] = (OperationalError, TimeoutError)
):
    """Decorador para lógica de reintento con backoff exponencial."""
    
    def decorador(func: Callable) -> Callable:
        @wraps(func)
        @retry(
            stop=stop_after_attempt(max_intentos),
            wait=wait_exponential(multiplier=1, min=espera_min, max=espera_max),
            retry=retry_if_exception_type(excepciones_reintentar),
            before_sleep=before_sleep_log(logger, logger.warning)
        )
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.warning(
                    f"Operación fallida, reintentando",
                    funcion=func.__name__,
                    error=str(e),
                    intento=wrapper.retry_count if hasattr(wrapper, 'retry_count') else 0
                )
                raise
        return wrapper
    return decorador

class ManejadorErrores:
    """Maneja y registra errores durante la migración."""
    
    def __init__(self, logger_migracion):
        self.logger = logger_migracion
        self.conteo_errores = 0
        self.errores_por_tipo = {}
        self.errores_registrados = []
        self.max_errores_continuos = 10
    
    def manejar_error_extraccion(
        self,
        nombre_tabla: str,
        error: Exception,
        info_lote: dict = None
    ) -> bool:
        """
        Maneja error de extracción.
        
        Returns:
            True si debe continuar, False si debe abortar
        """
        self._registrar_error('extraccion', nombre_tabla, error, info_lote)
        
        # Decidir si continuar o abortar
        if isinstance(error, (OperationalError, TimeoutError)):
            # Problemas de red/conexión - se puede reintentar
            return True
        elif isinstance(error, DatabaseError):
            # Problemas de base de datos - pueden ser graves
            if self.conteo_errores > self.max_errores_continuos:
                self.logger.error("Demasiados errores consecutivos, abortando")
                return False
            return True
        else:
            # Errores desconocidos - abortar
            self.logger.error(f"Tipo de error desconocido, abortando: {type(error).__name__}")
            return False
    
    def manejar_error_transformacion(
        self,
        nombre_tabla: str,
        datos_fila: dict,
        error: Exception
    ) -> dict:
        """
        Maneja error de transformación para una fila específica.
        
        Returns:
            Fila modificada con información de error o None para saltar
        """
        self._registrar_error('transformacion', nombre_tabla, error, {'vista_previa_fila': str(datos_fila)[:200]})
        
        # Agregar información de error a la fila para registro
        datos_fila['_error_transformacion'] = str(error)
        datos_fila['_timestamp_error'] = datetime.utcnow().isoformat()
        datos_fila['_tipo_error'] = type(error).__name__
        
        return datos_fila
    
    def manejar_error_carga(
        self,
        nombre_tabla: str,
        lote: list,
        error: Exception,
        num_lote: int
    ) -> str:
        """
        Maneja error de carga.
        
        Returns:
            Estrategia a seguir: 'reintentar', 'saltar_lote', 'abortar'
        """
        self._registrar_error('carga', nombre_tabla, error, {'num_lote': num_lote, 'tamano_lote': len(lote)})
        
        if isinstance(error, (OperationalError, TimeoutError)):
            return 'reintentar'
        elif isinstance(error, IntegrityError):
            return 'saltar_lote'
        else:
            return 'abortar'
    
    def _registrar_error(self, etapa: str, tabla: str, error: Exception, contexto: dict = None):
        """Registra un error en las estadísticas."""
        self.conteo_errores += 1
        tipo_error = type(error).__name__
        self.errores_por_tipo[tipo_error] = self.errores_por_tipo.get(tipo_error, 0) + 1
        
        error_info = {
            'timestamp': datetime.utcnow().isoformat(),
            'etapa': etapa,
            'tabla': tabla,
            'tipo_error': tipo_error,
            'mensaje': str(error),
            'contexto': contexto or {},
            'traceback': traceback.format_exc()[:1000]
        }
        self.errores_registrados.append(error_info)
        
        self.logger.log_error(
            etapa=etapa,
            tabla=tabla,
            error=error,
            contexto={
                'info_lote': contexto,
                'tipo_error': tipo_error,
                'conteo_errores': self.conteo_errores
            }
        )
    
    def obtener_resumen_errores(self) -> dict:
        """Obtiene resumen de los errores encontrados."""
        return {
            'total_errores': self.conteo_errores,
            'errores_por_tipo': self.errores_por_tipo,
            'ultimos_errores': self.errores_registrados[-10:],  # Últimos 10 errores
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def reiniciar(self):
        """Reinicia los contadores de errores."""
        self.conteo_errores = 0
        self.errores_por_tipo = {}
        self.errores_registrados = []
    
    def hay_demasiados_errores(self) -> bool:
        """Verifica si se ha superado el umbral de errores."""
        return self.conteo_errores > self.max_errores_continuos

# Importar para tipo IntegrityError
from sqlalchemy.exc import IntegrityError
