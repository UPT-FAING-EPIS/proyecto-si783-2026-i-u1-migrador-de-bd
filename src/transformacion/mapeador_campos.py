"""Issue #11: Motor de mapeo de campos con transformaciones simples."""

from typing import Dict, Any, Callable, Optional, List
from dataclasses import dataclass
from datetime import datetime
import structlog

logger = structlog.get_logger()

@dataclass
class ReglaMapeo:
    """Define una regla de mapeo de campos."""
    campo_origen: str
    campo_destino: str
    transformacion: Optional[str] = None
    valor_default: Optional[Any] = None
    requerido: bool = False
    descripcion: Optional[str] = None
    
    def __post_init__(self):
        self.funcion_transformacion = self._obtener_funcion_transformacion()
    
    def _obtener_funcion_transformacion(self) -> Optional[Callable]:
        """Obtiene la función de transformación según el tipo."""
        transformaciones = {
            'mayusculas': lambda x: str(x).upper() if x is not None else x,
            'minusculas': lambda x: str(x).lower() if x is not None else x,
            'recortar': lambda x: str(x).strip() if x is not None else x,
            'a_texto': lambda x: str(x) if x is not None else None,
            'a_entero': lambda x: int(float(x)) if x is not None and x != '' else None,
            'a_decimal': lambda x: float(x) if x is not None and x != '' else None,
            'a_fecha': lambda x: datetime.fromisoformat(str(x)) if x else None,
            'a_booleano': lambda x: bool(x) if x is not None else False,
            'sin_espacios': lambda x: str(x).replace(' ', '') if x is not None else x,
            'primeras_10': lambda x: str(x)[:10] if x is not None else x,
        }
        
        return transformaciones.get(self.transformacion)

class MapeadorCampos:
    """Mapea campos desde origen a destino con transformaciones."""
    
    def __init__(self, reglas_mapeo: List[ReglaMapeo]):
        """
        Inicializa el mapeador de campos.
        
        Args:
            reglas_mapeo: Lista de reglas de mapeo
        """
        self.reglas = reglas_mapeo
        self.origen_a_destino = {regla.campo_origen: regla for regla in reglas_mapeo}
        self.destino_a_origen = {regla.campo_destino: regla.campo_origen for regla in reglas_mapeo}
        self.logger = logger.bind(componente="MapeadorCampos")
    
    def mapear_fila(self, fila_origen: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mapea una fila individual de origen a destino.
        
        Args:
            fila_origen: Fila de datos de origen
            
        Returns:
            Fila de destino mapeada
        """
        fila_destino = {}
        
        for regla in self.reglas:
            # Obtener valor de origen
            valor_origen = fila_origen.get(regla.campo_origen)
            
            # Aplicar valor default si es None y hay default configurado
            if valor_origen is None and regla.valor_default is not None:
                valor_origen = regla.valor_default
            
            # Aplicar transformación si existe
            if valor_origen is not None and regla.funcion_transformacion:
                try:
                    valor_origen = regla.funcion_transformacion(valor_origen)
                except Exception as e:
                    self.logger.warning(
                        f"Transformación fallida para {regla.campo_origen}",
                        error=str(e),
                        valor=valor_origen
                    )
                    if regla.requerido:
                        raise ValueError(f"Campo requerido {regla.campo_origen} - transformación fallida")
                    valor_origen = None
            
            # Validar campos requeridos
            if regla.requerido and valor_origen is None:
                raise ValueError(f"Campo requerido {regla.campo_origen} está ausente")
            
            fila_destino[regla.campo_destino] = valor_origen
        
        return fila_destino
    
    def mapear_lote(self, lote_origen: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Mapea un lote de filas.
        
        Args:
            lote_origen: Lista de filas de origen
            
        Returns:
            Lista de filas de destino mapeadas
        """
        lote_mapeado = []
        
        for idx, fila_origen in enumerate(lote_origen):
            try:
                fila_mapeada = self.mapear_fila(fila_origen)
                lote_mapeado.append(fila_mapeada)
            except Exception as e:
                self.logger.error(f"Error al mapear fila {idx}", error=str(e), fila=fila_origen)
                # Agregar información de error a la fila
                fila_origen['_error_mapeo'] = str(e)
                fila_origen['_indice_fila'] = idx
                lote_mapeado.append(fila_origen)
        
        return lote_mapeado
    
    def agregar_regla_mapeo(self, regla: ReglaMapeo):
        """Agrega una nueva regla de mapeo dinámicamente."""
        self.reglas.append(regla)
        self.origen_a_destino[regla.campo_origen] = regla
        self.destino_a_origen[regla.campo_destino] = regla.campo_origen
    
    def eliminar_regla_mapeo(self, campo_origen: str):
        """Elimina una regla de mapeo por campo origen."""
        self.reglas = [r for r in self.reglas if r.campo_origen != campo_origen]
        if campo_origen in self.origen_a_destino:
            del self.origen_a_destino[campo_origen]
        # Limpiar destino_a_origen también
        for k, v in list(self.destino_a_origen.items()):
            if v == campo_origen:
                del self.destino_a_origen[k]
    
    def obtener_resumen_mapeo(self) -> Dict[str, Any]:
        """Obtiene resumen de los mapeos actuales."""
        return {
            'total_reglas': len(self.reglas),
            'campos_origen': list(self.origen_a_destino.keys()),
            'campos_destino': list(self.destino_a_origen.keys()),
            'reglas': [
                {
                    'origen': r.campo_origen,
                    'destino': r.campo_destino,
                    'transformacion': r.transformacion,
                    'requerido': r.requerido
                }
                for r in self.reglas
            ]
        }

class TransformadorSimple:
    """Transformador simple para operaciones comunes entre campos."""
    
    @staticmethod
    def concatenar_campos(fila: Dict, campos: List[str], separador: str = ' ') -> str:
        """Concatena múltiples campos."""
        valores = [str(fila.get(campo, '')) for campo in campos]
        return separador.join([v for v in valores if v])
    
    @staticmethod
    def dividir_campo(valor: str, separador: str = ',', indice: int = 0) -> Optional[str]:
        """Divide un campo y retorna una parte específica."""
        if not valor:
            return None
        partes = str(valor).split(separador)
        if indice < len(partes):
            return partes[indice].strip()
        return None
    
    @staticmethod
    def valor_condicional(fila: Dict, campo: str, condiciones: Dict[Any, Any]) -> Any:
        """Aplica mapeo condicional."""
        valor = fila.get(campo)
        return condiciones.get(valor, valor)
    
    @staticmethod
    def extraer_subcadena(valor: str, inicio: int, fin: int = None) -> Optional[str]:
        """Extrae subcadena de un texto."""
        if not valor:
            return None
        if fin:
            return str(valor)[inicio:fin]
        return str(valor)[inicio:]
    
    @staticmethod
    def reemplazar_texto(valor: str, buscar: str, reemplazar: str) -> str:
        """Reemplaza texto en un campo."""
        if not valor:
            return valor
        return str(valor).replace(buscar, reemplazar)
    
    @staticmethod
    def formatear_moneda(valor: Any, decimales: int = 2) -> Optional[float]:
        """Formatea valor como moneda."""
        try:
            return round(float(valor), decimales) if valor is not None else None
        except (ValueError, TypeError):
            return None
