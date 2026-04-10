"""Issue #8: Mecanismo de reanudación para migraciones interrumpidas."""

from typing import Dict, Any, Optional, Generator
from datetime import datetime
import structlog
from .gestor_puntos_control import GestorPuntosControl

logger = structlog.get_logger()

class ManejadorReanudacion:
    """Gestiona la reanudación de migraciones desde el último punto de control."""
    
    def __init__(self, gestor_puntos_control: GestorPuntosControl):
        self.gestor_pc = gestor_puntos_control
        self.logger = logger.bind(componente="ManejadorReanudacion")
    
    def obtener_punto_reanudacion(
        self,
        id_migracion: str,
        nombre_tabla: str,
        columna_id: str = "id"
    ) -> Optional[Dict[str, Any]]:
        """
        Obtiene el punto desde el cual reanudar la migración.
        
        Returns:
            Diccionario con información de reanudación o None si comenzar desde cero
        """
        punto_control = self.gestor_pc.obtener_punto_control(id_migracion, nombre_tabla)
        
        if not punto_control:
            self.logger.info(f"No se encontró punto de control, comenzando desde cero", tabla=nombre_tabla)
            return None
        
        if punto_control.get('estado') == 'completado':
            self.logger.info(f"Tabla ya completada anteriormente", tabla=nombre_tabla)
            return {'completado': True}
        
        info_reanudacion = {
            'ultimo_id_procesado': punto_control.get('ultimo_id_procesado'),
            'ultimo_timestamp': punto_control.get('ultimo_timestamp'),
            'numero_lote': punto_control.get('numero_lote', 0),
            'total_procesado': punto_control.get('total_procesado', 0),
            'columna_id': columna_id
        }
        
        self.logger.info(
            f"Reanudando desde punto de control",
            tabla=nombre_tabla,
            ultimo_id=info_reanudacion['ultimo_id_procesado'],
            lote=info_reanudacion['numero_lote']
        )
        
        return info_reanudacion
    
    def construir_consulta_reanudacion(
        self,
        consulta_base: str,
        punto_reanudacion: Optional[Dict],
        columna_id: str = "id",
        columna_timestamp: Optional[str] = None
    ) -> str:
        """Construye consulta que reanuda desde el punto de control."""
        if not punto_reanudacion or punto_reanudacion.get('completado'):
            return consulta_base
        
        condiciones_where = []
        
        if punto_reanudacion.get('ultimo_id_procesado'):
            condiciones_where.append(f"{columna_id} > {punto_reanudacion['ultimo_id_procesado']}")
        
        if punto_reanudacion.get('ultimo_timestamp') and columna_timestamp:
            timestamp = punto_reanudacion['ultimo_timestamp']
            if isinstance(timestamp, datetime):
                timestamp = timestamp.isoformat()
            condiciones_where.append(f"{columna_timestamp} >= '{timestamp}'")
        
        if condiciones_where:
            if 'WHERE' in consulta_base.upper():
                return f"{consulta_base} AND {' AND '.join(condiciones_where)}"
            else:
                return f"{consulta_base} WHERE {' AND '.join(condiciones_where)}"
        
        return consulta_base
    
    def deberia_saltar_lote(
        self,
        id_migracion: str,
        nombre_tabla: str,
        numero_lote: int
    ) -> bool:
        """Verifica si un lote debe ser saltado (ya procesado)."""
        punto_control = self.gestor_pc.obtener_punto_control(id_migracion, nombre_tabla)
        
        if punto_control and punto_control.get('numero_lote', -1) >= numero_lote:
            self.logger.debug(
                f"Saltando lote ya procesado",
                tabla=nombre_tabla,
                lote=numero_lote
            )
            return True
        
        return False
    
    def obtener_progreso(self, id_migracion: str) -> Dict[str, Any]:
        """Obtiene el progreso general de la migración."""
        puntos = self.gestor_pc.obtener_todos_puntos_control(id_migracion)
        
        total_tablas = len(puntos)
        tablas_completadas = sum(1 for pc in puntos.values() if pc.get('estado') == 'completado')
        
        return {
            'id_migracion': id_migracion,
            'total_tablas': total_tablas,
            'tablas_completadas': tablas_completadas,
            'porcentaje': (tablas_completadas / total_tablas * 100) if total_tablas > 0 else 0,
            'puntos_control': puntos
        }
