"""Issue #6: Extractor con manejo de lotes configurables."""

from typing import Dict, Any, List, Optional, Generator, Callable
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import structlog

logger = structlog.get_logger()

class ExtractorLotes:
    """Extrae datos en lotes configurables desde la base de datos origen."""
    
    def __init__(self, conector_origen, tamano_lote: int = 10000):
        """
        Inicializa el extractor de lotes.
        
        Args:
            conector_origen: Instancia de ConectorOrigen
            tamano_lote: Número de filas por lote
        """
        self.conector = conector_origen
        self.tamano_lote = tamano_lote
        self.tipo_bd = conector_origen.tipo
        self.logger = logger.bind(componente="ExtractorLotes")
    
    def extraer_tabla(
        self,
        nombre_tabla: str,
        columnas: Optional[List[str]] = None,
        clausula_where: Optional[str] = None,
        order_by: Optional[str] = None,
        consulta_personalizada: Optional[str] = None
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Extrae datos en lotes desde una tabla.
        
        Args:
            nombre_tabla: Nombre de la tabla origen
            columnas: Lista de columnas a extraer (None = todas)
            clausula_where: Condición WHERE opcional
            order_by: Ordenamiento opcional
            consulta_personalizada: Consulta SQL personalizada
            
        Yields:
            Lista de diccionarios representando filas
        """
        if consulta_personalizada:
            consulta_base = consulta_personalizada
        else:
            columnas_str = "*" if not columnas else ", ".join(columnas)
            consulta_base = f"SELECT {columnas_str} FROM {nombre_tabla}"
            if clausula_where:
                consulta_base += f" WHERE {clausula_where}"
            if order_by:
                consulta_base += f" ORDER BY {order_by}"
        
        offset = 0
        total_extraido = 0
        
        while True:
            # Construir consulta paginada según tipo de BD
            if self.tipo_bd == 'mssql':
                if order_by:
                    consulta = f"{consulta_base} OFFSET {offset} ROWS FETCH NEXT {self.tamano_lote} ROWS ONLY"
                else:
                    # SQL Server requiere ORDER BY con OFFSET
                    consulta = f"{consulta_base} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {self.tamano_lote} ROWS ONLY"
            else:
                # PostgreSQL y MySQL
                consulta = f"{consulta_base} LIMIT {self.tamano_lote} OFFSET {offset}"
            
            try:
                self.logger.debug(f"Ejecutando consulta por lotes", offset=offset, tamano_lote=self.tamano_lote)
                resultado = self.conector.ejecutar_consulta(consulta)
                filas = resultado.fetchall()
                
                if not filas:
                    break
                
                # Convertir filas a diccionarios
                lote = [dict(fila._mapping) for fila in filas]
                total_extraido += len(lote)
                
                self.logger.info(
                    f"Lote extraído",
                    tabla=nombre_tabla,
                    tamano_lote=len(lote),
                    offset=offset,
                    total_extraido=total_extraido
                )
                
                yield lote
                offset += self.tamano_lote
                
            except SQLAlchemyError as e:
                self.logger.error(f"Error en extracción: {str(e)}", tabla=nombre_tabla, offset=offset)
                raise
    
    def extraer_con_transformador(
        self,
        consulta: str,
        transformador: Optional[Callable] = None
    ) -> Generator[Any, None, None]:
        """
        Extrae con transformador personalizado por fila.
        
        Args:
            consulta: Consulta SQL a ejecutar
            transformador: Función para transformar cada fila
        """
        offset = 0
        
        while True:
            if self.tipo_bd == 'mssql':
                consulta_paginada = f"{consulta} ORDER BY (SELECT NULL) OFFSET {offset} ROWS FETCH NEXT {self.tamano_lote} ROWS ONLY"
            else:
                consulta_paginada = f"{consulta} LIMIT {self.tamano_lote} OFFSET {offset}"
            
            resultado = self.conector.ejecutar_consulta(consulta_paginada)
            filas = resultado.fetchall()
            
            if not filas:
                break
            
            for fila in filas:
                fila_dict = dict(fila._mapping)
                if transformador:
                    yield transformador(fila_dict)
                else:
                    yield fila_dict
            
            offset += self.tamano_lote
    
    def obtener_conteo_total(self, nombre_tabla: str, clausula_where: Optional[str] = None) -> int:
        """Obtiene el número total de filas a extraer."""
        consulta = f"SELECT COUNT(*) as conteo FROM {nombre_tabla}"
        if clausula_where:
            consulta += f" WHERE {clausula_where}"
        
        resultado = self.conector.ejecutar_consulta(consulta)
        return resultado.fetchone()[0]
    
    def obtener_todas_las_tablas(self, tablas: List[str]) -> Dict[str, List[Dict]]:
        """Extrae todas las tablas especificadas."""
        resultado = {}
        for tabla in tablas:
            self.logger.info(f"Extrayendo tabla {tabla}")
            todas_filas = []
            for lote in self.extraer_tabla(tabla):
                todas_filas.extend(lote)
            resultado[tabla] = todas_filas
            self.logger.info(f"Extracción completada", tabla=tabla, total=len(todas_filas))
        return resultado