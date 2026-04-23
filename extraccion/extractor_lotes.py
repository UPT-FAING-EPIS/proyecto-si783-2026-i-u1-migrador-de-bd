from typing import Iterator, List, Dict, Any
from sqlalchemy import text
import pandas as pd

from extraccion.conector_base_datos import ConectorBaseDatos
from utilidades.registro_logging import ConfiguradorLogging

class ExtractorLotes:
    def __init__(self, conector: ConectorBaseDatos, config_extraccion: dict):
        self.conector = conector
        self.config = config_extraccion
        self.logger = ConfiguradorLogging.obtener_logger(__name__)
        self.tamano_lote = config_extraccion.get('tamano_lote', 1000)
        self.consulta_sql = config_extraccion.get('consulta_sql', '')
    
    def extraer_por_lotes(self, consulta_personalizada: str = None) -> Iterator[List[Dict[str, Any]]]:
        consulta = consulta_personalizada or self.consulta_sql
        offset = 0
        
        self.logger.info("inicio_extraccion", consulta=consulta, tamano_lote=self.tamano_lote)
        
        while True:
            try:
                consulta_paginada = f"""
                {consulta}
                LIMIT {self.tamano_lote} OFFSET {offset}
                """
                
                with self.conector.obtener_conexion() as conexion:
                    resultado = conexion.execute(text(consulta_paginada))
                    columnas = resultado.keys()
                    filas = [dict(zip(columnas, fila)) for fila in resultado.fetchall()]
                    
                    if not filas:
                        self.logger.info("extraccion_completada", total_lotes=offset)
                        break
                    
                    self.logger.info("lote_extraido", 
                                   lote_numero=offset // self.tamano_lote + 1,
                                   filas_en_lote=len(filas))
                    
                    yield filas
                    offset += self.tamano_lote
                    
            except Exception as error:
                self.logger.error("error_extraccion_lote", 
                                offset=offset, 
                                error=str(error))
                raise
    
    def extraer_como_dataframe(self, consulta: str = None) -> pd.DataFrame:
        consulta = consulta or self.consulta_sql
        with self.conector.obtener_conexion() as conexion:
            return pd.read_sql_query(consulta, conexion)