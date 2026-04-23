from typing import List, Dict, Any
from sqlalchemy import text, Table, MetaData
import pandas as pd

from extraccion.conector_base_datos import ConectorBaseDatos
from utilidades.registro_logging import ConfiguradorLogging

class CargadorMasivo:
    def __init__(self, conector_destino: ConectorBaseDatos, config_carga: dict):
        self.conector = conector_destino
        self.config = config_carga
        self.logger = ConfiguradorLogging.obtener_logger(__name__)
        self.tamano_lote = config_carga.get('tamano_lote_carga', 500)
    
    def cargar_lote(self, datos: List[Dict[str, Any]], tabla_destino: str, 
                    usar_upsert: bool = True, clave_upsert: List[str] = None):
        if not datos:
            return 0
        
        total_cargados = 0
        for i in range(0, len(datos), self.tamano_lote):
            lote = datos[i:i + self.tamano_lote]
            
            try:
                with self.conector.obtener_conexion() as conexion:
                    metadata = MetaData()
                    tabla = Table(tabla_destino, metadata, autoload_with=self.conector.motor_sqlalchemy)
                    
                    if usar_upsert and clave_upsert:
                        self._ejecutar_upsert(conexion, tabla, lote, clave_upsert)
                    else:
                        conexion.execute(tabla.insert(), lote)
                    
                    conexion.commit()
                    total_cargados += len(lote)
                    self.logger.info("lote_cargado", 
                                   filas=len(lote), 
                                   total_acumulado=total_cargados)
            except Exception as error:
                self.logger.error("error_carga_lote", error=str(error))
                raise
        
        return total_cargados
    
    def _ejecutar_upsert(self, conexion, tabla, datos: List[Dict], clave_upsert: List[str]):
        motor = self.conector.config.get('motor', '').lower()
        
        for fila in datos:
            if 'postgresql' in motor:
                columnas = ', '.join(fila.keys())
                valores = ', '.join([f":{k}" for k in fila.keys()])
                actualizaciones = ', '.join([f"{k} = EXCLUDED.{k}" for k in fila.keys()])
                
                consulta = f"""
                INSERT INTO {tabla.name} ({columnas})
                VALUES ({valores})
                ON CONFLICT ({', '.join(clave_upsert)})
                DO UPDATE SET {actualizaciones}
                """
                
                try:
                    conexion.execute(text(consulta), fila)
                except Exception as e:
                    self.logger.warning(f"upsert_fallido_para_id: {fila.get('id', 'desconocido')}")
    
    def cargar_dataframe(self, df: pd.DataFrame, tabla_destino: str):
        total_filas = len(df)
        with self.conector.obtener_conexion() as conexion:
            df.to_sql(tabla_destino, conexion, if_exists='append', 
                     index=False, chunksize=self.tamano_lote)
        self.logger.info("dataframe_cargado", tabla=tabla_destino, filas=total_filas)
        return total_filas