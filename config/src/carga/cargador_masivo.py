"""Issue #14: Estrategia de inserción masiva para carga eficiente."""

from typing import List, Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import structlog
from datetime import datetime
import pandas as pd
from tqdm import tqdm

logger = structlog.get_logger()

class CargadorMasivo:
    """Gestiona operaciones de inserción masiva con tamaño de lote configurable."""
    
    def __init__(self, conector_destino, tamano_lote: int = 5000):
        """
        Inicializa el cargador masivo.
        
        Args:
            conector_destino: Conector de base de datos destino
            tamano_lote: Número de filas por lote
        """
        self.destino = conector_destino
        self.tamano_lote = tamano_lote
        self.tipo_bd = conector_destino.tipo
        self.logger = logger.bind(componente="CargadorMasivo")
        self.estadisticas = {
            'total_filas': 0,
            'lotes_procesados': 0,
            'errores': 0,
            'tiempo_inicio': None,
            'tiempo_fin': None
        }
    
    def insertar_masivo(
        self,
        nombre_tabla: str,
        datos: List[Dict[str, Any]],
        usar_ejecutar_muchos: bool = True,
        mostrar_progreso: bool = False
    ) -> Dict[str, Any]:
        """
        Realiza operación de inserción masiva.
        
        Args:
            nombre_tabla: Nombre de la tabla destino
            datos: Lista de diccionarios a insertar
            usar_ejecutar_muchos: Usar executemany para mejor rendimiento
            mostrar_progreso: Mostrar barra de progreso
            
        Returns:
            Estadísticas de la operación
        """
        if not datos:
            self.logger.warning(f"No hay datos para insertar en {nombre_tabla}")
            return self.estadisticas
        
        self.estadisticas['tiempo_inicio'] = datetime.utcnow()
        self.estadisticas['total_filas'] = len(datos)
        
        # Calcular número de lotes
        num_lotes = (len(datos) + self.tamano_lote - 1) // self.tamano_lote
        
        # Crear iterador con barra de progreso si se solicita
        iterador = range(0, len(datos), self.tamano_lote)
        if mostrar_progreso:
            iterador = tqdm(iterador, desc=f"Cargando {nombre_tabla}", total=num_lotes, unit="lote")
        
        for i in iterador:
            lote = datos[i:i + self.tamano_lote]
            try:
                self._insertar_lote(nombre_tabla, lote, usar_ejecutar_muchos)
                self.estadisticas['lotes_procesados'] += 1
                
                if not mostrar_progreso:
                    self.logger.debug(
                        f"Lote insertado",
                        tabla=nombre_tabla,
                        numero_lote=self.estadisticas['lotes_procesados'],
                        filas_en_lote=len(lote),
                        total_procesado=min(i + self.tamano_lote, len(datos))
                    )
            except Exception as e:
                self.estadisticas['errores'] += 1
                self.logger.error(f"Error insertando lote", error=str(e), lote=i//self.tamano_lote)
                if self.estadisticas['errores'] > 10:
                    self.logger.error("Demasiados errores, abortando carga")
                    break
        
        self.estadisticas['tiempo_fin'] = datetime.utcnow()
        duracion = (self.estadisticas['tiempo_fin'] - self.estadisticas['tiempo_inicio']).total_seconds()
        
        self.logger.info(
            f"Inserción masiva completada",
            tabla=nombre_tabla,
            total_filas=self.estadisticas['total_filas'],
            lotes=self.estadisticas['lotes_procesados'],
            duracion_segundos=duracion,
            filas_por_segundo=self.estadisticas['total_filas'] / duracion if duracion > 0 else 0,
            errores=self.estadisticas['errores']
        )
        
        return self.estadisticas
    
    def _insertar_lote(
        self,
        nombre_tabla: str,
        lote: List[Dict[str, Any]],
        usar_ejecutar_muchos: bool
    ):
        """Inserta un solo lote de datos."""
        if not lote:
            return
        
        columnas = list(lote[0].keys())
        # Filtrar columnas que empiezan con _ (metadatos internos)
        columnas = [c for c in columnas if not c.startswith('_')]
        
        if not columnas:
            return
        
        placeholders = ', '.join([f":{col}" for col in columnas])
        columnas_str = ', '.join(columnas)
        consulta = f"INSERT INTO {nombre_tabla} ({columnas_str}) VALUES ({placeholders})"
        
        # Filtrar datos para incluir solo las columnas
        datos_filtrados = []
        for fila in lote:
            fila_filtrada = {col: fila.get(col) for col in columnas}
            datos_filtrados.append(fila_filtrada)
        
        try:
            with self.destino.motor.connect() as conn:
                if usar_ejecutar_muchos and len(datos_filtrados) > 1:
                    conn.execute(text(consulta), datos_filtrados)
                else:
                    for fila in datos_filtrados:
                        conn.execute(text(consulta), fila)
                conn.commit()
        except SQLAlchemyError as e:
            self.logger.error(f"Inserción de lote fallida", error=str(e), tabla=nombre_tabla)
            raise
    
    def insertar_dataframe_masivo(
        self,
        nombre_tabla: str,
        df: pd.DataFrame,
        metodo: str = 'multi'
    ) -> Dict[str, Any]:
        """
        Inserta datos desde un DataFrame de pandas.
        
        Args:
            nombre_tabla: Nombre de la tabla destino
            df: DataFrame a insertar
            metodo: Método de inserción ('multi', 'insert', o 'callable')
        """
        self.estadisticas['tiempo_inicio'] = datetime.utcnow()
        self.estadisticas['total_filas'] = len(df)
        
        try:
            # Limpiar nombres de columnas
            df = df.rename(columns=lambda x: x.replace(' ', '_').lower())
            
            # Usar to_sql de pandas para mejor rendimiento con DataFrames grandes
            if metodo == 'multi':
                df.to_sql(
                    name=nombre_tabla,
                    con=self.destino.motor,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=self.tamano_lote
                )
            elif metodo == 'insert':
                registros = df.to_dict('records')
                self.insertar_masivo(nombre_tabla, registros, usar_ejecutar_muchos=True)
            else:
                # Método callable personalizado
                registros = df.to_dict('records')
                self.insertar_masivo(nombre_tabla, registros, usar_ejecutar_muchos=False)
            
            self.estadisticas['tiempo_fin'] = datetime.utcnow()
            
        except Exception as e:
            self.logger.error(f"Inserción de DataFrame fallida", error=str(e))
            self.estadisticas['errores'] += 1
            raise
        
        duracion = (self.estadisticas['tiempo_fin'] - self.estadisticas['tiempo_inicio']).total_seconds()
        
        return {
            'total_filas': self.estadisticas['total_filas'],
            'duracion_segundos': duracion,
            'filas_por_segundo': self.estadisticas['total_filas'] / duracion if duracion > 0 else 0,
            'errores': self.estadisticas['errores']
        }
    
    def obtener_estadisticas(self) -> Dict[str, Any]:
        """Obtiene las estadísticas de carga."""
        duracion = None
        if self.estadisticas['tiempo_inicio'] and self.estadisticas['tiempo_fin']:
            duracion = (self.estadisticas['tiempo_fin'] - self.estadisticas['tiempo_inicio']).total_seconds()
        
        return {
            **self.estadisticas,
            'duracion_segundos': duracion,
            'filas_por_segundo': self.estadisticas['total_filas'] / duracion if duracion and duracion > 0 else 0,
            'tasa_exito': ((self.estadisticas['total_filas'] - self.estadisticas['errores']) / self.estadisticas['total_filas'] * 100) if self.estadisticas['total_filas'] > 0 else 100
        }
    
    def reiniciar_estadisticas(self):
        """Reinicia las estadísticas de carga."""
        self.estadisticas = {
            'total_filas': 0,
            'lotes_procesados': 0,
            'errores': 0,
            'tiempo_inicio': None,
            'tiempo_fin': None
        }
