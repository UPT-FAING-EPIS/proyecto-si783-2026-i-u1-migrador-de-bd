import pandas as pd
from typing import List, Dict, Any, Callable
from utilidades.registro_logging import ConfiguradorLogging

class TransformadorComplejo:
    def __init__(self):
        self.logger = ConfiguradorLogging.obtener_logger(__name__)
        self.reglas_negocio: List[Callable] = []
    
    def agregar_regla_negocio(self, funcion: Callable):
        self.reglas_negocio.append(funcion)
        self.logger.info("regla_agregada", funcion=funcion.__name__)
    
    def limpiar_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df_sin_duplicados = df.drop_duplicates()
        df_sin_nulos = df_sin_duplicados.dropna(how='all')
        
        columnas_texto = df_sin_nulos.select_dtypes(include=['object']).columns
        for columna in columnas_texto:
            df_sin_nulos[columna] = df_sin_nulos[columna].str.strip()
        
        self.logger.info("limpieza_completada", 
                         filas_originales=len(df),
                         filas_limpias=len(df_sin_nulos))
        
        return df_sin_nulos
    
    def aplicar_agregaciones(self, df: pd.DataFrame, 
                            columna_agrupacion: str, 
                            columna_valor: str,
                            funcion_agregacion: str = 'sum') -> pd.DataFrame:
        return df.groupby(columna_agrupacion)[columna_valor].agg(funcion_agregacion).reset_index()
    
    def normalizar_fechas(self, df: pd.DataFrame, columnas_fecha: List[str]) -> pd.DataFrame:
        for columna in columnas_fecha:
            if columna in df.columns:
                df[columna] = pd.to_datetime(df[columna], errors='coerce')
        return df
    
    def aplicar_reglas_negocio(self, df: pd.DataFrame) -> pd.DataFrame:
        for regla in self.reglas_negocio:
            df = regla(df)
            self.logger.debug("regla_aplicada", resultado=df.shape)
        return df