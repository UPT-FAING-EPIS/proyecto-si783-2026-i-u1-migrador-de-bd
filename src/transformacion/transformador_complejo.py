"""Issue #12: Transformaciones complejas usando pandas."""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Callable, Union
from datetime import datetime
import structlog

logger = structlog.get_logger()

class TransformadorComplejo:
    """Realiza transformaciones complejas de datos usando pandas."""
    
    def __init__(self, configuracion: Optional[Dict] = None):
        """
        Inicializa el transformador complejo.
        
        Args:
            configuracion: Configuración de transformaciones
        """
        self.configuracion = configuracion or {}
        self.logger = logger.bind(componente="TransformadorComplejo")
        self._ultimas_operaciones = []
    
    def transformar_dataframe(
        self,
        df: pd.DataFrame,
        operaciones: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Aplica una serie de operaciones a un DataFrame.
        
        Args:
            df: DataFrame de entrada
            operaciones: Lista de configuraciones de operación
            
        Returns:
            DataFrame transformado
        """
        df_resultado = df.copy()
        self._ultimas_operaciones = []
        
        for idx, op in enumerate(operaciones):
            tipo_op = op.get('tipo')
            self._ultimas_operaciones.append(tipo_op)
            
            try:
                if tipo_op == 'limpiar_nulos':
                    df_resultado = self._limpiar_nulos(df_resultado, op.get('estrategia', 'eliminar'), op.get('columnas'))
                
                elif tipo_op == 'eliminar_duplicados':
                    df_resultado = self._eliminar_duplicados(df_resultado, op.get('subconjunto'))
                
                elif tipo_op == 'normalizar_texto':
                    df_resultado = self._normalizar_columnas_texto(df_resultado, op.get('columnas'))
                
                elif tipo_op == 'agrupar':
                    df_resultado = self._agrupar_datos(df_resultado, op)
                
                elif tipo_op == 'unir_datos':
                    df_resultado = self._unir_dataframes(df_resultado, op)
                
                elif tipo_op == 'tabla_dinamica':
                    df_resultado = self._crear_tabla_dinamica(df_resultado, op)
                
                elif tipo_op == 'aplicar_funcion':
                    df_resultado = self._aplicar_funcion_personalizada(df_resultado, op)
                
                elif tipo_op == 'renombrar_columnas':
                    df_resultado = self._renombrar_columnas(df_resultado, op.get('mapeo', {}))
                
                elif tipo_op == 'filtrar':
                    df_resultado = self._filtrar_datos(df_resultado, op.get('condicion'), op.get('columna'), op.get('valor'))
                
                elif tipo_op == 'ordenar':
                    df_resultado = self._ordenar_datos(df_resultado, op.get('por'), op.get('ascendente', True))
                
                elif tipo_op == 'convertir_tipos':
                    df_resultado = self._convertir_tipos(df_resultado, op.get('mapeo_tipos', {}))
                
                self.logger.debug(f"Operación aplicada", operacion=tipo_op, indice=idx)
                
            except Exception as e:
                self.logger.error(f"Error en operación {tipo_op}", error=str(e))
                raise ValueError(f"Error en operación {tipo_op}: {str(e)}")
        
        return df_resultado
    
    def _limpiar_nulos(self, df: pd.DataFrame, estrategia: str, columnas: Optional[List[str]]) -> pd.DataFrame:
        """Limpia valores nulos según estrategia."""
        if columnas:
            df_subset = df[columnas]
        else:
            df_subset = df
            columnas = df.columns.tolist()
        
        if estrategia == 'eliminar':
            return df.dropna(subset=columnas)
        elif estrategia == 'llenar_cero':
            df[columnas] = df_subset.fillna(0)
        elif estrategia == 'llenar_media':
            df[columnas] = df_subset.fillna(df_subset.mean(numeric_only=True))
        elif estrategia == 'llenar_mediana':
            df[columnas] = df_subset.fillna(df_subset.median(numeric_only=True))
        elif estrategia == 'llenar_adelante':
            df[columnas] = df_subset.fillna(method='ffill')
        elif estrategia == 'llenar_atras':
            df[columnas] = df_subset.fillna(method='bfill')
        elif estrategia == 'llenar_valor':
            valor = self.configuracion.get('valor_llenado', '')
            df[columnas] = df_subset.fillna(valor)
        
        return df
    
    def _eliminar_duplicados(self, df: pd.DataFrame, subconjunto: Optional[List[str]]) -> pd.DataFrame:
        """Elimina filas duplicadas."""
        if subconjunto:
            return df.drop_duplicates(subset=subconjunto, keep='first')
        return df.drop_duplicates()
    
    def _normalizar_columnas_texto(self, df: pd.DataFrame, columnas: Optional[List[str]]) -> pd.DataFrame:
        """Normaliza texto en columnas especificadas."""
        if not columnas:
            columnas = df.select_dtypes(include=['object']).columns.tolist()
        
        for col in columnas:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.lower()
                df[col] = df[col].replace('nan', None)
                df[col] = df[col].replace('none', None)
        
        return df
    
    def _agrupar_datos(self, df: pd.DataFrame, operacion: Dict) -> pd.DataFrame:
        """Realiza operaciones de agrupación."""
        agrupar_por = operacion.get('agrupar_por', [])
        agregaciones = operacion.get('agregaciones', {})
        
        if not agrupar_por or not agregaciones:
            self.logger.warning("Agrupación sin columnas o agregaciones")
            return df
        
        # Crear diccionario de agregaciones en formato pandas
        agg_dict = {}
        for columna, func in agregaciones.items():
            if columna in df.columns:
                agg_dict[columna] = func
        
        if not agg_dict:
            return df
        
        resultado = df.groupby(agrupar_por).agg(agg_dict).reset_index()
        
        # Aplanar nombres de columnas si es necesario
        if isinstance(resultado.columns, pd.MultiIndex):
            resultado.columns = ['_'.join(col).strip('_') for col in resultado.columns.values]
        
        return resultado
    
    def _unir_dataframes(self, df: pd.DataFrame, operacion: Dict) -> pd.DataFrame:
        """Une con otro DataFrame."""
        otro_df = operacion.get('otro_df')
        if otro_df is None:
            self.logger.warning("No se proporcionó otro DataFrame para la unión")
            return df
        
        como = operacion.get('como', 'inner')
        en = operacion.get('en')
        izquierda_en = operacion.get('izquierda_en')
        derecha_en = operacion.get('derecha_en')
        sufijos = operacion.get('sufijos', ('_x', '_y'))
        
        try:
            if en:
                return df.merge(otro_df, on=en, how=como, suffixes=sufijos)
            elif izquierda_en and derecha_en:
                return df.merge(otro_df, left_on=izquierda_en, right_on=derecha_en, how=como, suffixes=sufijos)
            else:
                return df.merge(otro_df, how=como, suffixes=sufijos)
        except Exception as e:
            self.logger.error(f"Error en unión de DataFrames", error=str(e))
            raise
    
    def _crear_tabla_dinamica(self, df: pd.DataFrame, operacion: Dict) -> pd.DataFrame:
        """Crea una tabla dinámica (pivot table)."""
        indice = operacion.get('indice', [])
        columnas = operacion.get('columnas', [])
        valores = operacion.get('valores', [])
        func_agregacion = operacion.get('func_agregacion', 'mean')
        fill_value = operacion.get('fill_value', 0)
        
        if not indice or not columnas or not valores:
            self.logger.warning("Faltan parámetros para tabla dinámica")
            return df
        
        dinamica = pd.pivot_table(
            df,
            index=indice,
            columns=columnas,
            values=valores,
            aggfunc=func_agregacion,
            fill_value=fill_value
        )
        
        return dinamica.reset_index()
    
    def _aplicar_funcion_personalizada(self, df: pd.DataFrame, operacion: Dict) -> pd.DataFrame:
        """Aplica función personalizada al DataFrame."""
        funcion = operacion.get('funcion')
        eje = operacion.get('eje', 0)  # 0 para columnas, 1 para filas
        columna = operacion.get('columna')
        
        if columna:
            # Aplicar a una columna específica
            if callable(funcion):
                df[columna] = df[columna].apply(funcion)
        elif callable(funcion):
            return df.apply(funcion, axis=eje)
        
        return df
    
    def _renombrar_columnas(self, df: pd.DataFrame, mapeo: Dict[str, str]) -> pd.DataFrame:
        """Renombra columnas del DataFrame."""
        return df.rename(columns=mapeo)
    
    def _filtrar_datos(self, df: pd.DataFrame, condicion: Optional[str], columna: Optional[str], valor: Any) -> pd.DataFrame:
        """Filtra datos según condición."""
        if condicion:
            return df.query(condicion)
        elif columna and valor is not None:
            return df[df[columna] == valor]
        return df
    
    def _ordenar_datos(self, df: pd.DataFrame, por: Union[str, List[str]], ascendente: bool = True) -> pd.DataFrame:
        """Ordena datos por columnas."""
        return df.sort_values(by=por, ascending=ascendente)
    
    def _convertir_tipos(self, df: pd.DataFrame, mapeo_tipos: Dict[str, str]) -> pd.DataFrame:
        """Convierte tipos de datos de columnas."""
        for columna, tipo in mapeo_tipos.items():
            if columna in df.columns:
                try:
                    if tipo == 'int':
                        df[columna] = pd.to_numeric(df[columna], errors='coerce').fillna(0).astype(int)
                    elif tipo == 'float':
                        df[columna] = pd.to_numeric(df[columna], errors='coerce').fillna(0.0)
                    elif tipo == 'datetime':
                        df[columna] = pd.to_datetime(df[columna], errors='coerce')
                    elif tipo == 'string':
                        df[columna] = df[columna].astype(str)
                    elif tipo == 'bool':
                        df[columna] = df[columna].astype(bool)
                except Exception as e:
                    self.logger.warning(f"Error convirtiendo columna {columna} a {tipo}", error=str(e))
        
        return df
    
    def detectar_valores_atipicos(
        self,
        df: pd.DataFrame,
        columna: str,
        metodo: str = 'iqr'
    ) -> pd.Series:
        """Detecta valores atípicos en una columna."""
        if metodo == 'iqr':
            Q1 = df[columna].quantile(0.25)
            Q3 = df[columna].quantile(0.75)
            IQR = Q3 - Q1
            limite_inferior = Q1 - 1.5 * IQR
            limite_superior = Q3 + 1.5 * IQR
            return (df[columna] < limite_inferior) | (df[columna] > limite_superior)
        
        elif metodo == 'zscore':
            z_scores = np.abs((df[columna] - df[columna].mean()) / df[columna].std())
            return z_scores > 3
        
        return pd.Series([False] * len(df))
    
    def llenar_valores_faltantes(
        self,
        df: pd.DataFrame,
        estrategia: str = 'media',
        columnas: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Llena valores faltantes usando varias estrategias."""
        if columnas is None:
            columnas = df.columns
        
        df_resultado = df.copy()
        
        for col in columnas:
            if col not in df.columns:
                continue
            
            if estrategia == 'media':
                df_resultado[col].fillna(df[col].mean(), inplace=True)
            elif estrategia == 'mediana':
                df_resultado[col].fillna(df[col].median(), inplace=True)
            elif estrategia == 'moda':
                moda = df[col].mode()
                if not moda.empty:
                    df_resultado[col].fillna(moda[0], inplace=True)
            elif estrategia == 'interpolar':
                df_resultado[col].interpolate(method='linear', inplace=True)
        
        return df_resultado
    
    def obtener_estadisticas(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Obtiene estadísticas descriptivas del DataFrame."""
        return {
            'filas': len(df),
            'columnas': len(df.columns),
            'nulos_por_columna': df.isnull().sum().to_dict(),
            'tipos_por_columna': df.dtypes.astype(str).to_dict(),
            'estadisticas_numericas': df.describe().to_dict() if len(df.select_dtypes(include=['number']).columns) > 0 else {},
            'ultimas_operaciones': self._ultimas_operaciones
        }
