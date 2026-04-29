import sqlite3
import pandas as pd
import json
import os
from typing import Dict, Any, List, Optional
from sqlalchemy import create_engine, inspect, text

class ConectorOrigen:
    """Conecta y extrae datos de cualquier archivo de base de datos"""
    
    def __init__(self, ruta: str, tipo: str):
        self.ruta = ruta
        self.tipo = tipo
        self.engine = None
        self.tablas = []
        self.esquema = {}
        
        if tipo == 'SQLite':
            self.engine = create_engine(f'sqlite:///{ruta}')
            self._descubrir_sqlite()
        elif tipo in ['PostgreSQL', 'MySQL', 'Microsoft SQL Server', 'Oracle', 'SQL Generico']:
            self._analizar_sql()
        elif tipo == 'MongoDB':
            self._cargar_json()
        elif tipo == 'CSV':
            self._cargar_csv()
        elif tipo == 'Excel':
            self._cargar_excel()
        elif tipo == 'Elasticsearch':
            self._cargar_json()
    
    def _descubrir_sqlite(self):
        inspector = inspect(self.engine)
        self.tablas = [t for t in inspector.get_table_names() if not t.startswith('sqlite_')]
        
        for tabla in self.tablas:
            columnas = inspector.get_columns(tabla)
            self.esquema[tabla] = [
                {'nombre': c['name'], 'tipo': str(c['type'])} 
                for c in columnas
            ]
    
    def _analizar_sql(self):
        with open(self.ruta, 'r', encoding='utf-8', errors='ignore') as f:
            contenido = f.read()
        
        self.tablas = []
        for linea in contenido.split('\n'):
            if 'CREATE TABLE' in linea.upper():
                partes = linea.replace('`', '').replace('"', '').replace('[', '').replace(']', '').split()
                for i, p in enumerate(partes):
                    if p.upper() == 'TABLE' and i+1 < len(partes):
                        tabla = partes[i+1].strip('();')
                        if tabla not in self.tablas:
                            self.tablas.append(tabla)
        
        self.esquema = {t: [{'nombre': 'columna_1', 'tipo': 'TEXT'}] for t in self.tablas}
    
    def _cargar_json(self):
        with open(self.ruta, 'r', encoding='utf-8') as f:
            datos = json.load(f)
        
        if isinstance(datos, list):
            self.tablas = ['documentos']
            if datos:
                self.esquema = {'documentos': [
                    {'nombre': k, 'tipo': str(type(v).__name__)} 
                    for k, v in datos[0].items()
                ]}
            self._datos_json = datos
        else:
            self.tablas = ['datos']
            self.esquema = {'datos': [{'nombre': 'contenido', 'tipo': 'JSON'}]}
    
    def _cargar_csv(self):
        nombre = os.path.basename(self.ruta).replace('.csv', '')
        self.tablas = [nombre]
        df = pd.read_csv(self.ruta, nrows=1)
        self.esquema = {nombre: [
            {'nombre': col, 'tipo': str(df[col].dtype)} 
            for col in df.columns
        ]}
    
    def _cargar_excel(self):
        xls = pd.ExcelFile(self.ruta)
        self.tablas = xls.sheet_names
        
        for hoja in self.tablas:
            df = pd.read_excel(self.ruta, sheet_name=hoja, nrows=1)
            self.esquema[hoja] = [
                {'nombre': col, 'tipo': str(df[col].dtype)} 
                for col in df.columns
            ]
    
    def extraer_datos(self, tabla: str) -> pd.DataFrame:
        """Extrae datos de una tabla especifica"""
        if self.tipo == 'SQLite':
            return pd.read_sql(f'SELECT * FROM "{tabla}"', self.engine)
        elif self.tipo == 'CSV':
            return pd.read_csv(self.ruta)
        elif self.tipo == 'Excel':
            return pd.read_excel(self.ruta, sheet_name=tabla)
        elif self.tipo == 'MongoDB' and hasattr(self, '_datos_json'):
            return pd.DataFrame(self._datos_json)
        return pd.DataFrame()