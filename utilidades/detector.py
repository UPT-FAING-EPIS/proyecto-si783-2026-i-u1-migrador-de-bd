import os
import json
import sqlite3
import pandas as pd
from typing import Dict, Any, Optional, Tuple

class DetectorBaseDatos:
    """Detecta automaticamente el tipo de base de datos"""
    
    @staticmethod
    def detectar(ruta: str, nombre: str) -> Tuple[str, str, Optional[Any]]:
        """
        Retorna: (tipo_detectado, mensaje, conexion_engine)
        """
        # Intentar SQLite primero (funciona con cualquier extension)
        try:
            conn = sqlite3.connect(ruta)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tablas = cursor.fetchall()
            if tablas:
                conn.close()
                return 'SQLite', f'SQLite: {len(tablas)} tablas', None
            conn.close()
        except:
            pass
        
        # Intentar SQL dump
        try:
            with open(ruta, 'r', encoding='utf-8', errors='ignore') as f:
                contenido = f.read(10000).upper()
            
            if 'CREATE TABLE' in contenido:
                if 'POSTGRES' in contenido or 'PG_' in contenido:
                    return 'PostgreSQL', 'Dump PostgreSQL detectado', None
                elif 'MYSQL' in contenido or 'AUTO_INCREMENT' in contenido:
                    return 'MySQL', 'Dump MySQL detectado', None
                elif 'MSSQL' in contenido or 'NVARCHAR' in contenido:
                    return 'Microsoft SQL Server', 'Dump SQL Server detectado', None
                elif 'ORACLE' in contenido or 'VARCHAR2' in contenido:
                    return 'Oracle', 'Dump Oracle detectado', None
                else:
                    return 'SQL Generico', 'Script SQL detectado', None
        except:
            pass
        
        # Intentar JSON (MongoDB/Elasticsearch)
        if nombre.lower().endswith('.json'):
            try:
                with open(ruta, 'r', encoding='utf-8') as f:
                    datos = json.load(f)
                if isinstance(datos, list):
                    return 'MongoDB', f'JSON: {len(datos)} documentos', None
                elif isinstance(datos, dict) and ('hits' in datos or 'mappings' in datos):
                    return 'Elasticsearch', 'JSON Elasticsearch detectado', None
            except:
                pass
        
        # Intentar CSV
        if nombre.lower().endswith('.csv'):
            try:
                df = pd.read_csv(ruta, nrows=1)
                return 'CSV', f'CSV: {len(df.columns)} columnas', None
            except:
                pass
        
        # Intentar Excel
        if nombre.lower().endswith(('.xlsx', '.xls')):
            try:
                xls = pd.ExcelFile(ruta)
                return 'Excel', f'Excel: {len(xls.sheet_names)} hojas', None
            except:
                pass
        
        return 'Desconocido', 'No se pudo detectar el tipo', None