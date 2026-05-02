import os
import json
import sqlite3
import pandas as pd
from typing import Dict, Any, Optional, Tuple

class DetectorBaseDatos:
    """Detecta automaticamente el tipo de base de datos a partir del contenido del archivo."""

    @staticmethod
    def detectar(ruta: str, nombre: str) -> Tuple[str, str, Optional[Any]]:
        """
        Intenta detectar el tipo de base de datos a partir del contenido real del archivo,
        sin depender exclusivamente de la extension.
        Retorna: (tipo_detectado, mensaje, conexion_engine)
        """
        ext = os.path.splitext(nombre)[1].lower()

        # 1. Intentar SQLite (archivo binario): funciona con cualquier extension
        try:
            conn = sqlite3.connect(ruta)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tablas = cursor.fetchall()
            conn.close()
            if tablas:
                return 'SQLite', f'SQLite: {len(tablas)} tablas', None
        except Exception:
            pass

        # 2. Intentar leer como texto para detectar SQL dumps
        try:
            with open(ruta, 'r', encoding='utf-8', errors='ignore') as f:
                contenido = f.read(20000).upper()

            if 'CREATE TABLE' in contenido:
                if 'POSTGRES' in contenido or 'PG_' in contenido or 'SERIAL' in contenido:
                    return 'PostgreSQL', 'Dump PostgreSQL detectado', None
                elif 'AUTO_INCREMENT' in contenido or 'ENGINE=INNODB' in contenido or 'ENGINE=MYISAM' in contenido:
                    return 'MySQL', 'Dump MySQL detectado', None
                elif 'NVARCHAR' in contenido or 'IDENTITY(' in contenido or 'GO\n' in contenido:
                    return 'Microsoft SQL Server', 'Dump SQL Server detectado', None
                elif 'VARCHAR2' in contenido or 'NUMBER(' in contenido or 'TABLESPACE' in contenido:
                    return 'Oracle', 'Dump Oracle detectado', None
                else:
                    return 'SQL Generico', 'Script SQL detectado', None
        except Exception:
            pass

        # 3. Intentar JSON (MongoDB / Elasticsearch)
        try:
            with open(ruta, 'r', encoding='utf-8') as f:
                datos = json.load(f)
            if isinstance(datos, list):
                return 'MongoDB', f'JSON: {len(datos)} documentos', None
            elif isinstance(datos, dict) and ('hits' in datos or 'mappings' in datos or '_index' in datos):
                return 'Elasticsearch', 'JSON Elasticsearch detectado', None
            elif isinstance(datos, dict):
                return 'MongoDB', 'JSON objeto detectado', None
        except Exception:
            pass

        # 4. Intentar CSV (por contenido, no solo por extension)
        try:
            df = pd.read_csv(ruta, nrows=5)
            if len(df.columns) >= 2:
                return 'CSV', f'CSV: {len(df.columns)} columnas', None
        except Exception:
            pass

        # 5. Intentar Excel
        try:
            xls = pd.ExcelFile(ruta)
            return 'Excel', f'Excel: {len(xls.sheet_names)} hojas', None
        except Exception:
            pass

        # 6. Ultimo recurso: usar la extension como pista
        ext_map = {
            '.db': 'SQLite', '.sqlite': 'SQLite', '.sqlite3': 'SQLite',
            '.sql': 'SQL Generico', '.dump': 'SQL Generico', '.bak': 'SQL Generico',
            '.dmp': 'SQL Generico',
            '.json': 'MongoDB', '.bson': 'MongoDB',
            '.csv': 'CSV', '.tsv': 'CSV',
            '.xlsx': 'Excel', '.xls': 'Excel', '.ods': 'Excel',
        }
        if ext in ext_map:
            tipo = ext_map[ext]
            return tipo, f'Tipo inferido por extensión ({ext}): {tipo}', None

        return 'Desconocido', (
            'No se pudo detectar el tipo de base de datos. '
            'Verifique que el archivo sea un formato soportado: '
            '.db, .sqlite, .sql, .dump, .json, .csv, .xlsx, etc.'
        ), None