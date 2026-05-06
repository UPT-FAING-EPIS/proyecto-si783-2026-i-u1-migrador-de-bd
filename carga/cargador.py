import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Any, List, Union
import os

class CargadorDestino:
    def __init__(self, motor_destino: str):
        self.motor = motor_destino
        self.engine = None
        self.ruta_salida = None
        
        # Usar ruta absoluta
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        upload_dir = os.path.join(base_dir, 'uploads')
        os.makedirs(upload_dir, exist_ok=True)
        
        nombre_archivo = f"migracion_{motor_destino.lower().replace(' ', '_')}.db"
        self.ruta_salida = os.path.join(upload_dir, nombre_archivo)
        self.engine = create_engine(f'sqlite:///{self.ruta_salida}')
    
    @staticmethod
    def _nombre_seguro(nombre: str) -> str:
        return str(nombre).replace(' ', '_').replace('-', '_').replace('.', '_')

    # Tipos de origen que se mapean a tipos SQLite
    INTEGER_TYPES = ('INT', 'SERIAL', 'BIGINT', 'SMALLINT')
    REAL_TYPES = ('REAL', 'FLOAT', 'DOUBLE', 'NUMERIC', 'DECIMAL')
    BLOB_TYPES = ('BLOB', 'BINARY', 'BYTES')

    def crear_estructura(self, esquema: Dict[str, Any]):
        creadas = 0
        with self.engine.connect() as conn:
            for tabla, info in esquema.items():
                nombre_tabla = self._nombre_seguro(tabla)
                if not nombre_tabla:
                    continue

                # Soporte para nuevo formato (dict) y formato antiguo (list)
                if isinstance(info, dict):
                    columnas = info.get('columnas', [])
                    pks = info.get('claves_primarias', [])
                    fks = info.get('claves_foraneas', [])
                    indices = info.get('indices', [])
                else:
                    columnas = info
                    pks = []
                    fks = []
                    indices = []

                if not columnas:
                    continue

                cols_sql = []
                for col in columnas:
                    col_nombre = self._nombre_seguro(col.get('nombre', 'col'))
                    tipo_origen = str(col.get('tipo', 'TEXT')).upper()
                    # Mapear tipos comunes a SQLite
                    if any(t in tipo_origen for t in self.INTEGER_TYPES):
                        tipo_sql = 'INTEGER'
                    elif any(t in tipo_origen for t in self.REAL_TYPES):
                        tipo_sql = 'REAL'
                    elif any(t in tipo_origen for t in self.BLOB_TYPES):
                        tipo_sql = 'BLOB'
                    else:
                        tipo_sql = 'TEXT'
                    nullable = '' if col.get('nullable', True) else ' NOT NULL'
                    default = col.get('default')
                    default_sql = f" DEFAULT {default}" if default is not None else ''
                    cols_sql.append(f'"{col_nombre}" {tipo_sql}{nullable}{default_sql}')

                if pks:
                    pk_cols = ', '.join(f'"{self._nombre_seguro(p)}"' for p in pks)
                    cols_sql.append(f'PRIMARY KEY ({pk_cols})')

                for fk in fks:
                    fk_cols = ', '.join(f'"{self._nombre_seguro(c)}"' for c in fk.get('columnas', []))
                    ref_tabla = self._nombre_seguro(fk.get('tabla_ref', ''))
                    ref_cols = ', '.join(f'"{self._nombre_seguro(c)}"' for c in fk.get('columnas_ref', []))
                    if fk_cols and ref_tabla and ref_cols:
                        cols_sql.append(
                            f'FOREIGN KEY ({fk_cols}) REFERENCES "{ref_tabla}" ({ref_cols})'
                        )

                sql = f'CREATE TABLE IF NOT EXISTS "{nombre_tabla}" ({", ".join(cols_sql)})'
                try:
                    conn.execute(text(sql))
                    creadas += 1
                except Exception as e:
                    print(f"Aviso creando tabla {nombre_tabla}: {e}")

                # Crear índices
                for idx in indices:
                    idx_nombre = self._nombre_seguro(idx.get('nombre') or f'idx_{nombre_tabla}')
                    idx_cols = ', '.join(
                        f'"{self._nombre_seguro(c)}"' for c in idx.get('columnas', []) if c
                    )
                    if not idx_cols:
                        continue
                    unico = 'UNIQUE ' if idx.get('unico') else ''
                    sql_idx = (
                        f'CREATE {unico}INDEX IF NOT EXISTS "{idx_nombre}" '
                        f'ON "{nombre_tabla}" ({idx_cols})'
                    )
                    try:
                        conn.execute(text(sql_idx))
                    except Exception as e:
                        print(f"Aviso creando índice {idx_nombre}: {e}")

            conn.commit()
        return creadas
    
    def cargar_tabla(self, tabla: str, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        
        nombre_tabla = self._nombre_seguro(tabla)
        df.columns = [self._nombre_seguro(c) for c in df.columns]
        
        try:
            df.to_sql(nombre_tabla, self.engine, if_exists='replace', index=False)
            return len(df)
        except Exception as e:
            print(f"Error cargando: {e}")
            return 0
    
    def generar_sql_dump(self) -> str:
        """Genera un dump SQL del SQLite migrado"""
        import sqlite3
        
        if not os.path.exists(self.ruta_salida):
            return ""
        
        conn = sqlite3.connect(self.ruta_salida)
        cursor = conn.cursor()
        
        sql_dump = "-- SQL Dump generado por MigradorBD\n"
        sql_dump += "-- Fecha: " + str(__import__('datetime').datetime.now()) + "\n\n"
        
        # Obtener todas las tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tablas = cursor.fetchall()
        
        for (tabla,) in tablas:
            # Obtener CREATE TABLE
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{tabla}'")
            create_sql = cursor.fetchone()
            if create_sql and create_sql[0]:
                sql_dump += create_sql[0] + ";\n\n"
            
            # Obtener datos
            cursor.execute(f"SELECT * FROM `{tabla}`")
            filas = cursor.fetchall()
            
            if filas:
                # Obtener nombres de columnas
                cursor.execute(f"PRAGMA table_info(`{tabla}`)")
                columnas = [col[1] for col in cursor.fetchall()]
                
                # Generar INSERT statements
                cols_str = ', '.join(f'`{col}`' for col in columnas)
                for fila in filas:
                    valores = []
                    for valor in fila:
                        if valor is None:
                            valores.append('NULL')
                        elif isinstance(valor, str):
                            # Escapar comillas simples
                            valor_escape = valor.replace("'", "''")
                            valores.append(f"'{valor_escape}'")
                        elif isinstance(valor, (int, float)):
                            valores.append(str(valor))
                        else:
                            valores.append(f"'{str(valor)}'")
                    valores_str = ', '.join(valores)
                    sql_dump += f"INSERT INTO `{tabla}` ({cols_str}) VALUES ({valores_str});\n"
                sql_dump += "\n"
        
        conn.close()
        return sql_dump
    
    def generar_export(self, motor: str = None) -> tuple:
        """Genera exportación en el formato específico del motor.
        Retorna (contenido, extensión, mimetype, es_binario)
        Si es_binario=True, el contenido es la ruta al archivo"""
        if motor is None:
            motor = self.motor.lower()
        else:
            motor = motor.lower()
        
        # SQLite - retornar archivo binario
        if 'sqlite' in motor:
            return (self.ruta_salida, '.db', 'application/x-sqlite3', True)
        
        # SQL databases
        elif any(x in motor for x in ['mysql', 'postgres', 'oracle', 'sql server', 'mariadb']):
            return (self._generar_sql(), '.sql', 'application/sql', False)
        
        # MongoDB - JSON format
        elif 'mongo' in motor:
            return (self._generar_json(), '.json', 'application/json', False)
        
        # Elasticsearch - NDJSON (newline-delimited JSON)
        elif 'elasticsearch' in motor:
            return (self._generar_ndjson(), '.ndjson', 'application/x-ndjson', False)
        
        # Cassandra - CQL
        elif 'cassandra' in motor:
            return (self._generar_cql(), '.cql', 'text/plain', False)
        
        # Redis - Redis commands
        elif 'redis' in motor:
            return (self._generar_redis(), '.redis', 'text/plain', False)
        
        # Default - JSON
        else:
            return (self._generar_json(), '.json', 'application/json', False)
    
    def _generar_sql(self) -> str:
        """Genera dump SQL para bases de datos SQL (MySQL, PostgreSQL, Oracle, etc)"""
        return self.generar_sql_dump()
    
    def _generar_json(self) -> str:
        """Genera JSON para MongoDB y otros JSON-based databases"""
        import sqlite3
        import json
        from datetime import datetime
        
        if not os.path.exists(self.ruta_salida):
            return ""
        
        conn = sqlite3.connect(self.ruta_salida)
        cursor = conn.cursor()
        
        export_data = {
            "metadata": {
                "generator": "MigradorBD",
                "timestamp": datetime.now().isoformat(),
                "motor": self.motor
            },
            "collections": {}
        }
        
        # Obtener todas las tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tablas = cursor.fetchall()
        
        for (tabla,) in tablas:
            cursor.execute(f"PRAGMA table_info(`{tabla}`)")
            columnas = [col[1] for col in cursor.fetchall()]
            
            cursor.execute(f"SELECT * FROM `{tabla}`")
            filas = cursor.fetchall()
            
            docs = []
            for fila in filas:
                doc = {}
                for col, valor in zip(columnas, fila):
                    doc[col] = valor
                docs.append(doc)
            
            export_data["collections"][tabla] = docs
        
        conn.close()
        return json.dumps(export_data, ensure_ascii=False, indent=2, default=str)
    
    def _generar_ndjson(self) -> str:
        """Genera NDJSON para Elasticsearch"""
        import sqlite3
        import json
        
        if not os.path.exists(self.ruta_salida):
            return ""
        
        conn = sqlite3.connect(self.ruta_salida)
        cursor = conn.cursor()
        
        ndjson_lines = []
        
        # Obtener todas las tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tablas = cursor.fetchall()
        
        for (tabla,) in tablas:
            cursor.execute(f"PRAGMA table_info(`{tabla}`)")
            columnas = [col[1] for col in cursor.fetchall()]
            
            cursor.execute(f"SELECT * FROM `{tabla}`")
            filas = cursor.fetchall()
            
            for idx, fila in enumerate(filas):
                # Metadata line (para Elasticsearch bulk API)
                metadata = json.dumps({"index": {"_index": tabla, "_id": idx}})
                ndjson_lines.append(metadata)
                
                # Data line
                doc = {}
                for col, valor in zip(columnas, fila):
                    doc[col] = valor
                data = json.dumps(doc, ensure_ascii=False, default=str)
                ndjson_lines.append(data)
        
        conn.close()
        return '\n'.join(ndjson_lines)
    
    def _generar_cql(self) -> str:
        """Genera CQL (Cassandra Query Language) para Apache Cassandra"""
        import sqlite3
        
        if not os.path.exists(self.ruta_salida):
            return ""
        
        conn = sqlite3.connect(self.ruta_salida)
        cursor = conn.cursor()
        
        cql_dump = "-- CQL Script generado por MigradorBD\n"
        cql_dump += "-- Fecha: " + str(__import__('datetime').datetime.now()) + "\n\n"
        cql_dump += "-- Cassandra keyspace (crear manualmente si es necesario)\n"
        cql_dump += "-- USE migracion_keyspace;\n\n"
        
        # Obtener todas las tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tablas = cursor.fetchall()
        
        for (tabla,) in tablas:
            cursor.execute(f"PRAGMA table_info(`{tabla}`)")
            columnas_info = cursor.fetchall()
            columnas = [col[1] for col in columnas_info]
            
            # Crear tabla CQL (conversión simple de tipos)
            cql_dump += f"CREATE TABLE IF NOT EXISTS {tabla} (\n"
            for i, col_info in enumerate(columnas_info):
                col_name = col_info[1]
                # Mapeo simple de tipos SQLite a CQL
                cql_dump += f"  {col_name} text"
                if i < len(columnas_info) - 1:
                    cql_dump += ",\n"
                else:
                    cql_dump += ",\n  PRIMARY KEY (id)\n"  # Simplified
            cql_dump += ") WITH CLUSTERING ORDER BY (id DESC);\n\n"
            
            # Obtener datos
            cursor.execute(f"SELECT * FROM `{tabla}`")
            filas = cursor.fetchall()
            
            if filas:
                for fila in filas:
                    valores = []
                    for valor in fila:
                        if valor is None:
                            valores.append("null")
                        elif isinstance(valor, str):
                            valor_escape = valor.replace("'", "''")
                            valores.append(f"'{valor_escape}'")
                        elif isinstance(valor, (int, float)):
                            valores.append(str(valor))
                        else:
                            valores.append(f"'{str(valor)}'")
                    
                    cols_str = ', '.join(columnas)
                    valores_str = ', '.join(valores)
                    cql_dump += f"INSERT INTO {tabla} ({cols_str}) VALUES ({valores_str});\n"
                cql_dump += "\n"
        
        conn.close()
        return cql_dump
    
    def _generar_redis(self) -> str:
        """Genera comandos Redis para importar datos"""
        import sqlite3
        
        if not os.path.exists(self.ruta_salida):
            return ""
        
        conn = sqlite3.connect(self.ruta_salida)
        cursor = conn.cursor()
        
        redis_cmds = "# Redis commands generadas por MigradorBD\n"
        redis_cmds += "# Fecha: " + str(__import__('datetime').datetime.now()) + "\n\n"
        redis_cmds += "# Usar: redis-cli < archivo.redis\n\n"
        
        # Obtener todas las tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        tablas = cursor.fetchall()
        
        for (tabla,) in tablas:
            cursor.execute(f"PRAGMA table_info(`{tabla}`)")
            columnas = [col[1] for col in cursor.fetchall()]
            
            cursor.execute(f"SELECT * FROM `{tabla}`")
            filas = cursor.fetchall()
            
            for idx, fila in enumerate(filas):
                # Usar HSET para almacenar registros como hashes
                key = f"{tabla}:{idx}"
                redis_cmds += f"HSET {key}"
                
                for col, valor in zip(columnas, fila):
                    if valor is not None:
                        valor_str = str(valor).replace('"', '\\"')
                        redis_cmds += f' {col} "{valor_str}"'
                
                redis_cmds += "\n"
            
            redis_cmds += "\n"
        
        conn.close()
        return redis_cmds
    
    def get_ruta_salida(self) -> str:
        return self.ruta_salida