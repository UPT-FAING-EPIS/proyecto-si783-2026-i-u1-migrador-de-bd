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
                    if any(t in tipo_origen for t in ('INT', 'SERIAL', 'BIGINT', 'SMALLINT')):
                        tipo_sql = 'INTEGER'
                    elif any(t in tipo_origen for t in ('REAL', 'FLOAT', 'DOUBLE', 'NUMERIC', 'DECIMAL')):
                        tipo_sql = 'REAL'
                    elif any(t in tipo_origen for t in ('BLOB', 'BINARY', 'BYTES')):
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
    
    def get_ruta_salida(self) -> str:
        return self.ruta_salida