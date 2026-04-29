import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Any, List
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
    
    def crear_estructura(self, esquema: Dict[str, List[Dict]]):
        with self.engine.connect() as conn:
            for tabla, columnas in esquema.items():
                nombre_tabla = str(tabla).replace(' ', '_').replace('-', '_').replace('.', '_')
                if not nombre_tabla:
                    continue
                
                cols_sql = []
                for col in columnas:
                    col_nombre = str(col.get('nombre', 'col')).replace(' ', '_').replace('-', '_')
                    cols_sql.append(f'"{col_nombre}" TEXT')
                
                if cols_sql:
                    sql = f'CREATE TABLE IF NOT EXISTS "{nombre_tabla}" ({", ".join(cols_sql)})'
                    try:
                        conn.execute(text(sql))
                    except Exception as e:
                        print(f"Aviso: {e}")
            conn.commit()
        return len(esquema)
    
    def cargar_tabla(self, tabla: str, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        
        nombre_tabla = str(tabla).replace(' ', '_').replace('-', '_').replace('.', '_')
        df.columns = [str(c).replace(' ', '_').replace('-', '_').replace('.', '_') for c in df.columns]
        
        try:
            df.to_sql(nombre_tabla, self.engine, if_exists='replace', index=False)
            return len(df)
        except Exception as e:
            print(f"Error cargando: {e}")
            return 0
    
    def get_ruta_salida(self) -> str:
        return self.ruta_salida