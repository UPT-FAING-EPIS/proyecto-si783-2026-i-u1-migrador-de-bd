from sqlalchemy import inspect, MetaData, Table
from typing import Dict, Any, List
import json

from extraccion.conector_base_datos import ConectorBaseDatos
from utilidades.registro_logging import ConfiguradorLogging

class DescubridorEsquema:
    """Descubre automáticamente la estructura de cualquier base de datos"""
    
    def __init__(self, conector: ConectorBaseDatos):
        self.conector = conector
        self.logger = ConfiguradorLogging.obtener_logger(__name__)
    
    def descubrir_esquema_completo(self) -> Dict[str, Any]:
        """Descubre todas las tablas, columnas, tipos y relaciones"""
        esquema = {
            "motor": self.conector.config.get("motor"),
            "nombre_bd": self.conector.config.get("nombre_bd"),
            "tablas": {},
            "vistas": [],
            "procedimientos": []
        }
        
        with self.conector.obtener_conexion() as conexion:
            inspector = inspect(conexion)
            
            # Descubrir todas las tablas
            tablas = inspector.get_table_names()
            
            for tabla in tablas:
                self.logger.info(f"descubriendo_tabla", tabla=tabla)
                
                info_tabla = {
                    "nombre": tabla,
                    "columnas": [],
                    "claves_primarias": [],
                    "claves_foraneas": [],
                    "indices": []
                }
                
                # Descubrir columnas
                columnas = inspector.get_columns(tabla)
                for columna in columnas:
                    info_columna = {
                        "nombre": columna["name"],
                        "tipo": str(columna["type"]),
                        "nullable": columna.get("nullable", True),
                        "valor_defecto": str(columna.get("default")) if columna.get("default") else None,
                        "autoincrementable": columna.get("autoincrement", False)
                    }
                    info_tabla["columnas"].append(info_columna)
                
                # Descubrir claves primarias
                pk = inspector.get_pk_constraint(tabla)
                if pk and pk.get("constrained_columns"):
                    info_tabla["claves_primarias"] = pk["constrained_columns"]
                
                # Descubrir claves foráneas
                fks = inspector.get_foreign_keys(tabla)
                for fk in fks:
                    info_fk = {
                        "columnas_origen": fk["constrained_columns"],
                        "tabla_referenciada": fk["referred_table"],
                        "columnas_referenciadas": fk["referred_columns"]
                    }
                    info_tabla["claves_foraneas"].append(info_fk)
                
                # Descubrir índices
                indices = inspector.get_indexes(tabla)
                info_tabla["indices"] = [
                    {
                        "nombre": idx["name"],
                        "columnas": idx["column_names"],
                        "unico": idx.get("unique", False)
                    }
                    for idx in indices
                ]
                
                esquema["tablas"][tabla] = info_tabla
            
            # Descubrir vistas
            try:
                vistas = inspector.get_view_names()
                esquema["vistas"] = vistas
            except:
                pass
        
        return esquema
    
    def generar_script_creacion(self, esquema: Dict[str, Any]) -> str:
        """Genera script SQL para recrear el esquema en otra base de datos"""
        motor_destino = "postgresql"  # Se puede configurar
        script = []
        
        script.append(f"-- Script de creación generado automáticamente")
        script.append(f"-- Motor origen: {esquema.get('motor')}")
        script.append(f"-- Base de datos: {esquema.get('nombre_bd')}")
        script.append("")
        
        for nombre_tabla, info_tabla in esquema.get("tablas", {}).items():
            script.append(f"-- Tabla: {nombre_tabla}")
            
            columnas_sql = []
            for columna in info_tabla["columnas"]:
                tipo_sql = self._convertir_tipo(columna["tipo"], motor_destino)
                
                col_sql = f"    {columna['nombre']} {tipo_sql}"
                
                if not columna.get("nullable", True):
                    col_sql += " NOT NULL"
                
                if columna.get("valor_defecto"):
                    col_sql += f" DEFAULT {columna['valor_defecto']}"
                
                columnas_sql.append(col_sql)
            
            # Agregar clave primaria
            if info_tabla["claves_primarias"]:
                pk = ", ".join(info_tabla["claves_primarias"])
                columnas_sql.append(f"    PRIMARY KEY ({pk})")
            
            script.append(f"CREATE TABLE IF NOT EXISTS {nombre_tabla} (")
            script.append(",\n".join(columnas_sql))
            script.append(");")
            script.append("")
        
        return "\n".join(script)
    
    def _convertir_tipo(self, tipo_origen: str, motor_destino: str) -> str:
        """Convierte tipos de datos entre diferentes motores de BD"""
        tipo_origen = tipo_origen.upper()
        
        mapeo_tipos = {
            "INTEGER": "INTEGER",
            "INT": "INTEGER",
            "BIGINT": "BIGINT",
            "SMALLINT": "SMALLINT",
            "VARCHAR": "VARCHAR",
            "CHAR": "CHAR",
            "TEXT": "TEXT",
            "FLOAT": "FLOAT",
            "DOUBLE": "DOUBLE PRECISION",
            "DECIMAL": "DECIMAL",
            "NUMERIC": "NUMERIC",
            "BOOLEAN": "BOOLEAN",
            "BOOL": "BOOLEAN",
            "DATE": "DATE",
            "TIME": "TIME",
            "TIMESTAMP": "TIMESTAMP",
            "DATETIME": "TIMESTAMP",
            "BLOB": "BYTEA",
            "JSON": "JSONB",
            "UUID": "UUID"
        }
        
        # Buscar coincidencia parcial
        for clave, valor in mapeo_tipos.items():
            if clave in tipo_origen:
                return valor
        
        return tipo_origen
    
    def guardar_esquema(self, esquema: Dict[str, Any], archivo: str = "esquema_descubierto.json"):
        """Guarda el esquema descubierto en un archivo JSON"""
        import json
        with open(archivo, "w", encoding="utf-8") as f:
            json.dump(esquema, f, indent=2, default=str)
        self.logger.info(f"esquema_guardado", archivo=archivo)
    
    def comparar_esquemas(self, esquema_origen: Dict, esquema_destino: Dict) -> Dict:
        """Compara dos esquemas y encuentra diferencias"""
        diferencias = {
            "tablas_faltantes": [],
            "tablas_sobrantes": [],
            "columnas_diferentes": {}
        }
        
        tablas_origen = set(esquema_origen.get("tablas", {}).keys())
        tablas_destino = set(esquema_destino.get("tablas", {}).keys())
        
        diferencias["tablas_faltantes"] = list(tablas_origen - tablas_destino)
        diferencias["tablas_sobrantes"] = list(tablas_destino - tablas_origen)
        
        for tabla in tablas_origen & tablas_destino:
            cols_origen = {c["nombre"]: c["tipo"] for c in esquema_origen["tablas"][tabla]["columnas"]}
            cols_destino = {c["nombre"]: c["tipo"] for c in esquema_destino["tablas"][tabla]["columnas"]}
            
            if cols_origen != cols_destino:
                diferencias["columnas_diferentes"][tabla] = {
                    "faltantes": list(set(cols_origen.keys()) - set(cols_destino.keys())),
                    "sobrantes": list(set(cols_destino.keys()) - set(cols_origen.keys())),
                    "tipos_diferentes": {
                        col: {"origen": cols_origen[col], "destino": cols_destino.get(col)}
                        for col in set(cols_origen.keys()) & set(cols_destino.keys())
                        if cols_origen[col] != cols_destino[col]
                    }
                }
        
        return diferencias