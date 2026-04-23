from typing import Dict, Any, Optional
import json

class ConectorMultiBase:
    """Soporte para las 21 bases de datos diferentes"""
    
    BASES_SOPORTADAS = {
        # Relacionales
        "MySQL": {
            "driver": "mysql+mysqlconnector",
            "puerto_default": 3306,
            "tipo": "relacional"
        },
        "Oracle": {
            "driver": "oracle+cx_oracle",
            "puerto_default": 1521,
            "tipo": "relacional"
        },
        "Microsoft SQL Server": {
            "driver": "mssql+pymssql",
            "puerto_default": 1433,
            "tipo": "relacional"
        },
        "PostgreSQL": {
            "driver": "postgresql+psycopg2",
            "puerto_default": 5432,
            "tipo": "relacional"
        },
        "CockroachDB": {
            "driver": "cockroachdb",
            "puerto_default": 26257,
            "tipo": "relacional"
        },
        "Yugabyte DB": {
            "driver": "postgresql+psycopg2",
            "puerto_default": 5433,
            "tipo": "relacional"
        },
        
        # NoSQL - Documentos
        "MongoDB": {
            "driver": "mongodb",
            "puerto_default": 27017,
            "tipo": "documento"
        },
        "Apache CouchDB": {
            "driver": "couchdb",
            "puerto_default": 5984,
            "tipo": "documento"
        },
        "Cosmos DB": {
            "driver": "cosmosdb",
            "puerto_default": 443,
            "tipo": "documento"
        },
        "MarkLogic": {
            "driver": "marklogic",
            "puerto_default": 8000,
            "tipo": "documento"
        },
        
        # NoSQL - Búsqueda
        "Elasticsearch": {
            "driver": "elasticsearch",
            "puerto_default": 9200,
            "tipo": "busqueda"
        },
        
        # NoSQL - Clave-Valor
        "Redis": {
            "driver": "redis",
            "puerto_default": 6379,
            "tipo": "clave_valor"
        },
        "Amazon DynamoDB": {
            "driver": "dynamodb",
            "puerto_default": 8000,
            "tipo": "clave_valor"
        },
        "Aerospike": {
            "driver": "aerospike",
            "puerto_default": 3000,
            "tipo": "clave_valor"
        },
        "Cache": {
            "driver": "cache",
            "puerto_default": 1972,
            "tipo": "multimodelo"
        },
        
        # NoSQL - Grafos
        "Apache Cassandra": {
            "driver": "cassandra",
            "puerto_default": 9042,
            "tipo": "columnar"
        },
        "BoltDB": {
            "driver": "boltdb",
            "puerto_default": 0,
            "tipo": "embebido"
        },
        
        # Orientado a Objetos
        "ZODB": {
            "driver": "zodb",
            "puerto_default": 0,
            "tipo": "objetos"
        },
        "ObjectDB": {
            "driver": "objectdb",
            "puerto_default": 6136,
            "tipo": "objetos"
        },
        "db4o": {
            "driver": "db4o",
            "puerto_default": 0,
            "tipo": "objetos"
        },
        
        # NewSQL
        "Nuob DB": {
            "driver": "nuodb",
            "puerto_default": 48004,
            "tipo": "newsql"
        }
    }
    
    def __init__(self, motor_origen: str, motor_destino: str):
        self.motor_origen = motor_origen
        self.motor_destino = motor_destino
        self.info_origen = self.BASES_SOPORTADAS.get(motor_origen)
        self.info_destino = self.BASES_SOPORTADAS.get(motor_destino)
        
        if not self.info_origen:
            raise ValueError(f"Base de datos origen '{motor_origen}' no soportada. Opciones: {list(self.BASES_SOPORTADAS.keys())}")
        if not self.info_destino:
            raise ValueError(f"Base de datos destino '{motor_destino}' no soportada. Opciones: {list(self.BASES_SOPORTADAS.keys())}")
    
    def obtener_driver_origen(self) -> str:
        return self.info_origen["driver"]
    
    def obtener_driver_destino(self) -> str:
        return self.info_destino["driver"]
    
    def es_migracion_heterogenea(self) -> bool:
        """Determina si la migración es entre diferentes tipos de BD"""
        return self.info_origen["tipo"] != self.info_destino["tipo"]
    
    def obtener_tipo_migracion(self) -> str:
        """Retorna el tipo de migración"""
        if self.motor_origen == self.motor_destino:
            return "homogenea"
        if self.info_origen["tipo"] == self.info_destino["tipo"]:
            return "heterogenea_mismo_tipo"
        return "heterogenea_cross_tipo"
    
    def generar_configuracion_conexion(self, tipo: str = "origen") -> Dict[str, Any]:
        """Genera configuración de conexión según el tipo de BD"""
        info = self.info_origen if tipo == "origen" else self.info_destino
        
        return {
            "motor": self.motor_origen if tipo == "origen" else self.motor_destino,
            "driver": info["driver"],
            "tipo": info["tipo"],
            "puerto_default": info["puerto_default"]
        }
    
    @classmethod
    def listar_bases_soportadas(cls):
        """Lista todas las bases de datos soportadas con su información"""
        print("=" * 80)
        print(f"{'BASE DE DATOS':<25} {'TIPO':<15} {'DRIVER':<30}")
        print("=" * 80)
        for nombre, info in cls.BASES_SOPORTADAS.items():
            print(f"{nombre:<25} {info['tipo']:<15} {info['driver']:<30}")
        print("=" * 80)
    
    @classmethod
    def migraciones_posibles(cls):
        """Muestra ejemplos de migraciones posibles"""
        tipos_unicos = set(info["tipo"] for info in cls.BASES_SOPORTADAS.values())
        
        print("\nTIPOS DE BASES DE DATOS SOPORTADAS:")
        for tipo in tipos_unicos:
            bases = [nombre for nombre, info in cls.BASES_SOPORTADAS.items() if info["tipo"] == tipo]
            print(f"  {tipo.upper()}: {', '.join(bases[:3])}... ({len(bases)} bases)")
        
        print("\nEJEMPLOS DE MIGRACIONES:")
        print("  1. Homogénea: PostgreSQL → PostgreSQL (mismo motor)")
        print("  2. Heterogénea mismo tipo: PostgreSQL → MySQL (relacional a relacional)")
        print("  3. Heterogénea cross-tipo: MongoDB → PostgreSQL (documento a relacional)")
        print("  4. Heterogénea cross-tipo: MySQL → Elasticsearch (relacional a búsqueda)")
        print("  5. Objetos a relacional: ZODB → PostgreSQL")

# Prueba rápida
if __name__ == "__main__":
    # Mostrar información del sistema
    ConectorMultiBase.listar_bases_soportadas()
    ConectorMultiBase.migraciones_posibles()
    
    # Ejemplo de migración MongoDB a PostgreSQL
    print("\n" + "=" * 80)
    print("EJEMPLO: MongoDB → PostgreSQL")
    print("=" * 80)
    migrador = ConectorMultiBase("MongoDB", "PostgreSQL")
    print(f"Tipo de migración: {migrador.obtener_tipo_migracion()}")
    print(f"Es heterogénea: {migrador.es_migracion_heterogenea()}")
    print(f"Driver origen: {migrador.obtener_driver_origen()}")
    print(f"Driver destino: {migrador.obtener_driver_destino()}")