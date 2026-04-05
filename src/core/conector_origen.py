"""Issue #2: Configuración de conexión a base de datos origen con SQLAlchemy."""

from typing import Dict, Any, Optional
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
import structlog
import os
import yaml

logger = structlog.get_logger()

class ConectorOrigen:
    """
    Conector para base de datos origen.
    Soporta PostgreSQL, MySQL y SQL Server.
    """
    
    # Mapeo de tipos de BD a cadenas de conexión
    CADENAS_CONEXION = {
        'postgresql': 'postgresql+psycopg2://{usuario}:{contrasena}@{host}:{puerto}/{base_datos}',
        'mysql': 'mysql+pymysql://{usuario}:{contrasena}@{host}:{puerto}/{base_datos}',
        'mssql': 'mssql+pyodbc://{usuario}:{contrasena}@{host}:{puerto}/{base_datos}?driver=ODBC+Driver+17+for+SQL+Server'
    }
    
    def __init__(self, configuracion: Dict[str, Any]):
        """
        Inicializa conector origen con configuración dinámica.
        
        Args:
            configuracion: Diccionario con tipo, host, puerto, base_datos, usuario, contrasena
        """
        self.tipo = configuracion['tipo'].lower()
        self.host = configuracion['host']
        self.puerto = configuracion.get('puerto', self._puerto_por_defecto())
        self.base_datos = configuracion['base_datos']
        self.usuario = configuracion['usuario']
        self.contrasena = configuracion['contrasena']
        self.tamano_pool = configuracion.get('tamano_pool', 5)
        self.tiempo_espera = configuracion.get('tiempo_espera', 30)
        self.motor: Optional[Engine] = None
        
        self.logger = logger.bind(componente="ConectorOrigen", tipo=self.tipo)
    
    def _puerto_por_defecto(self) -> int:
        """Puerto por defecto según tipo de base de datos."""
        puertos = {'postgresql': 5432, 'mysql': 3306, 'mssql': 1433}
        return puertos.get(self.tipo, 5432)
    
    def _obtener_cadena_conexion(self) -> str:
        """Genera la cadena de conexión para SQLAlchemy."""
        if self.tipo not in self.CADENAS_CONEXION:
            raise ValueError(f"Base de datos no soportada: {self.tipo}. Soporta: {list(self.CADENAS_CONEXION.keys())}")
        
        return self.CADENAS_CONEXION[self.tipo].format(
            usuario=self.usuario,
            contrasena=self.contrasena,
            host=self.host,
            puerto=self.puerto,
            base_datos=self.base_datos
        )
    
    def conectar(self) -> 'ConectorOrigen':
        """Establece la conexión a la base de datos origen."""
        try:
            cadena = self._obtener_cadena_conexion()
            
            self.motor = create_engine(
                cadena,
                poolclass=QueuePool,
                pool_size=self.tamano_pool,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={'timeout': self.tiempo_espera}
            )
            
            # Probar conexión
            with self.motor.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info(
                f"Conectado a base de datos origen",
                tipo=self.tipo,
                host=self.host,
                base_datos=self.base_datos
            )
            return self
            
        except Exception as e:
            self.logger.error(f"Fallo al conectar a origen", error=str(e))
            raise
    
    @contextmanager
    def obtener_sesion(self):
        """Administrador de contexto para sesiones de base de datos."""
        if not self.motor:
            self.conectar()
        
        from sqlalchemy.orm import sessionmaker
        Sesion = sessionmaker(bind=self.motor)
        sesion = Sesion()
        try:
            yield sesion
            sesion.commit()
        except Exception as e:
            sesion.rollback()
            self.logger.error(f"Error en sesión: {str(e)}")
            raise
        finally:
            sesion.close()
    
    def ejecutar_consulta(self, consulta: str, params: Optional[Dict] = None):
        """Ejecuta una consulta SQL y retorna resultado."""
        with self.motor.connect() as conn:
            resultado = conn.execute(text(consulta), params or {})
            return resultado
    
    def obtener_tablas(self) -> list:
        """Obtiene lista de todas las tablas en la base de datos origen."""
        inspector = inspect(self.motor)
        return inspector.get_table_names()
    
    def obtener_esquema_tabla(self, nombre_tabla: str) -> Dict[str, Any]:
        """Obtiene el esquema completo de una tabla."""
        inspector = inspect(self.motor)
        return {
            'nombre': nombre_tabla,
            'columnas': inspector.get_columns(nombre_tabla),
            'llave_primaria': inspector.get_pk_constraint(nombre_tabla),
            'llaves_foraneas': inspector.get_foreign_keys(nombre_tabla),
            'indices': inspector.get_indexes(nombre_tabla)
        }
    
    def cerrar(self):
        """Cierra todas las conexiones."""
        if self.motor:
            self.motor.dispose()
            self.logger.info("Conexiones cerradas")
    
    def probar_conexion(self) -> bool:
        """Prueba si la conexión funciona correctamente."""
        try:
            with self.motor.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            self.logger.error(f"Prueba de conexión fallida: {str(e)}")
            return False