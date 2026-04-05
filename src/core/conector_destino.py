"""Issue #3: Configuración de conexión a base de datos destino con pool y timeouts."""

from typing import Dict, Any, Optional
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
import structlog

logger = structlog.get_logger()

class ConectorDestino:
    """
    Conector para base de datos destino.
    Soporta PostgreSQL, MySQL y SQL Server con pool de conexiones.
    """
    
    CADENAS_CONEXION = {
        'postgresql': 'postgresql+psycopg2://{usuario}:{contrasena}@{host}:{puerto}/{base_datos}',
        'mysql': 'mysql+pymysql://{usuario}:{contrasena}@{host}:{puerto}/{base_datos}',
        'mssql': 'mssql+pyodbc://{usuario}:{contrasena}@{host}:{puerto}/{base_datos}?driver=ODBC+Driver+17+for+SQL+Server'
    }
    
    # Mapeo de tipos de datos entre bases de datos
    MAPEO_TIPOS = {
        ('mysql', 'postgresql'): {
            'INT': 'INTEGER',
            'TINYINT(1)': 'BOOLEAN',
            'DATETIME': 'TIMESTAMP',
            'LONGTEXT': 'TEXT',
            'VARCHAR': 'VARCHAR'
        },
        ('mssql', 'postgresql'): {
            'NVARCHAR': 'VARCHAR',
            'DATETIME': 'TIMESTAMP',
            'MONEY': 'NUMERIC(19,4)',
            'BIT': 'BOOLEAN'
        },
        ('postgresql', 'mysql'): {
            'BOOLEAN': 'TINYINT(1)',
            'TIMESTAMP': 'DATETIME',
            'UUID': 'VARCHAR(36)'
        }
    }
    
    def __init__(self, configuracion: Dict[str, Any]):
        """
        Inicializa conector destino con configuración dinámica.
        
        Args:
            configuracion: Diccionario con tipo, host, puerto, base_datos, usuario, contrasena
        """
        self.tipo = configuracion['tipo'].lower()
        self.host = configuracion['host']
        self.puerto = configuracion.get('puerto', self._puerto_por_defecto())
        self.base_datos = configuracion['base_datos']
        self.usuario = configuracion['usuario']
        self.contrasena = configuracion['contrasena']
        self.tamano_pool = configuracion.get('tamano_pool', 10)
        self.tiempo_espera = configuracion.get('tiempo_espera', 30)
        self.motor: Optional[Engine] = None
        
        self.logger = logger.bind(componente="ConectorDestino", tipo=self.tipo)
    
    def _puerto_por_defecto(self) -> int:
        """Puerto por defecto según tipo de base de datos."""
        puertos = {'postgresql': 5432, 'mysql': 3306, 'mssql': 1433}
        return puertos.get(self.tipo, 5432)
    
    def _obtener_cadena_conexion(self) -> str:
        """Genera la cadena de conexión para SQLAlchemy."""
        if self.tipo not in self.CADENAS_CONEXION:
            raise ValueError(f"Base de datos no soportada: {self.tipo}")
        
        return self.CADENAS_CONEXION[self.tipo].format(
            usuario=self.usuario,
            contrasena=self.contrasena,
            host=self.host,
            puerto=self.puerto,
            base_datos=self.base_datos
        )
    
    def conectar(self) -> 'ConectorDestino':
        """Establece la conexión a la base de datos destino con pooling."""
        try:
            cadena = self._obtener_cadena_conexion()
            
            self.motor = create_engine(
                cadena,
                poolclass=QueuePool,
                pool_size=self.tamano_pool,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={'timeout': self.tiempo_espera}
            )
            
            # Probar conexión
            with self.motor.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.logger.info(
                f"Conectado a base de datos destino",
                tipo=self.tipo,
                host=self.host,
                base_datos=self.base_datos,
                tamano_pool=self.tamano_pool
            )
            return self
            
        except Exception as e:
            self.logger.error(f"Fallo al conectar a destino", error=str(e))
            raise
    
    @contextmanager
    def obtener_sesion(self):
        """Administrador de contexto para sesiones."""
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
        """Ejecuta una consulta SQL."""
        with self.motor.connect() as conn:
            resultado = conn.execute(text(consulta), params or {})
            if resultado.returns_rows:
                return resultado
            conn.commit()
            return None
    
    def ejecutar_muchos(self, consulta: str, datos: list):
        """Ejecuta consulta con executemany para inserción masiva."""
        with self.motor.connect() as conn:
            conn.execute(text(consulta), datos)
            conn.commit()
    
    def convertir_tipo_para_destino(self, tipo_origen: str) -> str:
        """Convierte un tipo de datos de origen al equivalente en destino."""
        clave = (self.tipo, 'destino')  # Simplificado, en producción se usa el tipo real
        tipo_base = tipo_origen.upper().split('(')[0]
        
        # Buscar en mapeo específico
        for (origen, destino), mapeo in self.MAPEO_TIPOS.items():
            if destino == self.tipo and tipo_base in mapeo:
                return mapeo[tipo_base]
        
        # Mapeos por defecto
        if tipo_base in ['INTEGER', 'INT', 'BIGINT']:
            return 'INTEGER' if self.tipo == 'postgresql' else 'INT'
        elif tipo_base in ['VARCHAR', 'CHAR', 'TEXT']:
            return 'VARCHAR(255)' if self.tipo == 'mysql' else 'VARCHAR'
        elif tipo_base in ['BOOLEAN', 'BOOL']:
            return 'BOOLEAN' if self.tipo == 'postgresql' else 'TINYINT(1)'
        elif tipo_base in ['TIMESTAMP', 'DATETIME']:
            return 'TIMESTAMP' if self.tipo == 'postgresql' else 'DATETIME'
        
        return 'TEXT'
    
    def cerrar(self):
        """Cierra todas las conexiones."""
        if self.motor:
            self.motor.dispose()
            self.logger.info("Conexiones de destino cerradas")