from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager
from typing import Any, Generator
import time
import random

from utilidades.registro_logging import ConfiguradorLogging

class ConectorBaseDatos:
    def __init__(self, config_conexion: dict):
        self.config = config_conexion
        self.logger = ConfiguradorLogging.obtener_logger(__name__)
        self.motor_sqlalchemy = None
        self._inicializar_conexion()
    
    def _construir_url_conexion(self) -> str:
        motor = self.config.get('motor', 'PostgreSQL').lower()
        usuario = self.config.get('usuario', '')
        contrasena = self.config.get('contrasena', '')
        host = self.config.get('host', 'localhost')
        puerto = self.config.get('puerto', '')
        nombre_bd = self.config.get('nombre_bd', '')
        
        # Mapeo de motores a drivers SQLAlchemy
        mapeo_motores = {
            'postgresql': 'postgresql+psycopg2',
            'mysql': 'mysql+mysqlconnector',
            'microsoft sql server': 'mssql+pymssql',
            'oracle': 'oracle+cx_oracle',
            'sqlite': 'sqlite',
            'mariadb': 'mysql+mysqlconnector'
        }
        
        driver = mapeo_motores.get(motor, motor)
        
        # Construir URL según el tipo
        if motor in ['sqlite']:
            return f"sqlite:///{nombre_bd or 'default.db'}"
        elif motor in ['microsoft sql server']:
            return f"mssql+pymssql://{usuario}:{contrasena}@{host}:{puerto}/{nombre_bd}"
        else:
            return f"{driver}://{usuario}:{contrasena}@{host}:{puerto}/{nombre_bd}"
    
    def _inicializar_conexion(self):
        url_conexion = self._construir_url_conexion()
        
        # Argumentos de conexión según el motor
        connect_args = {}
        motor = self.config.get('motor', '').lower()
        
        if motor not in ['sqlite']:
            connect_args['connect_timeout'] = self.config.get('timeout_conexion', 30)
        
        self.motor_sqlalchemy = create_engine(
            url_conexion,
            connect_args=connect_args if connect_args else {}
        )
        
        self.logger.info("conexion_inicializada", 
                        motor=self.config.get('motor'),
                        host=self.config.get('host', 'localhost'))
    
    @contextmanager
    def obtener_conexion(self) -> Generator:
        try:
            conexion = self.motor_sqlalchemy.connect()
            yield conexion
        except Exception as error:
            self.logger.error("error_conexion", error=str(error))
            raise
        finally:
            if 'conexion' in locals():
                conexion.close()
    
    def ejecutar_consulta_con_reintentos(self, consulta: str, parametros: dict = None) -> Any:
        reintentos_maximos = 3
        
        for intento in range(reintentos_maximos):
            try:
                with self.obtener_conexion() as conexion:
                    resultado = conexion.execute(text(consulta), parametros or {})
                    return resultado
            except Exception as error:
                self.logger.warning(f"intento_{intento + 1}_fallido", error=str(error))
                if intento == reintentos_maximos - 1:
                    raise
                time.sleep((2 ** intento) + random.uniform(0, 1))