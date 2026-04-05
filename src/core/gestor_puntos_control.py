"""Issue #7: Sistema de checkpoint para guardar estado de migración."""

from typing import Dict, Any, Optional
from datetime import datetime
from sqlalchemy import text, Table, Column, Integer, String, DateTime, JSON, MetaData
from sqlalchemy.exc import SQLAlchemyError
import structlog
import json

logger = structlog.get_logger()

class GestorPuntosControl:
    """Gestiona los puntos de control de migración para operaciones reanudables."""
    
    def __init__(self, conector_destino, nombre_tabla: str = "puntos_control_migracion"):
        """
        Inicializa el gestor de puntos de control.
        
        Args:
            conector_destino: ConectorDestino para la base de datos destino
            nombre_tabla: Nombre de la tabla de puntos de control
        """
        self.conector = conector_destino
        self.nombre_tabla = nombre_tabla
        self.logger = logger.bind(componente="GestorPuntosControl")
        self._asegurar_tabla_puntos_control()
    
    def _asegurar_tabla_puntos_control(self):
        """Crea la tabla de puntos de control si no existe."""
        tipo_bd = self.conector.tipo
        
        if tipo_bd == 'postgresql':
            ddl = f"""
                CREATE TABLE IF NOT EXISTS {self.nombre_tabla} (
                    id SERIAL PRIMARY KEY,
                    id_migracion VARCHAR(100) NOT NULL,
                    nombre_tabla VARCHAR(200) NOT NULL,
                    ultimo_id_procesado INTEGER,
                    ultimo_timestamp TIMESTAMP,
                    numero_lote INTEGER,
                    total_procesado INTEGER,
                    estado VARCHAR(50),
                    metadata JSONB,
                    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    actualizado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(id_migracion, nombre_tabla)
                )
            """
        elif tipo_bd == 'mysql':
            ddl = f"""
                CREATE TABLE IF NOT EXISTS {self.nombre_tabla} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    id_migracion VARCHAR(100) NOT NULL,
                    nombre_tabla VARCHAR(200) NOT NULL,
                    ultimo_id_procesado INT,
                    ultimo_timestamp DATETIME,
                    numero_lote INT,
                    total_procesado INT,
                    estado VARCHAR(50),
                    metadata JSON,
                    creado_en DATETIME DEFAULT CURRENT_TIMESTAMP,
                    actualizado_en DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uk_migracion_tabla (id_migracion, nombre_tabla)
                )
            """
        else:  # mssql
            ddl = f"""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{self.nombre_tabla}' AND xtype='U')
                CREATE TABLE {self.nombre_tabla} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    id_migracion VARCHAR(100) NOT NULL,
                    nombre_tabla VARCHAR(200) NOT NULL,
                    ultimo_id_procesado INT,
                    ultimo_timestamp DATETIME,
                    numero_lote INT,
                    total_procesado INT,
                    estado VARCHAR(50),
                    metadata NVARCHAR(MAX),
                    creado_en DATETIME DEFAULT GETDATE(),
                    actualizado_en DATETIME DEFAULT GETDATE(),
                    CONSTRAINT UQ_migracion_tabla UNIQUE(id_migracion, nombre_tabla)
                )
            """
        
        try:
            self.conector.ejecutar_consulta(ddl)
            self.logger.info(f"Tabla de puntos de control asegurada", tabla=self.nombre_tabla)
        except Exception as e:
            self.logger.warning(f"La tabla de puntos de control puede ya existir", error=str(e))
    
    def guardar_punto_control(
        self,
        id_migracion: str,
        nombre_tabla: str,
        ultimo_id_procesado: Optional[int] = None,
        ultimo_timestamp: Optional[datetime] = None,
        numero_lote: int = 0,
        total_procesado: int = 0,
        estado: str = "en_progreso",
        metadata_adicional: Optional[Dict] = None
    ):
        """Guarda un punto de control de migración."""
        tipo_bd = self.conector.tipo
        
        if tipo_bd == 'postgresql':
            consulta = f"""
                INSERT INTO {self.nombre_tabla} 
                (id_migracion, nombre_tabla, ultimo_id_procesado, ultimo_timestamp, 
                 numero_lote, total_procesado, estado, metadata, actualizado_en)
                VALUES 
                (:id_migracion, :nombre_tabla, :ultimo_id_procesado, :ultimo_timestamp,
                 :numero_lote, :total_procesado, :estado, :metadata, CURRENT_TIMESTAMP)
                ON CONFLICT (id_migracion, nombre_tabla) 
                DO UPDATE SET
                    ultimo_id_procesado = EXCLUDED.ultimo_id_procesado,
                    ultimo_timestamp = EXCLUDED.ultimo_timestamp,
                    numero_lote = EXCLUDED.numero_lote,
                    total_procesado = EXCLUDED.total_procesado,
                    estado = EXCLUDED.estado,
                    metadata = EXCLUDED.metadata,
                    actualizado_en = CURRENT_TIMESTAMP
            """
        else:
            # MySQL y SQL Server usan INSERT ... ON DUPLICATE KEY UPDATE
            consulta = f"""
                INSERT INTO {self.nombre_tabla} 
                (id_migracion, nombre_tabla, ultimo_id_procesado, ultimo_timestamp, 
                 numero_lote, total_procesado, estado, metadata, actualizado_en)
                VALUES 
                (:id_migracion, :nombre_tabla, :ultimo_id_procesado, :ultimo_timestamp,
                 :numero_lote, :total_procesado, :estado, :metadata, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    ultimo_id_procesado = VALUES(ultimo_id_procesado),
                    ultimo_timestamp = VALUES(ultimo_timestamp),
                    numero_lote = VALUES(numero_lote),
                    total_procesado = VALUES(total_procesado),
                    estado = VALUES(estado),
                    metadata = VALUES(metadata),
                    actualizado_en = CURRENT_TIMESTAMP
            """
        
        try:
            self.conector.ejecutar_consulta(
                consulta,
                {
                    "id_migracion": id_migracion,
                    "nombre_tabla": nombre_tabla,
                    "ultimo_id_procesado": ultimo_id_procesado,
                    "ultimo_timestamp": ultimo_timestamp,
                    "numero_lote": numero_lote,
                    "total_procesado": total_procesado,
                    "estado": estado,
                    "metadata": json.dumps(metadata_adicional) if metadata_adicional else None
                }
            )
            self.logger.info(
                f"Punto de control guardado",
                id_migracion=id_migracion,
                tabla=nombre_tabla,
                lote=numero_lote,
                procesado=total_procesado
            )
        except Exception as e:
            self.logger.error(f"Error al guardar punto de control", error=str(e))
            raise
    
    def obtener_punto_control(
        self,
        id_migracion: str,
        nombre_tabla: str
    ) -> Optional[Dict[str, Any]]:
        """Recupera el último punto de control para una tabla."""
        consulta = f"""
            SELECT * FROM {self.nombre_tabla}
            WHERE id_migracion = :id_migracion AND nombre_tabla = :nombre_tabla
        """
        
        try:
            resultado = self.conector.ejecutar_consulta(
                consulta,
                {"id_migracion": id_migracion, "nombre_tabla": nombre_tabla}
            )
            fila = resultado.fetchone()
            
            if fila:
                punto_control = dict(fila._mapping)
                if punto_control.get('metadata'):
                    punto_control['metadata'] = json.loads(punto_control['metadata'])
                return punto_control
            
            return None
        except Exception as e:
            self.logger.error(f"Error al obtener punto de control", error=str(e))
            return None
    
    def obtener_todos_puntos_control(self, id_migracion: str) -> Dict[str, Dict]:
        """Obtiene todos los puntos de control para una migración."""
        consulta = f"""
            SELECT * FROM {self.nombre_tabla}
            WHERE id_migracion = :id_migracion
        """
        
        resultado = self.conector.ejecutar_consulta(consulta, {"id_migracion": id_migracion})
        puntos_control = {}
        for fila in resultado.fetchall():
            pc = dict(fila._mapping)
            if pc.get('metadata'):
                pc['metadata'] = json.loads(pc['metadata'])
            puntos_control[pc['nombre_tabla']] = pc
        
        return puntos_control
    
    def marcar_completado(self, id_migracion: str, nombre_tabla: str, total_procesado: int):
        """Marca una tabla como completamente migrada."""
        self.guardar_punto_control(
            id_migracion=id_migracion,
            nombre_tabla=nombre_tabla,
            estado='completado',
            total_procesado=total_procesado
        )