"""Issue #5: Definición de esquemas de transformación con Pydantic."""

from pydantic import BaseModel, Field, validator, ConfigDict
from typing import Optional, Any, Dict, List, Union
from datetime import datetime, date
from decimal import Decimal
import re

class ModeloBaseMigracion(BaseModel):
    """Modelo base con configuraciones comunes para todos los esquemas."""
    model_config = ConfigDict(extra='allow', arbitrary_types_allowed=True)
    
    creado_en: Optional[datetime] = None
    actualizado_en: Optional[datetime] = None
    
    @validator('creado_en', 'actualizado_en', pre=True)
    def parse_datetime(cls, v):
        """Parsea datetime desde múltiples formatos."""
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M:%S', '%d/%m/%Y']:
                try:
                    return datetime.strptime(v, fmt)
                except ValueError:
                    continue
        return None

class EsquemaOrigenGenerico(ModeloBaseMigracion):
    """Esquema genérico para datos de origen."""
    id: Optional[int] = None
    codigo: Optional[str] = Field(None, max_length=100)
    nombre: Optional[str] = Field(None, max_length=200)
    descripcion: Optional[str] = None
    estado: Optional[str] = Field(None, pattern='^(activo|inactivo|eliminado|pendiente)$')
    monto: Optional[Decimal] = Field(None, ge=0)
    cantidad: Optional[int] = Field(None, ge=0)
    fecha_creacion: Optional[date] = None
    metadata: Optional[Dict[str, Any]] = None
    
    @validator('codigo')
    def validar_codigo(cls, v):
        """Valida formato del código."""
        if v and not re.match(r'^[A-Za-z0-9_-]+$', v):
            raise ValueError('El código solo puede contener letras, números, guiones y guiones bajos')
        return v

class EsquemaDestinoGenerico(ModeloBaseMigracion):
    """Esquema genérico para datos de destino."""
    id: Optional[int] = None
    id_externo: Optional[str] = Field(None, max_length=100)
    codigo: Optional[str] = Field(None, max_length=100)
    nombre: Optional[str] = Field(None, max_length=200)
    descripcion_completa: Optional[str] = None
    esta_activo: Optional[bool] = True
    monto_total: Optional[Decimal] = Field(None, ge=0)
    cantidad_total: Optional[int] = Field(None, ge=0)
    timestamp_creacion: Optional[datetime] = None
    ultima_modificacion: Optional[datetime] = None
    origen_datos: Optional[str] = Field(None, max_length=50)
    
    @validator('id_externo')
    def validar_id_externo(cls, v):
        """Valida formato de ID externo."""
        if v and len(v) > 100:
            raise ValueError('ID externo demasiado largo (máximo 100 caracteres)')
        return v

class ReglaMapeo(BaseModel):
    """Define el mapeo entre campos origen y destino."""
    campo_origen: str
    campo_destino: str
    transformacion: Optional[str] = None
    valor_default: Optional[Any] = None
    requerido: bool = False
    descripcion: Optional[str] = None

class ResultadoValidacion(BaseModel):
    """Resultado de validación de datos."""
    es_valido: bool
    errores: List[str] = []
    advertencias: List[str] = []
    datos_transformados: Optional[Dict[str, Any]] = None

class ConfiguracionMigracion(BaseModel):
    """Configuración completa de una migración."""
    id_migracion: str
    origen_tipo: str
    destino_tipo: str
    tablas: List[str]
    tamano_lote: int = 10000
    estrategia_carga: str = Field(default='insert', pattern='^(insert|upsert|reemplazar)$')
    crear_tablas_destino: bool = True
    validar_post_migracion: bool = True