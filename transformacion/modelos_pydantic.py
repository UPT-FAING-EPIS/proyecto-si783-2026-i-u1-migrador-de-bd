from pydantic import BaseModel, Field, validator
from typing import Optional, Any, List
from datetime import datetime
from decimal import Decimal

class ModeloDatosOrigen(BaseModel):
    id: int
    nombre: str
    valor: float
    fecha_creacion: datetime
    activo: bool = True
    
    class Config:
        from_attributes = True

class ModeloDatosDestino(BaseModel):
    identificador: int
    nombre_completo: str
    monto_total: Decimal
    fecha_registro: datetime
    estado: str = "ACTIVO"
    
    class Config:
        from_attributes = True
    
    @validator('nombre_completo')
    def validar_nombre_no_vacio(cls, valor):
        if not valor or len(valor.strip()) == 0:
            raise ValueError('El nombre no puede estar vacío')
        return valor.strip()

class ConfiguracionMapeo(BaseModel):
    campo_origen: str
    campo_destino: str
    transformacion: Optional[str] = None
    valor_por_defecto: Optional[Any] = None
    obligatorio: bool = False