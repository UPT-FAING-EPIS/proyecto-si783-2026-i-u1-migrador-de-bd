import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

class PuntoControl:
    def __init__(self, archivo_estado: str = "punto_control.json"):
        self.archivo_estado = Path(archivo_estado)
        self.estado = self._cargar_estado()
    
    def _cargar_estado(self) -> Dict[str, Any]:
        if self.archivo_estado.exists():
            with open(self.archivo_estado, 'r') as archivo:
                return json.load(archivo)
        return {
            "ultimo_id": 0,
            "ultimo_timestamp": None,
            "lote_actual": 0,
            "estado": "no_iniciado"
        }
    
    def guardar_progreso(self, ultimo_id: int, lote_actual: int):
        self.estado.update({
            "ultimo_id": ultimo_id,
            "ultimo_timestamp": datetime.now().isoformat(),
            "lote_actual": lote_actual,
            "estado": "en_progreso"
        })
        
        with open(self.archivo_estado, 'w') as archivo:
            json.dump(self.estado, archivo, indent=2)
    
    def marcar_completado(self):
        self.estado["estado"] = "completado"
        self.estado["fecha_completado"] = datetime.now().isoformat()
        
        with open(self.archivo_estado, 'w') as archivo:
            json.dump(self.estado, archivo, indent=2)
    
    def obtener_ultimo_punto(self) -> Optional[int]:
        return self.estado.get("ultimo_id")
    
    def debe_reanudar(self) -> bool:
        return self.estado.get("estado") == "en_progreso"