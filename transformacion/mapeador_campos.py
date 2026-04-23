from typing import Dict, Any, List, Callable
from transformacion.modelos_pydantic import ConfiguracionMapeo

class MapeadorCampos:
    def __init__(self):
        self.mapeos: List[ConfiguracionMapeo] = []
        self.transformaciones: Dict[str, Callable] = {
            'mayusculas': lambda x: x.upper() if x else '',
            'minusculas': lambda x: x.lower() if x else '',
            'titulo': lambda x: x.title() if x else '',
            'strip': lambda x: x.strip() if x else '',
            'entero': lambda x: int(x) if x is not None else 0,
            'flotante': lambda x: float(x) if x is not None else 0.0,
            'booleano': lambda x: bool(x),
            'texto': lambda x: str(x) if x is not None else ''
        }
    
    def agregar_mapeo(self, campo_origen: str, campo_destino: str, 
                      transformacion: str = None, valor_por_defecto: Any = None):
        mapeo = ConfiguracionMapeo(
            campo_origen=campo_origen,
            campo_destino=campo_destino,
            transformacion=transformacion,
            valor_por_defecto=valor_por_defecto
        )
        self.mapeos.append(mapeo)
    
    def aplicar_mapeo(self, datos_origen: Dict[str, Any]) -> Dict[str, Any]:
        datos_destino = {}
        
        for mapeo in self.mapeos:
            valor = datos_origen.get(mapeo.campo_origen, mapeo.valor_por_defecto)
            
            if mapeo.transformacion and mapeo.transformacion in self.transformaciones:
                try:
                    valor = self.transformaciones[mapeo.transformacion](valor)
                except Exception:
                    valor = mapeo.valor_por_defecto
            
            datos_destino[mapeo.campo_destino] = valor
        
        return datos_destino
    
    def agregar_transformacion_personalizada(self, nombre: str, funcion: Callable):
        self.transformaciones[nombre] = funcion