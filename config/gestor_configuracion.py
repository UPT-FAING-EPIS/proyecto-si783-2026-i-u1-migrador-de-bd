import os
import yaml
from pathlib import Path
from typing import Dict, Any

class GestorConfiguracion:
    def __init__(self, ruta_config: str = None):
        if ruta_config is None:
            ruta_actual = Path(__file__).parent
            ruta_config = ruta_actual / "configuracion.yaml"
        
        self.ruta_config = Path(ruta_config)
        self.configuracion = self._cargar_configuracion()
        self._reemplazar_variables_entorno()
    
    def _cargar_configuracion(self) -> Dict[str, Any]:
        with open(self.ruta_config, 'r', encoding='utf-8') as archivo:
            return yaml.safe_load(archivo)
    
    def _reemplazar_variables_entorno(self):
        def reemplazar_recursivo(objeto):
            if isinstance(objeto, dict):
                for clave, valor in objeto.items():
                    if isinstance(valor, str) and valor.startswith('${') and valor.endswith('}'):
                        variable = valor[2:-1]
                        objeto[clave] = os.getenv(variable, '')
                    elif isinstance(valor, (dict, list)):
                        reemplazar_recursivo(valor)
            elif isinstance(objeto, list):
                for item in objeto:
                    reemplazar_recursivo(item)
        
        reemplazar_recursivo(self.configuracion)
    
    def obtener_configuracion_origen(self) -> Dict[str, Any]:
        return self.configuracion.get('configuracion_conexion', {}).get('origen', {})
    
    def obtener_configuracion_destino(self) -> Dict[str, Any]:
        return self.configuracion.get('configuracion_conexion', {}).get('destino', {})
    
    def obtener_configuracion_extraccion(self) -> Dict[str, Any]:
        return self.configuracion.get('configuracion_extraccion', {})
    
    def obtener_configuracion_carga(self) -> Dict[str, Any]:
        return self.configuracion.get('configuracion_carga', {})