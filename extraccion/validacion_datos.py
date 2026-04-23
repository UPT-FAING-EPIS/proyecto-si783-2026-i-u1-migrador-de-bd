from typing import Any, List, Dict
import re
from datetime import datetime

class ValidadorDatos:
    @staticmethod
    def validar_tipo(valor: Any, tipo_esperado: type) -> bool:
        try:
            if tipo_esperado == datetime:
                datetime.fromisoformat(str(valor))
                return True
            return isinstance(valor, tipo_esperado)
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def sanitizar_texto(texto: str, longitud_maxima: int = 1000) -> str:
        if not isinstance(texto, str):
            return str(texto)[:longitud_maxima]
        
        texto = texto.strip()
        texto = re.sub(r'[<>]', '', texto)
        texto = texto.replace("'", "''")
        
        return texto[:longitud_maxima]
    
    @staticmethod
    def verificar_integridad_fila(fila: Dict[str, Any], campos_obligatorios: List[str]) -> List[str]:
        campos_faltantes = []
        for campo in campos_obligatorios:
            if campo not in fila or fila[campo] is None:
                campos_faltantes.append(campo)
        return campos_faltantes