"""Issue #10: Utilidades de validación y sanitización de datos."""

import re
from typing import Any, Optional, Union, Tuple, List
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
import unicodedata

class ValidadorDatos:
    """Clase utilitaria para validación de datos."""
    
    @staticmethod
    def validar_email(email: str) -> Tuple[bool, Optional[str]]:
        """Valida el formato de email."""
        patron = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if re.match(patron, email):
            return True, email.lower()
        return False, None
    
    @staticmethod
    def validar_telefono(telefono: str, longitud_min: int = 8, longitud_max: int = 15) -> Tuple[bool, Optional[str]]:
        """Valida y limpia número de teléfono."""
        # Eliminar caracteres no numéricos
        numeros = re.sub(r'\D', '', telefono)
        if longitud_min <= len(numeros) <= longitud_max:
            return True, numeros
        return False, None
    
    @staticmethod
    def validar_fecha(fecha_str: str, formatos: list = None) -> Tuple[bool, Optional[date]]:
        """Valida cadena de fecha contra múltiples formatos."""
        if formatos is None:
            formatos = ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y%m%d', '%d-%m-%Y']
        
        for fmt in formatos:
            try:
                parseado = datetime.strptime(fecha_str, fmt).date()
                return True, parseado
            except (ValueError, TypeError):
                continue
        
        return False, None
    
    @staticmethod
    def validar_decimal(valor: Any, min_val: float = None, max_val: float = None) -> Tuple[bool, Optional[Decimal]]:
        """Valida valores decimales/númericos."""
        try:
            decimal_val = Decimal(str(valor))
            
            if min_val is not None and decimal_val < Decimal(str(min_val)):
                return False, None
            
            if max_val is not None and decimal_val > Decimal(str(max_val)):
                return False, None
            
            return True, decimal_val
        except (InvalidOperation, ValueError, TypeError):
            return False, None
    
    @staticmethod
    def validar_entero(valor: Any, min_val: int = None, max_val: int = None) -> Tuple[bool, Optional[int]]:
        """Valida valores enteros."""
        try:
            entero_val = int(float(valor))
            
            if min_val is not None and entero_val < min_val:
                return False, None
            
            if max_val is not None and entero_val > max_val:
                return False, None
            
            return True, entero_val
        except (ValueError, TypeError):
            return False, None
    
    @staticmethod
    def validar_codigo(codigo: str, patron: str = r'^[A-Za-z0-9_-]+$') -> Tuple[bool, Optional[str]]:
        """Valida formato de código."""
        if re.match(patron, codigo):
            return True, codigo.upper()
        return False, None

class SanitizadorDatos:
    """Clase utilitaria para sanitización de datos."""
    
    @staticmethod
    def sanitizar_texto(valor: Any, longitud_max: int = None, permitir_vacio: bool = False) -> str:
        """Sanitiza texto eliminando caracteres no deseados."""
        if valor is None:
            return "" if permitir_vacio else None
        
        if not isinstance(valor, str):
            valor = str(valor)
        
        # Eliminar caracteres de control
        sanitizado = ''.join(c for c in valor if ord(c) >= 32 or c == '\n')
        
        # Eliminar espacios en blanco extra
        sanitizado = ' '.join(sanitizado.split())
        
        # Recortar a longitud máxima si se especifica
        if longitud_max and len(sanitizado) > longitud_max:
            sanitizado = sanitizado[:longitud_max]
        
        return sanitizado.strip() if sanitizado else ("" if permitir_vacio else None)
    
    @staticmethod
    def sanitizar_html(texto: str) -> str:
        """Elimina etiquetas HTML de un texto."""
        if not texto:
            return texto
        
        import html
        # Desescapar entidades HTML
        texto = html.unescape(texto)
        # Eliminar etiquetas HTML
        limpio = re.compile('<.*?>')
        return re.sub(limpio, '', texto)
    
    @staticmethod
    def normalizar_unicode(texto: str) -> str:
        """Normaliza caracteres unicode a ASCII."""
        if not texto:
            return texto
        
        normalizado = unicodedata.normalize('NFKD', texto)
        return normalizado.encode('ASCII', 'ignore').decode('ASCII')
    
    @staticmethod
    def sanitizar_para_sql(valor: Any) -> str:
        """Sanitiza valor para SQL (escape básico)."""
        if valor is None:
            return 'NULL'
        
        if isinstance(valor, str):
            # Escapar comillas simples
            escapado = valor.replace("'", "''")
            return f"'{escapado}'"
        elif isinstance(valor, bool):
            return 'TRUE' if valor else 'FALSE'
        elif isinstance(valor, (int, float)):
            return str(valor)
        else:
            return f"'{str(valor)}'"
    
    @staticmethod
    def limpiar_nulos(fila: dict, reemplazo: Any = None) -> dict:
        """Reemplaza valores None en un diccionario."""
        return {k: (v if v is not None else reemplazo) for k, v in fila.items()}

class VerificadorIntegridad:
    """Verifica la integridad de los datos entre origen y destino."""
    
    @staticmethod
    def verificar_integridad_fila(fila_origen: dict, fila_destino: dict, campos_clave: list) -> dict:
        """Verifica la integridad de una fila individual."""
        problemas = []
        
        for campo in campos_clave:
            valor_origen = fila_origen.get(campo)
            valor_destino = fila_destino.get(campo)
            
            # Normalizar para comparación
            if isinstance(valor_origen, Decimal):
                valor_origen = float(valor_origen)
            if isinstance(valor_destino, Decimal):
                valor_destino = float(valor_destino)
            
            if valor_origen != valor_destino:
                problemas.append({
                    'campo': campo,
                    'valor_origen': valor_origen,
                    'valor_destino': valor_destino
                })
        
        return {
            'tiene_problemas': len(problemas) > 0,
            'problemas': problemas
        }
    
    @staticmethod
    def calcular_checksum(fila: dict, campos: list = None) -> str:
        """Calcula checksum MD5 de los datos de la fila."""
        import hashlib
        import json
        
        if campos:
            datos = {k: fila.get(k) for k in campos if k in fila}
        else:
            datos = fila
        
        # Convertir a JSON con claves ordenadas para consistencia
        def json_serializable(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, date):
                return obj.isoformat()
            raise TypeError(f"Tipo no serializable: {type(obj)}")
        
        json_str = json.dumps(datos, sort_keys=True, default=json_serializable)
        return hashlib.md5(json_str.encode()).hexdigest()
    
    @staticmethod
    def comparar_conjuntos(conjunto_origen: set, conjunto_destino: set) -> dict:
        """Compara dos conjuntos de IDs."""
        solo_origen = conjunto_origen - conjunto_destino
        solo_destino = conjunto_destino - conjunto_origen
        
        return {
            'coinciden': len(solo_origen) == 0 and len(solo_destino) == 0,
            'solo_origen': list(solo_origen),
            'solo_destino': list(solo_destino),
            'total_origen': len(conjunto_origen),
            'total_destino': len(conjunto_destino)
        }
