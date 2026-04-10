"""Issue #13: Reglas de negocio personalizables para transformaciones."""

from typing import Dict, Any, Callable, Optional, List
from dataclasses import dataclass, field
from datetime import datetime, date
from decimal import Decimal
import structlog
import importlib.util
import sys
from pathlib import Path

logger = structlog.get_logger()

@dataclass
class ReglaNegocio:
    """Define una regla de negocio para transformación."""
    nombre: str
    funcion: Callable
    descripcion: str = ""
    prioridad: int = 1
    habilitada: bool = True
    
    def aplicar(self, fila: Dict[str, Any]) -> Dict[str, Any]:
        """Aplica la regla de negocio a una fila."""
        if not self.habilitada:
            return fila
        
        try:
            return self.funcion(fila)
        except Exception as e:
            logger.error(f"Regla de negocio {self.nombre} falló", error=str(e), fila=fila)
            fila[f'_error_regla_{self.nombre}'] = str(e)
            return fila

class MotorReglasNegocio:
    """Motor para aplicar reglas de negocio personalizadas."""
    
    def __init__(self):
        self.reglas: Dict[str, ReglaNegocio] = {}
        self.orden_ejecucion: List[str] = []
        self.logger = logger.bind(componente="MotorReglasNegocio")
    
    def registrar_regla(self, regla: ReglaNegocio):
        """Registra una regla de negocio."""
        self.reglas[regla.nombre] = regla
        self._actualizar_orden()
        self.logger.info(f"Regla de negocio registrada", nombre_regla=regla.nombre, prioridad=regla.prioridad)
    
    def registrar_regla_desde_funcion(self, nombre: str, funcion: Callable, descripcion: str = "", prioridad: int = 1):
        """Registra una regla desde una función."""
        regla = ReglaNegocio(nombre=nombre, funcion=funcion, descripcion=descripcion, prioridad=prioridad)
        self.registrar_regla(regla)
    
    def cargar_reglas_desde_modulo(self, ruta_modulo: str):
        """Carga reglas de negocio desde un módulo Python externo."""
        ruta = Path(ruta_modulo)
        if not ruta.exists():
            self.logger.error(f"Módulo no encontrado", ruta=ruta_modulo)
            return
        
        nombre_modulo = ruta.stem
        especificacion = importlib.util.spec_from_file_location(nombre_modulo, ruta_modulo)
        modulo = importlib.util.module_from_spec(especificacion)
        sys.modules[nombre_modulo] = modulo
        especificacion.loader.exec_module(modulo)
        
        # Buscar funciones decoradas con @regla_negocio
        reglas_cargadas = 0
        for nombre_atributo in dir(modulo):
            atributo = getattr(modulo, nombre_atributo)
            if callable(atributo) and hasattr(atributo, '_es_regla_negocio'):
                self.registrar_regla_desde_funcion(
                    nombre=nombre_atributo,
                    funcion=atributo,
                    descripcion=getattr(atributo, '_descripcion_regla', ''),
                    prioridad=getattr(atributo, '_prioridad_regla', 1)
                )
                reglas_cargadas += 1
        
        self.logger.info(f"Reglas cargadas desde módulo", modulo=ruta_modulo, cantidad=reglas_cargadas)
    
    def _actualizar_orden(self):
        """Actualiza el orden de ejecución según prioridad."""
        self.orden_ejecucion = sorted(
            self.reglas.keys(),
            key=lambda x: self.reglas[x].prioridad
        )
    
    def aplicar_reglas(
        self,
        fila: Dict[str, Any],
        nombres_reglas: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Aplica las reglas registradas a una fila."""
        fila_resultado = fila.copy()
        
        if nombres_reglas:
            reglas_a_aplicar = [self.reglas[nombre] for nombre in nombres_reglas if nombre in self.reglas]
            reglas_a_aplicar.sort(key=lambda r: r.prioridad)
        else:
            reglas_a_aplicar = [self.reglas[nombre] for nombre in self.orden_ejecucion]
        
        for regla in reglas_a_aplicar:
            fila_resultado = regla.aplicar(fila_resultado)
        
        return fila_resultado
    
    def aplicar_reglas_a_lote(
        self,
        lote: List[Dict[str, Any]],
        nombres_reglas: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Aplica reglas a un lote completo de filas."""
        lote_transformado = []
        for fila in lote:
            lote_transformado.append(self.aplicar_reglas(fila, nombres_reglas))
        return lote_transformado
    
    def deshabilitar_regla(self, nombre: str):
        """Deshabilita una regla temporalmente."""
        if nombre in self.reglas:
            self.reglas[nombre].habilitada = False
            self.logger.info(f"Regla deshabilitada", nombre_regla=nombre)
    
    def habilitar_regla(self, nombre: str):
        """Habilita una regla."""
        if nombre in self.reglas:
            self.reglas[nombre].habilitada = True
            self.logger.info(f"Regla habilitada", nombre_regla=nombre)
    
    def eliminar_regla(self, nombre: str):
        """Elimina una regla del motor."""
        if nombre in self.reglas:
            del self.reglas[nombre]
            self._actualizar_orden()
            self.logger.info(f"Regla eliminada", nombre_regla=nombre)
    
    def obtener_reglas(self) -> List[Dict[str, Any]]:
        """Obtiene lista de todas las reglas registradas."""
        return [
            {
                'nombre': r.nombre,
                'descripcion': r.descripcion,
                'prioridad': r.prioridad,
                'habilitada': r.habilitada
            }
            for r in self.reglas.values()
        ]

# Decorador para marcar reglas de negocio
def regla_negocio(descripcion: str = "", prioridad: int = 1):
    """Decorador para marcar una función como regla de negocio."""
    def decorador(func):
        func._es_regla_negocio = True
        func._descripcion_regla = descripcion
        func._prioridad_regla = prioridad
        return func
    return decorador

# ============================================================
# REGLAS DE NEGOCIO DE EJEMPLO
# ============================================================

class ReglasNegocioComunes:
    """Colección de reglas de negocio comunes."""
    
    @staticmethod
    @regla_negocio("Calcular impuesto basado en subtotal (IVA 16%)", prioridad=1)
    def calcular_impuesto(fila: Dict) -> Dict:
        """Calcula impuesto IVA del 16%."""
        subtotal = fila.get('subtotal', 0)
        if isinstance(subtotal, str):
            try:
                subtotal = float(subtotal)
            except:
                subtotal = 0
        
        fila['monto_impuesto'] = round(subtotal * 0.16, 2)
        fila['total'] = round(subtotal + fila['monto_impuesto'], 2)
        return fila
    
    @staticmethod
    @regla_negocio("Aplicar descuento por cantidad", prioridad=2)
    def aplicar_descuento_cantidad(fila: Dict) -> Dict:
        """Aplica descuento del 10% si cantidad > 100."""
        cantidad = fila.get('cantidad', 0)
        if isinstance(cantidad, str):
            try:
                cantidad = int(cantidad)
            except:
                cantidad = 0
        
        if cantidad > 100:
            precio = fila.get('precio', 0)
            if isinstance(precio, (int, float)):
                fila['precio_con_descuento'] = round(precio * 0.9, 2)
        
        return fila
    
    @staticmethod
    @regla_negocio("Formatear números de teléfono", prioridad=3)
    def formatear_telefono(fila: Dict) -> Dict:
        """Formatea número de teléfono a formato internacional."""
        telefono = fila.get('telefono')
        if telefono:
            # Limpiar y formatear
            numeros = ''.join(filter(str.isdigit, str(telefono)))
            if len(numeros) == 10:
                fila['telefono_formateado'] = f"+1 ({numeros[:3]}) {numeros[3:6]}-{numeros[6:]}"
            elif len(numeros) > 10:
                fila['telefono_formateado'] = f"+{numeros}"
            else:
                fila['telefono_formateado'] = telefono
        return fila
    
    @staticmethod
    @regla_negocio("Establecer estado por antigüedad", prioridad=4)
    def establecer_estado_por_antiguedad(fila: Dict) -> Dict:
        """Establece estado basado en la antigüedad del registro."""
        fecha_creacion = fila.get('fecha_creacion')
        if fecha_creacion:
            if isinstance(fecha_creacion, str):
                try:
                    fecha_creacion = datetime.fromisoformat(fecha_creacion).date()
                except:
                    fecha_creacion = date.today()
            
            if isinstance(fecha_creacion, (datetime, date)):
                dias_antiguedad = (date.today() - fecha_creacion).days
                
                if dias_antiguedad > 365:
                    fila['estado'] = 'archivado'
                elif dias_antiguedad > 90:
                    fila['estado'] = 'antiguo'
                elif dias_antiguedad > 30:
                    fila['estado'] = 'activo'
                else:
                    fila['estado'] = 'nuevo'
        
        return fila
    
    @staticmethod
    @regla_negocio("Normalizar códigos de producto", prioridad=5)
    def normalizar_codigo_producto(fila: Dict) -> Dict:
        """Normaliza códigos de producto a mayúsculas sin espacios."""
        codigo = fila.get('codigo_producto') or fila.get('codigo')
        if codigo:
            normalizado = str(codigo).upper().strip().replace(' ', '_')
            # Reemplazar caracteres especiales
            import re
            normalizado = re.sub(r'[^A-Z0-9_-]', '', normalizado)
            fila['codigo_producto_normalizado'] = normalizado
        return fila
    
    @staticmethod
    @regla_negocio("Calcular edad desde fecha de nacimiento", prioridad=6)
    def calcular_edad(fila: Dict) -> Dict:
        """Calcula edad a partir de fecha de nacimiento."""
        fecha_nacimiento = fila.get('fecha_nacimiento')
        if fecha_nacimiento:
            if isinstance(fecha_nacimiento, str):
                try:
                    fecha_nacimiento = datetime.fromisoformat(fecha_nacimiento).date()
                except:
                    return fila
            
            if isinstance(fecha_nacimiento, date):
                hoy = date.today()
                edad = hoy.year - fecha_nacimiento.year
                if hoy.month < fecha_nacimiento.month or (hoy.month == fecha_nacimiento.month and hoy.day < fecha_nacimiento.day):
                    edad -= 1
                fila['edad'] = edad
        
        return fila

# Constructor de reglas para reglas dinámicas
class ConstructorReglas:
    """Construye reglas de negocio dinámicamente."""
    
    @staticmethod
    def crear_regla_condicional(
        nombre: str,
        condicion: Callable[[Dict], bool],
        accion: Callable[[Dict], Dict],
        descripcion: str = "",
        prioridad: int = 1
    ) -> ReglaNegocio:
        """Crea una regla de negocio condicional."""
        def aplicar_condicional(fila: Dict) -> Dict:
            if condicion(fila):
                return accion(fila)
            return fila
        
        return ReglaNegocio(
            nombre=nombre,
            funcion=aplicar_condicional,
            descripcion=descripcion,
            prioridad=prioridad
        )
    
    @staticmethod
    def crear_regla_mapeo_campo(
        nombre: str,
        campo_origen: str,
        campo_destino: str,
        transformador: Optional[Callable] = None,
        prioridad: int = 1
    ) -> ReglaNegocio:
        """Crea una regla para mapeo de campos."""
        def aplicar_mapeo(fila: Dict) -> Dict:
            if campo_origen in fila:
                valor = fila[campo_origen]
                if transformador:
                    valor = transformador(valor)
                fila[campo_destino] = valor
            return fila
        
        return ReglaNegocio(
            nombre=nombre,
            funcion=aplicar_mapeo,
            descripcion=f"Mapear {campo_origen} a {campo_destino}",
            prioridad=prioridad
        )
    
    @staticmethod
    def crear_regla_calculo(
        nombre: str,
        campo_resultado: str,
        expresion: Callable[[Dict], Any],
        descripcion: str = "",
        prioridad: int = 1
    ) -> ReglaNegocio:
        """Crea una regla para cálculos personalizados."""
        def aplicar_calculo(fila: Dict) -> Dict:
            try:
                fila[campo_resultado] = expresion(fila)
            except Exception as e:
                fila[f'_error_calculo_{campo_resultado}'] = str(e)
            return fila
        
        return ReglaNegocio(
            nombre=nombre,
            funcion=aplicar_calculo,
            descripcion=descripcion or f"Calcular {campo_resultado}",
            prioridad=prioridad
        )
