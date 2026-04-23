import unittest
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

# Agregar directorio raíz al PATH
sys.path.insert(0, str(Path(__file__).parent.parent))

from extraccion.validacion_datos import ValidadorDatos
from transformacion.mapeador_campos import MapeadorCampos
from transformacion.transformador_complejo import TransformadorComplejo
from utilidades.registro_logging import ConfiguradorLogging

class PruebaValidacionDatos(unittest.TestCase):
    def setUp(self):
        self.validador = ValidadorDatos()
        self.logger = ConfiguradorLogging.obtener_logger(__name__)
    
    def test_validar_tipo_entero(self):
        self.assertTrue(self.validador.validar_tipo(123, int))
        self.assertFalse(self.validador.validar_tipo("123", int))
    
    def test_validar_tipo_flotante(self):
        self.assertTrue(self.validador.validar_tipo(123.45, float))
        self.assertFalse(self.validador.validar_tipo("123.45", float))
    
    def test_validar_fecha(self):
        self.assertTrue(self.validador.validar_tipo("2024-01-01", datetime))
        self.assertFalse(self.validador.validar_tipo("fecha_invalida", datetime))
    
    def test_sanitizar_texto(self):
        texto_original = "<script>alert('test')</script>"
        texto_sanitizado = self.validador.sanitizar_texto(texto_original)
        self.assertNotIn('<script>', texto_sanitizado)
        self.assertNotIn('</script>', texto_sanitizado)
    
    def test_verificar_integridad_fila(self):
        fila = {"id": 1, "nombre": "test"}
        campos_obligatorios = ["id", "nombre", "email"]
        faltantes = self.validador.verificar_integridad_fila(fila, campos_obligatorios)
        self.assertEqual(faltantes, ["email"])

class PruebaMapeadorCampos(unittest.TestCase):
    def setUp(self):
        self.mapeador = MapeadorCampos()
        self.logger = ConfiguradorLogging.obtener_logger(__name__)
    
    def test_mapeo_simple(self):
        self.mapeador.agregar_mapeo("nombre", "nombre_completo")
        datos = {"nombre": "Juan Pérez"}
        resultado = self.mapeador.aplicar_mapeo(datos)
        self.assertEqual(resultado["nombre_completo"], "Juan Pérez")
    
    def test_mapeo_con_transformacion(self):
        self.mapeador.agregar_mapeo("nombre", "nombre_completo", transformacion="mayusculas")
        datos = {"nombre": "Juan Pérez"}
        resultado = self.mapeador.aplicar_mapeo(datos)
        self.assertEqual(resultado["nombre_completo"], "JUAN PÉREZ")
    
    def test_mapeo_con_valor_defecto(self):
        self.mapeador.agregar_mapeo("edad", "edad_usuario", valor_por_defecto=25)
        datos = {"nombre": "Test"}
        resultado = self.mapeador.aplicar_mapeo(datos)
        self.assertEqual(resultado["edad_usuario"], 25)
    
    def test_mapeo_personalizado(self):
        def duplicar(valor):
            return valor * 2 if isinstance(valor, (int, float)) else valor
        
        self.mapeador.agregar_transformacion_personalizada("duplicar", duplicar)
        self.mapeador.agregar_mapeo("precio", "precio_doble", transformacion="duplicar")
        
        datos = {"precio": 100}
        resultado = self.mapeador.aplicar_mapeo(datos)
        self.assertEqual(resultado["precio_doble"], 200)

class PruebaTransformadorComplejo(unittest.TestCase):
    def setUp(self):
        self.transformador = TransformadorComplejo()
        self.logger = ConfiguradorLogging.obtener_logger(__name__)
    
    def test_limpiar_dataframe(self):
        df = pd.DataFrame({
            'nombre': ['Juan  ', 'María ', 'Juan  '],
            'edad': [25, 30, 25],
            'ciudad': ['Madrid', None, 'Madrid']
        })
        
        df_limpio = self.transformador.limpiar_dataframe(df)
        self.assertEqual(len(df_limpio), 2)  # Debería eliminar duplicados
        
        # Verificar que los nombres estén sin espacios
        nombres_limpios = df_limpio['nombre'].tolist()
        for nombre in nombres_limpios:
            self.assertEqual(nombre, nombre.strip())
    
    def test_normalizar_fechas(self):
        df = pd.DataFrame({
            'fecha': ['2024-01-01', '2023-12-31', 'fecha_invalida'],
            'valor': [1, 2, 3]
        })
        
        df_normalizado = self.transformador.normalizar_fechas(df, ['fecha'])
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df_normalizado['fecha']))
    
    def test_reglas_negocio(self):
        df = pd.DataFrame({
            'producto': ['A', 'B', 'C'],
            'precio': [100, 200, 300],
            'cantidad': [1, 2, 3]
        })
        
        def calcular_total(df):
            df['total'] = df['precio'] * df['cantidad']
            return df
        
        self.transformador.agregar_regla_negocio(calcular_total)
        df_resultado = self.transformador.aplicar_reglas_negocio(df)
        
        self.assertIn('total', df_resultado.columns)
        self.assertEqual(df_resultado['total'].tolist(), [100, 400, 900])

class PruebaPipelineEndToEnd(unittest.TestCase):
    """Prueba de integración del pipeline completo con datos de muestra"""
    
    def setUp(self):
        self.logger = ConfiguradorLogging.obtener_logger(__name__)
    
    def test_flujo_completo_simulado(self):
        """Simula el flujo completo del sistema con datos de prueba"""
        self.logger.info("inicio_prueba_integracion")
        
        # 1. Validar datos de prueba
        validador = ValidadorDatos()
        datos_prueba = [
            {"id": 1, "nombre": "Cliente 1", "email": "cliente1@test.com"},
            {"id": 2, "nombre": "Cliente 2", "email": "cliente2@test.com"},
            {"id": 3, "nombre": "Cliente 3", "email": None}
        ]
        
        errores = []
        for fila in datos_prueba:
            campos_faltantes = validador.verificar_integridad_fila(
                fila, ["id", "nombre", "email"]
            )
            if campos_faltantes:
                errores.append(f"Fila {fila['id']}: faltan {campos_faltantes}")
        
        self.assertEqual(len(errores), 1)  # Solo la fila 3 debería tener error
        
        # 2. Probar transformación
        mapeador = MapeadorCampos()
        mapeador.agregar_mapeo("id", "identificador")
        mapeador.agregar_mapeo("nombre", "nombre_cliente", transformacion="mayusculas")
        mapeador.agregar_mapeo("email", "correo_electronico", valor_por_defecto="sin_email@test.com")
        
        datos_transformados = []
        for fila in datos_prueba:
            datos_transformados.append(mapeador.aplicar_mapeo(fila))
        
        self.assertEqual(len(datos_transformados), 3)
        self.assertEqual(datos_transformados[0]['nombre_cliente'], "CLIENTE 1")
        self.assertEqual(datos_transformados[2]['correo_electronico'], "sin_email@test.com")
        
        self.logger.info("prueba_integracion_completada")

if __name__ == '__main__':
    unittest.main(verbosity=2)