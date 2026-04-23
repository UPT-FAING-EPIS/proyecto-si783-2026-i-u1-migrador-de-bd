from datetime import datetime
from typing import Dict, Any
import json
from pathlib import Path

class GeneradorReporte:
    def __init__(self, directorio_salida: str = "reportes"):
        self.directorio = Path(directorio_salida)
        self.directorio.mkdir(exist_ok=True)
    
    def generar_reporte_json(self, metricas: Dict[str, Any]) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_archivo = f"reporte_migracion_{timestamp}.json"
        ruta = self.directorio / nombre_archivo
        
        with open(ruta, 'w', encoding='utf-8') as archivo:
            json.dump(metricas, archivo, indent=2, default=str)
        
        return str(ruta)
    
    def generar_reporte_html(self, metricas: Dict[str, Any]) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        nombre_archivo = f"reporte_migracion_{timestamp}.html"
        ruta = self.directorio / nombre_archivo
        
        plantilla_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Reporte de Migración - {timestamp}</title>
            <style>
                body {{ font-family: Arial; margin: 20px; }}
                .metricas {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }}
                .metrica {{ padding: 15px; border: 1px solid #ddd; border-radius: 5px; }}
                .exito {{ background-color: #d4edda; }}
                .error {{ background-color: #f8d7da; }}
            </style>
        </head>
        <body>
            <h1>Reporte de Migración de Base de Datos</h1>
            <p>Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div class="metricas">
                <div class="metrica exito">
                    <h3>Filas Extraídas</h3>
                    <p>{metricas.get('filas_extraidas', 0):,}</p>
                </div>
                <div class="metrica exito">
                    <h3>Filas Transformadas</h3>
                    <p>{metricas.get('filas_transformadas', 0):,}</p>
                </div>
                <div class="metrica exito">
                    <h3>Filas Cargadas</h3>
                    <p>{metricas.get('filas_cargadas', 0):,}</p>
                </div>
            </div>
            
            <h2>Resumen de Errores</h2>
            <p>Total errores: {metricas.get('errores', 0)}</p>
            
            <h2>Duración Total</h2>
            <p>{metricas.get('duracion', 0):.2f} segundos</p>
        </body>
        </html>
        """
        
        with open(ruta, 'w', encoding='utf-8') as archivo:
            archivo.write(plantilla_html)
        
        return str(ruta)