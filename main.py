#!/usr/bin/env python3
# Archivo principal del sistema de migración de bases de datos

import sys
from pathlib import Path

# Agregar directorio raíz al PATH
sys.path.insert(0, str(Path(__file__).parent))

from orquestador.pipeline import PipelineMigracion
from utilidades.registro_logging import ConfiguradorLogging

def principal():
    logger = ConfiguradorLogging.obtener_logger(__name__)
    
    try:
        logger.info("inicio_sistema_migracion")
        pipeline = PipelineMigracion()
        pipeline.ejecutar()
        logger.info("migracion_completada_exitosamente")
        
    except Exception as error:
        logger.error("error_sistema_migracion", error=str(error))
        sys.exit(1)

if __name__ == "__main__":
    principal()