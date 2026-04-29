from flask import Blueprint, render_template, request, jsonify, send_file
from app import socketio
import os
import threading
from werkzeug.utils import secure_filename

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utilidades.detector import DetectorBaseDatos
from extraccion.conector import ConectorOrigen
from transformacion.mapeador import MapeadorDatos
from carga.cargador import CargadorDestino

principal = Blueprint('principal', __name__)

# Corregir ruta de uploads
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

estado_app = {
    'origen': None,
    'destino': None,
    'proceso_activo': False,
    'metricas': {'extraidos': 0, 'cargados': 0, 'errores': 0, 'tablas_ok': 0},
    'historial': []
}

@principal.route('/')
def index():
    return render_template('index.html')

@principal.route('/configuracion')
def configuracion():
    return render_template('configuracion.html')

@principal.route('/migracion')
def migracion():
    return render_template('configuracion.html')

@principal.route('/reporte')
def reporte():
    return render_template('reporte.html', metricas=estado_app['metricas'], historial=estado_app['historial'])

@principal.route('/api/subir-archivo', methods=['POST'])
def api_subir_archivo():
    if 'archivo' not in request.files:
        return jsonify({'estado': 'error', 'mensaje': 'No se subio archivo'})
    
    archivo = request.files['archivo']
    if archivo.filename == '':
        return jsonify({'estado': 'error', 'mensaje': 'Archivo vacio'})
    
    nombre = secure_filename(archivo.filename)
    ruta = os.path.join(UPLOAD_FOLDER, nombre)
    archivo.save(ruta)
    
    tipo, mensaje, _ = DetectorBaseDatos.detectar(ruta, nombre)
    
    if tipo == 'Desconocido':
        return jsonify({'estado': 'error', 'mensaje': mensaje})
    
    try:
        origen = ConectorOrigen(ruta, tipo)
        
        if not origen.tablas:
            return jsonify({'estado': 'error', 'mensaje': 'No se encontraron tablas'})
        
        estado_app['origen'] = origen
        
        socketio.emit('log', {'mensaje': f'Archivo detectado: {tipo} - {len(origen.tablas)} tablas'})
        
        return jsonify({
            'estado': 'exito',
            'tipo_detectado': tipo,
            'nombre_archivo': nombre,
            'tablas': origen.tablas,
            'total_tablas': len(origen.tablas),
            'esquema': origen.esquema
        })
    except Exception as e:
        return jsonify({'estado': 'error', 'mensaje': f'Error: {str(e)}'})

@principal.route('/api/configurar-destino', methods=['POST'])
def api_configurar_destino():
    datos = request.json
    motor = datos.get('motor_destino', 'SQLite')
    
    try:
        destino = CargadorDestino(motor)
        
        if estado_app['origen'] and estado_app['origen'].esquema:
            creadas = destino.crear_estructura(estado_app['origen'].esquema)
            socketio.emit('log', {'mensaje': f'{creadas} tablas creadas en destino {motor}'})
        
        estado_app['destino'] = destino
        
        return jsonify({
            'estado': 'exito',
            'mensaje': f'Destino {motor} configurado. Archivo: {os.path.basename(destino.ruta_salida)}',
            'motor': motor
        })
    except Exception as e:
        return jsonify({'estado': 'error', 'mensaje': str(e)})

@principal.route('/api/iniciar-migracion', methods=['POST'])
def api_iniciar_migracion():
    if not estado_app['origen']:
        return jsonify({'estado': 'error', 'mensaje': 'Suba un archivo primero'})
    if not estado_app['destino']:
        return jsonify({'estado': 'error', 'mensaje': 'Configure el destino primero'})
    
    estado_app['proceso_activo'] = True
    estado_app['metricas'] = {'extraidos': 0, 'cargados': 0, 'errores': 0, 'tablas_ok': 0}
    
    socketio.emit('log', {'mensaje': 'Iniciando migracion...'})
    
    threading.Thread(target=ejecutar_migracion).start()
    return jsonify({'estado': 'exito'})

def ejecutar_migracion():
    origen = estado_app['origen']
    destino = estado_app['destino']
    tablas = origen.tablas
    total = len(tablas)
    
    socketio.emit('log', {'mensaje': f'Migrando {total} tablas...'})
    
    for idx, tabla in enumerate(tablas):
        if not estado_app['proceso_activo']:
            break
        
        progreso = int(((idx + 1) / total) * 100) if total > 0 else 100
        
        try:
            socketio.emit('log', {'mensaje': f'Extrayendo: {tabla}...'})
            df = origen.extraer_datos(tabla)
            estado_app['metricas']['extraidos'] += len(df)
            
            if not df.empty:
                socketio.emit('log', {'mensaje': f'Cargando: {tabla} ({len(df)} registros)...'})
                df = MapeadorDatos.limpiar_dataframe(df)
                cargados = destino.cargar_tabla(tabla, df)
                estado_app['metricas']['cargados'] += cargados
            
            estado_app['metricas']['tablas_ok'] += 1
            
            socketio.emit('progreso', {
                'porcentaje': progreso,
                'tabla': tabla,
                'estado': f'{tabla}: {len(df)} registros migrados',
                'metricas': estado_app['metricas']
            })
            
        except Exception as e:
            estado_app['metricas']['errores'] += 1
            socketio.emit('log', {'mensaje': f'ERROR en {tabla}: {str(e)}', 'tipo': 'error'})
    
    estado_app['proceso_activo'] = False
    
    from datetime import datetime
    estado_app['historial'].append({
        'fecha': datetime.now().isoformat(),
        'metricas': estado_app['metricas'].copy(),
        'motor_destino': destino.motor
    })
    
    socketio.emit('progreso', {
        'porcentaje': 100,
        'tabla': '',
        'estado': 'Migracion completada',
        'metricas': estado_app['metricas']
    })
    
    socketio.emit('log', {'mensaje': f'Migracion completada. Archivo: {os.path.basename(destino.ruta_salida)}'})
    socketio.emit('migracion_completada', estado_app['metricas'])

@principal.route('/api/descargar')
def api_descargar():
    if estado_app['destino']:
        ruta = estado_app['destino'].get_ruta_salida()
        if os.path.exists(ruta):
            return send_file(ruta, as_attachment=True, download_name='migracion_resultado.db')
    return jsonify({'estado': 'error', 'mensaje': 'No hay archivo para descargar. Ejecute una migracion primero.'})

@principal.route('/api/pausar', methods=['POST'])
def api_pausar():
    estado_app['proceso_activo'] = False
    socketio.emit('log', {'mensaje': 'Migracion pausada'})
    return jsonify({'estado': 'exito'})

@principal.route('/api/estado')
def api_estado():
    if estado_app['origen']:
        return jsonify({
            'estado': 'exito',
            'tipo_detectado': estado_app['origen'].tipo,
            'tablas': estado_app['origen'].tablas,
            'total_tablas': len(estado_app['origen'].tablas),
            'esquema': estado_app['origen'].esquema,
            'motor_destino': estado_app['destino'].motor if estado_app['destino'] else None,
            'metricas': estado_app['metricas']
        })
    return jsonify({'estado': 'sin_origen'})

@socketio.on('conectar')
def conectar():
    socketio.emit('log', {'mensaje': 'Sistema listo. Suba un archivo para comenzar.'})