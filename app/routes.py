from flask import Blueprint, render_template, request, jsonify, send_file
from app import socketio
import os
import threading
from werkzeug.utils import secure_filename
from datetime import datetime

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
    'historial': [],   # registros de migraciones completadas
    'logs': [],        # log detallado de todas las acciones
    'ips': [],         # registro de accesos por IP
}

def _registrar_log(mensaje: str, tipo: str = 'info', ip: str = None):
    """Agrega entrada al log detallado de la aplicacion."""
    entrada = {
        'fecha': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'tipo': tipo,
        'mensaje': mensaje,
        'ip': ip or '',
    }
    estado_app['logs'].append(entrada)
    socketio.emit('log', {'mensaje': mensaje, 'tipo': tipo})

MAX_IPS = 1000  # limite para evitar crecimiento ilimitado de memoria

def _registrar_ip(ip: str, actividad: str):
    """Registra acceso de una IP (máximo MAX_IPS entradas)."""
    if len(estado_app['ips']) >= MAX_IPS:
        estado_app['ips'].pop(0)
    estado_app['ips'].append({
        'ip': ip,
        'fecha': datetime.now().strftime('%Y-%m-%d'),
        'hora': datetime.now().strftime('%H:%M:%S'),
        'actividad': actividad,
    })

@principal.before_app_request
def _capturar_ip():
    ip = request.remote_addr or 'desconocida'
    ruta = request.path
    # No registrar assets estáticos ni sockets
    if not ruta.startswith('/static') and not ruta.startswith('/socket.io'):
        _registrar_ip(ip, ruta)

@principal.route('/')
def index():
    return render_template('index.html')

@principal.route('/configuracion')
def configuracion():
    return render_template('configuracion.html')

@principal.route('/migracion')
def migracion():
    return render_template('configuracion.html')

@principal.route('/historial')
def historial():
    return render_template('historial.html',
                           historial=estado_app['historial'],
                           logs=estado_app['logs'])

@principal.route('/monitoreo-ip')
def monitoreo_ip():
    return render_template('monitoreo_ip.html', ips=estado_app['ips'])

# Ruta de compatibilidad: redirigir /reporte a /historial
@principal.route('/reporte')
def reporte():
    from flask import redirect, url_for
    return redirect(url_for('principal.historial'))

def _esquema_serializable(esquema: dict) -> dict:
    """Convierte el esquema al formato simple de lista de columnas para la UI."""
    resultado = {}
    for tabla, info in esquema.items():
        if isinstance(info, dict):
            resultado[tabla] = info.get('columnas', [])
        else:
            resultado[tabla] = info
    return resultado

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
    
    ip = request.remote_addr or 'desconocida'
    tipo, mensaje, _ = DetectorBaseDatos.detectar(ruta, nombre)
    
    if tipo == 'Desconocido':
        _registrar_log(f'Archivo rechazado "{nombre}": {mensaje}', 'error', ip)
        return jsonify({'estado': 'error', 'mensaje': mensaje})
    
    try:
        origen = ConectorOrigen(ruta, tipo)
        
        if not origen.tablas:
            _registrar_log(f'Archivo "{nombre}" sin tablas detectadas', 'error', ip)
            return jsonify({'estado': 'error', 'mensaje': 'No se encontraron tablas'})
        
        estado_app['origen'] = origen
        _registrar_log(
            f'Archivo cargado: "{nombre}" ({tipo}) - {len(origen.tablas)} tablas', 'info', ip
        )
        
        return jsonify({
            'estado': 'exito',
            'tipo_detectado': tipo,
            'nombre_archivo': nombre,
            'tablas': origen.tablas,
            'total_tablas': len(origen.tablas),
            'esquema': _esquema_serializable(origen.esquema)
        })
    except Exception as e:
        _registrar_log(f'Error al cargar "{nombre}": {str(e)}', 'error', ip)
        return jsonify({'estado': 'error', 'mensaje': f'Error: {str(e)}'})

@principal.route('/api/configurar-destino', methods=['POST'])
def api_configurar_destino():
    datos = request.json
    motor = datos.get('motor_destino', 'SQLite')
    ip = request.remote_addr or 'desconocida'
    
    try:
        destino = CargadorDestino(motor)
        
        if estado_app['origen'] and estado_app['origen'].esquema:
            creadas = destino.crear_estructura(estado_app['origen'].esquema)
            _registrar_log(
                f'Destino {motor} configurado: {creadas} tablas/estructuras creadas', 'info', ip
            )
        
        estado_app['destino'] = destino
        
        return jsonify({
            'estado': 'exito',
            'mensaje': f'Destino {motor} configurado. Archivo: {os.path.basename(destino.ruta_salida)}',
            'motor': motor
        })
    except Exception as e:
        _registrar_log(f'Error configurando destino {motor}: {str(e)}', 'error', ip)
        return jsonify({'estado': 'error', 'mensaje': str(e)})

@principal.route('/api/iniciar-migracion', methods=['POST'])
def api_iniciar_migracion():
    if not estado_app['origen']:
        return jsonify({'estado': 'error', 'mensaje': 'Suba un archivo primero'})
    if not estado_app['destino']:
        return jsonify({'estado': 'error', 'mensaje': 'Configure el destino primero'})
    
    ip = request.remote_addr or 'desconocida'
    estado_app['proceso_activo'] = True
    estado_app['metricas'] = {'extraidos': 0, 'cargados': 0, 'errores': 0, 'tablas_ok': 0}
    
    _registrar_log('Iniciando migracion...', 'info', ip)
    
    threading.Thread(target=ejecutar_migracion).start()
    return jsonify({'estado': 'exito'})

def ejecutar_migracion():
    origen = estado_app['origen']
    destino = estado_app['destino']
    tablas = origen.tablas
    total = len(tablas)
    
    _registrar_log(f'Migrando {total} tablas...')
    
    for idx, tabla in enumerate(tablas):
        if not estado_app['proceso_activo']:
            _registrar_log('Migracion pausada por el usuario', 'warning')
            break
        
        progreso = int(((idx + 1) / total) * 100) if total > 0 else 100
        
        try:
            _registrar_log(f'Extrayendo: {tabla}...')
            df = origen.extraer_datos(tabla)
            estado_app['metricas']['extraidos'] += len(df)
            
            if not df.empty:
                _registrar_log(f'Cargando: {tabla} ({len(df)} registros)...')
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
            _registrar_log(f'ERROR en {tabla}: {str(e)}', 'error')
    
    estado_app['proceso_activo'] = False
    
    estado_app['historial'].append({
        'fecha': datetime.now().isoformat(),
        'metricas': estado_app['metricas'].copy(),
        'motor_destino': destino.motor,
        'archivo_origen': os.path.basename(origen.ruta) if origen.ruta else '',
        'total_tablas': len(tablas),
    })
    
    _registrar_log(
        f'Migracion completada. Tablas: {estado_app["metricas"]["tablas_ok"]}, '
        f'Registros: {estado_app["metricas"]["cargados"]}, '
        f'Errores: {estado_app["metricas"]["errores"]}'
    )
    
    socketio.emit('progreso', {
        'porcentaje': 100,
        'tabla': '',
        'estado': 'Migracion completada',
        'metricas': estado_app['metricas']
    })
    
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
    ip = request.remote_addr or 'desconocida'
    _registrar_log('Migracion pausada', 'warning', ip)
    return jsonify({'estado': 'exito'})

@principal.route('/api/estado')
def api_estado():
    if estado_app['origen']:
        return jsonify({
            'estado': 'exito',
            'tipo_detectado': estado_app['origen'].tipo,
            'tablas': estado_app['origen'].tablas,
            'total_tablas': len(estado_app['origen'].tablas),
            'esquema': _esquema_serializable(estado_app['origen'].esquema),
            'motor_destino': estado_app['destino'].motor if estado_app['destino'] else None,
            'metricas': estado_app['metricas']
        })
    return jsonify({'estado': 'sin_origen'})

@principal.route('/api/historial')
def api_historial():
    return jsonify({'historial': estado_app['historial'], 'logs': estado_app['logs']})

@principal.route('/api/ips')
def api_ips():
    return jsonify({'ips': estado_app['ips']})

@socketio.on('conectar')
def conectar():
    socketio.emit('log', {'mensaje': 'Sistema listo. Suba un archivo para comenzar.'})