from flask import Blueprint, render_template, request, jsonify, send_file
from app import socketio
import os
from werkzeug.utils import secure_filename
from datetime import datetime
import tempfile
import zipfile
import json

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
    socketio.emit('log', {'mensaje': mensaje, 'tipo': tipo}, skip_sid=None)

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
    from flask import redirect, url_for
    return redirect(url_for('principal.migracion'))

@principal.route('/migracion')
def migracion():
    return render_template('migracion.html')

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


def _extension_para_motor(motor: str) -> str:
    """Devuelve una extensión de archivo recomendada según el motor destino."""
    if not motor:
        return '.db'
    m = motor.lower()
    if 'sqlite' in m:
        return '.db'
    if 'postgres' in m or 'mysql' in m or 'sql server' in m or 'oracle' in m:
        return '.sql'
    if 'mongo' in m or 'json' in m:
        return '.json'
    if 'csv' in m:
        return '.csv'
    if 'elasticsearch' in m or 'cassandra' in m:
        return '.ndjson'
    return '.db'

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
    if estado_app['proceso_activo']:
        return jsonify({'estado': 'error', 'mensaje': 'Ya hay una migracion en curso'})
    
    ip = request.remote_addr or 'desconocida'
    estado_app['proceso_activo'] = True
    estado_app['metricas'] = {'extraidos': 0, 'cargados': 0, 'errores': 0, 'tablas_ok': 0}
    
    _registrar_log('Iniciando migracion...', 'info', ip)
    
    socketio.start_background_task(ejecutar_migracion)
    return jsonify({'estado': 'exito'})

def ejecutar_migracion():
    origen = estado_app.get('origen')
    destino = estado_app.get('destino')
    tablas = origen.tablas if origen else []
    total = len(tablas)

    _registrar_log(f'Migrando {total} tablas...')

    if total == 0:
        try:
            estado_app['proceso_activo'] = False
            _registrar_log('No hay tablas para migrar', 'warning')
            socketio.emit('progreso', {
                'porcentaje': 100,
                'tabla': '',
                'estado': 'No hay tablas para migrar',
                'metricas': estado_app['metricas']
            }, skip_sid=None)
            socketio.emit('migracion_completada', estado_app['metricas'], skip_sid=None)
        finally:
            return

    try:
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
                }, skip_sid=None)

            except Exception as e:
                estado_app['metricas']['errores'] += 1
                _registrar_log(f'ERROR en {tabla}: {str(e)}', 'error')
                socketio.emit('progreso', {
                    'porcentaje': int(((idx + 1) / total) * 100) if total > 0 else 0,
                    'tabla': tabla,
                    'estado': f'ERROR en {tabla}: {str(e)}',
                    'metricas': estado_app['metricas']
                }, broadcast=True)
    finally:
        # Asegurar que siempre se marca como terminado y se notifica al cliente
        try:
            estado_app['proceso_activo'] = False

            if origen:
                estado_app['historial'].append({
                    'fecha': datetime.now().isoformat(),
                    'metricas': estado_app['metricas'].copy(),
                    'motor_destino': destino.motor if destino else None,
                    'archivo_origen': os.path.basename(origen.ruta) if getattr(origen, 'ruta', None) else '',
                    'total_tablas': total,
                })

            _registrar_log(
                f'Migracion finalizada. Tablas: {estado_app["metricas"].get("tablas_ok",0)}, '
                f'Registros: {estado_app["metricas"].get("cargados",0)}, '
                f'Errores: {estado_app["metricas"].get("errores",0)}'
            )

            socketio.emit('progreso', {
                'porcentaje': 100,
                'tabla': '',
                'estado': 'Migracion completada',
                'metricas': estado_app['metricas']
            }, skip_sid=None)

            socketio.emit('migracion_completada', estado_app['metricas'], skip_sid=None)
        except Exception as e:
            _registrar_log(f'Error al finalizar migracion: {str(e)}', 'error')

@principal.route('/api/descargar')
def api_descargar():
    """Descarga adaptable según motor destino con formato específico de cada BD"""
    if not estado_app['destino']:
        return jsonify({'estado': 'error', 'mensaje': 'No hay migración. Ejecute una migracion primero.'})
    
    destino = estado_app['destino']
    motor = destino.motor if destino else None
    
    try:
        # Generar exportación en formato específico del motor
        resultado, ext, mimetype, es_binario = destino.generar_export(motor)
        
        if not resultado:
            return jsonify({'estado': 'error', 'mensaje': 'Error generando exportación'})
        
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        
        # Si es binario (SQLite), usar directamente el archivo
        if es_binario:
            nombre_archivo = f'migracion_{timestamp}{ext}'
            if os.path.exists(resultado):
                try:
                    return send_file(resultado, as_attachment=True, download_name=nombre_archivo, mimetype=mimetype)
                except TypeError:
                    return send_file(resultado, as_attachment=True, attachment_filename=nombre_archivo, mimetype=mimetype)
        
        # Si es texto, escribir en archivo temporal
        else:
            nombre_archivo = f'migracion_{timestamp}{ext}'
            ruta_temp = os.path.join(UPLOAD_FOLDER, nombre_archivo)
            
            with open(ruta_temp, 'w', encoding='utf-8') as f:
                f.write(resultado)
            
            # Para JSON, Cassandra CQL y Redis, usar application/octet-stream para forzar descarga
            # en lugar de mostrar en navegador
            if ext in ['.json', '.cql', '.redis', '.ndjson']:
                mimetype = 'application/octet-stream'
            
            try:
                return send_file(ruta_temp, as_attachment=True, download_name=nombre_archivo, mimetype=mimetype)
            except TypeError:
                return send_file(ruta_temp, as_attachment=True, attachment_filename=nombre_archivo, mimetype=mimetype)
    
    except Exception as e:
        _registrar_log(f'Error generando exportación: {str(e)}', 'error')
        return jsonify({'estado': 'error', 'mensaje': f'Error: {str(e)}'})


@principal.route('/api/descargar-todo')
def api_descargar_todo():
    """Crea un ZIP con la base de datos resultante y un reporte JSON, y lo devuelve."""
    if estado_app['destino']:
        destino = estado_app['destino']
        ruta = destino.get_ruta_salida()
        if ruta and os.path.exists(ruta):
            timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
            motor = destino.motor if destino else 'unknown'
            motor_safe = motor.lower().replace(' ', '_')
            nombre_zip = f'migracion_paquete_{motor_safe}_{timestamp}.zip'
            ruta_zip = os.path.join(UPLOAD_FOLDER, nombre_zip)

            # Construir archivo de reporte temporal
            reporte = {
                'metricas': estado_app.get('metricas', {}),
                'historial': estado_app.get('historial', [])[-10:],
                'logs_recientes': estado_app.get('logs', [])[-200:]
            }

            reporte_path = os.path.join(UPLOAD_FOLDER, f'reporte_migracion_{timestamp}.json')
            try:
                with open(reporte_path, 'w', encoding='utf-8') as f:
                    json.dump(reporte, f, ensure_ascii=False, indent=2)

                # Crear ZIP
                ext = _extension_para_motor(destino.motor if destino else None)
                arc_db_name = f'migracion_resultado{ext}'
                with zipfile.ZipFile(ruta_zip, 'w', zipfile.ZIP_DEFLATED) as zf:
                    # incluir la DB con nombre que refleje el destino
                    zf.write(ruta, arcname=arc_db_name)
                    zf.write(reporte_path, arcname=os.path.basename(reporte_path))

                nombre = os.path.basename(ruta_zip)
                try:
                    return send_file(ruta_zip, as_attachment=True, download_name=nombre)
                except TypeError:
                    return send_file(ruta_zip, as_attachment=True, attachment_filename=nombre)
            finally:
                # Intentar eliminar el archivo de reporte temporal (el ZIP puede quedarse para auditoría)
                try:
                    if os.path.exists(reporte_path):
                        os.remove(reporte_path)
                except Exception:
                    pass

    return jsonify({'estado': 'error', 'mensaje': 'No hay archivo para descargar. Ejecute una migracion primero.'})

@principal.route('/api/descargar-sql')
def api_descargar_sql():
    """Descarga el SQL dump de la migración realizada"""
    if estado_app['destino']:
        destino = estado_app['destino']
        try:
            sql_dump = destino.generar_sql_dump()
            if sql_dump:
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
                nombre_archivo = f'migracion_dump_{timestamp}.sql'
                
                # Crear archivo temporal
                ruta_temp = os.path.join(UPLOAD_FOLDER, nombre_archivo)
                with open(ruta_temp, 'w', encoding='utf-8') as f:
                    f.write(sql_dump)
                
                try:
                    return send_file(ruta_temp, as_attachment=True, download_name=nombre_archivo, mimetype='application/sql')
                except TypeError:
                    return send_file(ruta_temp, as_attachment=True, attachment_filename=nombre_archivo, mimetype='application/sql')
        except Exception as e:
            _registrar_log(f'Error generando SQL dump: {str(e)}', 'error')
            return jsonify({'estado': 'error', 'mensaje': f'Error: {str(e)}'})
    
    return jsonify({'estado': 'error', 'mensaje': 'No hay migración completada. Ejecute una migracion primero.'})

@principal.route('/api/pausar', methods=['POST'])
def api_pausar():
    estado_app['proceso_activo'] = False
    ip = request.remote_addr or 'desconocida'
    _registrar_log('Migracion pausada', 'warning', ip)
    return jsonify({'estado': 'exito'})


@principal.route('/api/crear-estructura', methods=['POST'])
def api_crear_estructura():
    """Endpoint para crear la estructura en el destino desde la UI."""
    ip = request.remote_addr or 'desconocida'
    if not estado_app.get('destino'):
        _registrar_log('Intento de crear estructura sin destino configurado', 'error', ip)
        return jsonify({'estado': 'error', 'mensaje': 'Configure el destino primero'})

    destino = estado_app['destino']
    try:
        if estado_app.get('origen') and estado_app['origen'].esquema:
            creadas = destino.crear_estructura(estado_app['origen'].esquema)
            _registrar_log(f'Estructura creada: {creadas} tablas', 'info', ip)
            return jsonify({'estado': 'exito', 'mensaje': f'Estructura creada: {creadas} tablas'})
        else:
            _registrar_log('No hay esquema de origen para crear estructura', 'error', ip)
            return jsonify({'estado': 'error', 'mensaje': 'No hay esquema de origen. Suba un archivo primero.'})
    except Exception as e:
        _registrar_log(f'Error creando estructura: {str(e)}', 'error', ip)
        return jsonify({'estado': 'error', 'mensaje': str(e)})

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
            'metricas': estado_app['metricas'],
            'proceso_activo': estado_app['proceso_activo']
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