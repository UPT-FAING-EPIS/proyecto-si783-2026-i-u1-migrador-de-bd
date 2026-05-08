import sqlite3
import os
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta
from functools import wraps
from flask import session, redirect, url_for, current_app, jsonify
import secrets
import string
import random
from flask_mail import Message
from app import mail

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'auth.db')

def inicializar_bd():
    """Crea las tablas si no existen y actualiza columnas faltantes."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Crear tabla usuarios si no existe
    c.execute('''
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            contraseña TEXT,
            rol TEXT NOT NULL DEFAULT 'usuario',
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            activo BOOLEAN DEFAULT 1,
            verification_code TEXT,
            verified BOOLEAN DEFAULT 0
        )
    ''')

    # Verificar y agregar columnas faltantes
    c.execute("PRAGMA table_info(usuarios)")
    columns = [column[1] for column in c.fetchall()]

    if 'verification_code' not in columns:
        c.execute("ALTER TABLE usuarios ADD COLUMN verification_code TEXT")

    if 'verified' not in columns:
        c.execute("ALTER TABLE usuarios ADD COLUMN verified BOOLEAN DEFAULT 0")

    # Crear tabla oauth_usuarios si no existe
    c.execute('''
        CREATE TABLE IF NOT EXISTS oauth_usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usuario_id INTEGER NOT NULL,
            proveedor TEXT NOT NULL,
            proveedor_id TEXT NOT NULL,
            email TEXT NOT NULL,
            nombre TEXT,
            foto_url TEXT,
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (usuario_id) REFERENCES usuarios(id),
            UNIQUE(proveedor, proveedor_id)
        )
    ''')
    conn.commit()
    conn.close()

    # Crear admin por defecto si no existe
    crear_admin_defecto()

def crear_admin_defecto():
    """Crea un admin por defecto si no existe ninguno."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM usuarios WHERE rol = ?', ('admin',))
    if c.fetchone()[0] == 0:
        contraseña_hash = generate_password_hash('admin123')
        c.execute(
            'INSERT INTO usuarios (usuario, email, contraseña, rol, verified) VALUES (?, ?, ?, ?, ?)',
            ('admin', 'admin@migrador.local', contraseña_hash, 'admin', 1)
        )
        conn.commit()
    conn.close()

def enviar_email_verificacion(email, codigo):
    """Envía un email con el código de verificación."""
    try:
        msg = Message('Código de Verificación - MigradorBD',
                      sender=current_app.config['MAIL_DEFAULT_SENDER'],
                      recipients=[email])
        msg.body = f'''Hola,

Tu código de verificación para activar tu cuenta en MigradorBD es: {codigo}

Ingresa este código en la página de verificación para completar tu registro.

Si no solicitaste este registro, ignora este mensaje.

Saludos,
Equipo de MigradorBD
'''
        mail.send(msg)
        return True, None
    except Exception as e:
        error_text = str(e)
        print(f"Error enviando email: {error_text}")
        return False, error_text


def enviar_email_notificacion(email, asunto, mensaje):
    """Envía un email de notificación genérico al usuario."""
    try:
        msg = Message(asunto,
                      sender=current_app.config['MAIL_DEFAULT_SENDER'],
                      recipients=[email])
        msg.body = mensaje
        mail.send(msg)
        return True, None
    except Exception as e:
        error_text = str(e)
        print(f"Error enviando email de notificación: {error_text}")
        return False, error_text


def registrar_usuario(usuario, email, contraseña):
    """Registra un nuevo usuario con rol 'usuario' y envía código de verificación."""
    try:
        # Generar código de verificación
        codigo = ''.join(random.choices(string.digits, k=6))

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        contraseña_hash = generate_password_hash(contraseña)
        c.execute(
            'INSERT INTO usuarios (usuario, email, contraseña, rol, verification_code, verified) VALUES (?, ?, ?, ?, ?, ?)',
            (usuario, email, contraseña_hash, 'usuario', codigo, 0)
        )
        conn.commit()
        conn.close()

        # Enviar email de verificación
        enviado, error = enviar_email_verificacion(email, codigo)
        if enviado:
            return True, 'Registro exitoso. Revisa tu email para el código de verificación.'
        else:
            mensaje = 'Registro exitoso, pero no se pudo enviar el email de verificación.'
            if 'Username and Password not accepted' in str(error) or 'BadCredentials' in str(error):
                mensaje += ' Revisa MAIL_USERNAME y MAIL_PASSWORD en .env; usa una contraseña de aplicación de Gmail si corresponde.'
            else:
                mensaje += ' Contacta al administrador.'
            return True, mensaje
    except sqlite3.IntegrityError as e:
        if 'usuario' in str(e):
            return False, 'El usuario ya existe'
        elif 'email' in str(e):
            return False, 'El email ya está registrado'
        return False, 'Error en el registro'
    except Exception as e:
        return False, str(e)

def verificar_usuario(usuario, contraseña):
    """Verifica credenciales y devuelve datos del usuario."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT id, usuario, email, rol, activo, verified FROM usuarios WHERE usuario = ?', (usuario,))
        resultado = c.fetchone()
        conn.close()

        if not resultado:
            return None

        id_usuario, user, email, rol, activo, verified = resultado

        if not verified:
            return None  # Usuario no verificado

        # Verificar contraseña
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT contraseña FROM usuarios WHERE id = ?', (id_usuario,))
        contraseña_hash = c.fetchone()[0]
        conn.close()

        if check_password_hash(contraseña_hash, contraseña) and activo:
            return {'id': id_usuario, 'usuario': user, 'email': email, 'rol': rol}
        return None
    except Exception as e:
        return None

def obtener_usuario_por_email(email):
    """Obtiene datos del usuario por email."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT id, usuario, email, verified FROM usuarios WHERE email = ?', (email,))
        resultado = c.fetchone()
        conn.close()

        if resultado:
            return {'id': resultado[0], 'usuario': resultado[1], 'email': resultado[2], 'verified': resultado[3]}
        return None
    except Exception as e:
        return None

def verificar_codigo(email, codigo):
    """Verifica el código de verificación y activa la cuenta."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT id, verification_code FROM usuarios WHERE email = ? AND verified = 0', (email,))
        resultado = c.fetchone()

        if not resultado:
            conn.close()
            return False, 'Usuario no encontrado o ya verificado'

        id_usuario, stored_code = resultado

        if stored_code == codigo:
            c.execute('UPDATE usuarios SET verified = 1, verification_code = NULL WHERE id = ?', (id_usuario,))
            conn.commit()
            conn.close()
            return True, 'Cuenta verificada exitosamente'
        else:
            conn.close()
            return False, 'Código incorrecto'
    except Exception as e:
        return False, str(e)

def registrar_usuario_oauth(proveedor, proveedor_id, email, nombre=None, foto_url=None):
    """Registra o actualiza un usuario OAuth."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # Verificar si el usuario ya existe con este proveedor
        c.execute(
            'SELECT usuario_id FROM oauth_usuarios WHERE proveedor = ? AND proveedor_id = ?',
            (proveedor, proveedor_id)
        )
        resultado = c.fetchone()

        if resultado:
            # Usuario ya existe, actualizar
            usuario_id = resultado[0]
            c.execute(
                'UPDATE oauth_usuarios SET nombre = ?, foto_url = ? WHERE usuario_id = ? AND proveedor = ?',
                (nombre, foto_url, usuario_id, proveedor)
            )
            conn.commit()
        else:
            # Usuario nuevo, crear
            # Primero verificar si el email ya existe
            c.execute('SELECT id FROM usuarios WHERE email = ?', (email,))
            usuario_existente = c.fetchone()

            if usuario_existente:
                usuario_id = usuario_existente[0]
            else:
                # Crear nuevo usuario con nombre del proveedor
                usuario_generado = nombre or email.split('@')[0]
                # Asegurar que el usuario es único
                usuario_base = usuario_generado
                contador = 1
                while True:
                    c.execute('SELECT id FROM usuarios WHERE usuario = ?', (usuario_generado,))
                    if not c.fetchone():
                        break
                    usuario_generado = f"{usuario_base}{contador}"
                    contador += 1

                c.execute(
                    'INSERT INTO usuarios (usuario, email, rol) VALUES (?, ?, ?)',
                    (usuario_generado, email, 'usuario')
                )
                conn.commit()
                usuario_id = c.lastrowid

            # Registrar OAuth
            c.execute(
                'INSERT INTO oauth_usuarios (usuario_id, proveedor, proveedor_id, email, nombre, foto_url) VALUES (?, ?, ?, ?, ?, ?)',
                (usuario_id, proveedor, proveedor_id, email, nombre, foto_url)
            )
            conn.commit()

        conn.close()
        return obtener_usuario_por_id(usuario_id)
    except Exception as e:
        return None

def obtener_usuario_por_id(usuario_id):
    """Obtiene datos del usuario por ID."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT id, usuario, email, rol FROM usuarios WHERE id = ?', (usuario_id,))
        resultado = c.fetchone()
        conn.close()
        if resultado:
            return {'id': resultado[0], 'usuario': resultado[1], 'email': resultado[2], 'rol': resultado[3]}
    except:
        pass
    return None

def requerir_login(f):
    """Decorador para proteger rutas que requieren login."""
    @wraps(f)
    def decorado(*args, **kwargs):
        if 'usuario_id' not in session:
            return redirect(url_for('principal.login'))
        return f(*args, **kwargs)
    return decorado

def requerir_admin(f):
    """Decorador para proteger rutas que requieren rol admin."""
    @wraps(f)
    def decorado(*args, **kwargs):
        if 'usuario_id' not in session:
            return redirect(url_for('principal.login'))

        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute('SELECT rol FROM usuarios WHERE id = ?', (session['usuario_id'],))
        resultado = c.fetchone()
        conn.close()

        if not resultado or resultado[0] != 'admin':
            return jsonify({'estado': 'error', 'mensaje': 'Solo administradores pueden acceder'}), 403

        return f(*args, **kwargs)
    return decorado

def obtener_usuario_actual():
    """Obtiene los datos del usuario en sesión."""
    if 'usuario_id' not in session:
        return None
    return obtener_usuario_por_id(session['usuario_id'])

def crear_nuevo_admin(usuario_creador_id, usuario_nuevo, email_nuevo):
    """Solo un admin puede crear otro admin."""
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        # Verificar que el creador es admin
        c.execute('SELECT rol FROM usuarios WHERE id = ?', (usuario_creador_id,))
        resultado = c.fetchone()
        if not resultado or resultado[0] != 'admin':
            conn.close()
            return False, 'Solo administradores pueden crear otros admins'

        # Generar contraseña temporal
        caracteres = string.ascii_letters + string.digits + "!@#$%"
        contraseña_temporal = ''.join(secrets.choice(caracteres) for _ in range(12))
        contraseña_hash = generate_password_hash(contraseña_temporal)

        c.execute(
            'INSERT INTO usuarios (usuario, email, contraseña, rol) VALUES (?, ?, ?, ?)',
            (usuario_nuevo, email_nuevo, contraseña_hash, 'admin')
        )
        conn.commit()
        conn.close()
        return True, f'Admin creado. Contraseña temporal: {contraseña_temporal}'
    except sqlite3.IntegrityError:
        return False, 'El usuario o email ya existe'
    except Exception as e:
        return False, str(e)
