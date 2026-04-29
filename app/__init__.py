from flask import Flask
from flask_socketio import SocketIO

socketio = SocketIO()

def crear_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'migrador-bd-secreto-2024'
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
    app.config['UPLOAD_FOLDER'] = 'uploads'
    
    socketio.init_app(app, cors_allowed_origins="*")
    
    from app.routes import principal
    app.register_blueprint(principal)
    
    return app