 -- Crear la base de datos
CREATE DATABASE IF NOT EXISTS EVENTOS_DB;
USE EVENTOS_DB;

-- TABLA DE ROLES
CREATE TABLE roles (
    id_rol INT AUTO_INCREMENT PRIMARY KEY,
    nombre_rol ENUM('Administrador', 'Asistente', 'Participante') NOT NULL UNIQUE
);

-- Insertar roles iniciales
INSERT INTO roles (nombre_rol) VALUES
('Administrador'),
('Asistente'),
('Participante');

-- TABLA DE USUARIOS
CREATE TABLE usuarios (
    id_usuario INT AUTO_INCREMENT PRIMARY KEY,
    usuario VARCHAR(50) NOT NULL UNIQUE,
    contraseña VARCHAR(100) NOT NULL,
    id_rol INT NOT NULL,
    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_rol) REFERENCES roles(id_rol)
);

-- TABLA DE EVENTOS
CREATE TABLE eventos (
    id_evento INT AUTO_INCREMENT PRIMARY KEY,
    nombre_evento VARCHAR(100) NOT NULL,
    fecha_evento DATE NOT NULL,
    aforo_maximo INT NOT NULL,
    aforo_actual INT DEFAULT 0
);

-- TABLA DE INSCRIPCIONES
CREATE TABLE inscripciones (
    id_inscripcion INT AUTO_INCREMENT PRIMARY KEY,
    id_usuario INT NOT NULL,
    id_evento INT NOT NULL,
    fecha_inscripcion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario),
    FOREIGN KEY (id_evento) REFERENCES eventos(id_evento),
    UNIQUE(id_usuario, id_evento) -- un usuario no puede inscribirse dos veces al mismo evento
);


-- TRIGGER: Validar aforo en inscripciones

DELIMITER //
CREATE TRIGGER trg_validar_aforo
BEFORE INSERT ON inscripciones
FOR EACH ROW
BEGIN
    DECLARE aforo_max INT;
    DECLARE aforo_act INT;

    -- Obtener aforo máximo y actual
    SELECT aforo_maximo, aforo_actual
    INTO aforo_max, aforo_act
    FROM eventos
    WHERE id_evento = NEW.id_evento;

    -- Verificar si el evento ya está lleno
    IF aforo_act >= aforo_max THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = '❌ Aforo completo. No se pueden registrar más inscripciones.';
    ELSE
        -- Incrementar aforo_actual
        UPDATE eventos
        SET aforo_actual = aforo_actual + 1
        WHERE id_evento = NEW.id_evento;
    END IF;
END//
DELIMITER ;


-- PROCEDIMIENTO: Reporte de inscripciones por evento

DELIMITER //
CREATE PROCEDURE sp_reporte_inscripciones(IN evento_id INT)
BEGIN
    SELECT e.nombre_evento,
           e.fecha_evento,
           u.usuario,
           r.nombre_rol,
           i.fecha_inscripcion
    FROM inscripciones i
    INNER JOIN usuarios u ON i.id_usuario = u.id_usuario
    INNER JOIN roles r ON u.id_rol = r.id_rol
    INNER JOIN eventos e ON i.id_evento = e.id_evento
    WHERE e.id_evento = evento_id
    ORDER BY i.fecha_inscripcion;
END//
DELIMITER ;


-- PROCEDIMIENTO: Crear nuevo usuario
DELIMITER //
CREATE PROCEDURE sp_registrar_usuario(
    IN p_usuario VARCHAR(50),
    IN p_contraseña VARCHAR(100),
    IN p_id_rol INT
)
BEGIN
    INSERT INTO usuarios (usuario, contraseña, id_rol)
    VALUES (p_usuario, p_contraseña, p_id_rol);
END//
DELIMITER ;

-- PROCEDIMIENTO: Login de usuario
DELIMITER //
CREATE PROCEDURE sp_login(
    IN p_usuario VARCHAR(50),
    IN p_contraseña VARCHAR(100)
)
BEGIN
    SELECT u.id_usuario, u.usuario, r.nombre_rol
    FROM usuarios u
    INNER JOIN roles r ON u.id_rol = r.id_rol
    WHERE u.usuario = p_usuario AND u.contraseña = p_contraseña;
END//
DELIMITER ;


-- ÍNDICES para optimización
CREATE INDEX idx_usuario_login ON usuarios(usuario, contraseña);
CREATE INDEX idx_evento_fecha ON eventos(fecha_evento);
CREATE INDEX idx_inscripcion_usuario_evento ON inscripciones(id_usuario, id_evento);