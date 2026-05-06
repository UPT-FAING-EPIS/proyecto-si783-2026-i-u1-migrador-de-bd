

-- Eliminar tabla si existe (solo para desarrollo)
DROP TABLE IF EXISTS personas;


CREATE TABLE personas (
     id INT AUTO_INCREMENT PRIMARY KEY,
     nombre VARCHAR(100) NOT NULL,
     apellido VARCHAR(100) NOT NULL,
    email VARCHAR(100),
     telefono VARCHAR(20),    
	  fecha_creacion DATETIME,
     fecha_actualizacion DATETIME
);

-- Crear índices
CREATE INDEX idx_nombre_apellido ON personas(nombre, apellido);
CREATE INDEX idx_email ON personas(email);

-- Insertar datos de ejemplo
INSERT INTO personas (nombre, apellido, email, telefono) VALUES
('Juan', 'Pérez', 'juan.perez@email.com', '+1234567890'),
('María', 'Gómez', 'maria.gomez@email.com', '+1234567891'),
('Carlos', 'López', 'carlos.lopez@email.com', '+1234567892'),
('Ana', 'Martínez', 'ana.martinez@email.com', '+1234567893'),
('Pedro', 'Rodríguez', 'pedro.rodriguez@email.com', '+1234567894'),
('Laura', 'Hernández', 'laura.hernandez@email.com', '+1234567895'),
('Miguel', 'Díaz', 'miguel.diaz@email.com', '+1234567896'),
('Sofía', 'Torres', 'sofia.torres@email.com', '+1234567897'),
('David', 'Ramírez', 'david.ramirez@email.com', '+1234567898'),
('Elena', 'Flores', 'elena.flores@email.com', '+1234567899');

-- Verificar la inserción
SELECT * FROM personas;