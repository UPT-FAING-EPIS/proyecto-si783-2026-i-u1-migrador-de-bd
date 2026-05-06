CREATE DATABASE inv_farmacia;
USE inv_farmacia;

-- Tabla de productos
CREATE TABLE Producto (
    id_producto INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    descripcion VARCHAR(100),
    stock_minimo INT NOT NULL
);

-- Tabla de lotes
CREATE TABLE Lote (
    id_lote INT AUTO_INCREMENT PRIMARY KEY,
    id_producto INT NOT NULL,
    fecha_vencimiento DATE NULL,
    cantidad INT NOT NULL,
    CONSTRAINT fk_lote_producto FOREIGN KEY (id_producto)
        REFERENCES Producto(id_producto)
);

-- Tabla de usuarios
CREATE TABLE Usuario (
    id_usuario INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    rol ENUM('ADMIN', 'FARMACEUTICO') NOT NULL
);

-- Tabla de movimientos
CREATE TABLE MovimientoInventario (
    id_movimiento INT AUTO_INCREMENT PRIMARY KEY,
    id_lote INT NOT NULL,
    tipo_movimiento ENUM('ENTRADA','SALIDA') NOT NULL,
    cantidad INT NOT NULL,
    fecha DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    id_usuario INT NOT NULL,
    CONSTRAINT fk_movimiento_lote FOREIGN KEY (id_lote)
        REFERENCES Lote(id_lote),
    CONSTRAINT fk_movimiento_usuario FOREIGN KEY (id_usuario)
        REFERENCES Usuario(id_usuario)
);

-- Tabla de alertas
CREATE TABLE Alertas (
    id_alerta INT AUTO_INCREMENT PRIMARY KEY,
    id_producto INT NOT NULL,
    tipo_alerta ENUM('STOCK_BAJO','FECHA_VENCIMIENTO','OTROS') NOT NULL,
    mensaje VARCHAR(255) NOT NULL,
    fecha_generada DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atendida BOOLEAN DEFAULT FALSE,
    CONSTRAINT fk_alerta_producto FOREIGN KEY (id_producto)
        REFERENCES Producto(id_producto)
);
