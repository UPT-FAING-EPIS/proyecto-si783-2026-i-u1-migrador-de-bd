-- SQL con esquemas para pruebas
-- Esquema Inventario
CREATE TABLE Inventario.Productos (
    id INT PRIMARY KEY,
    nombre VARCHAR(255),
    cantidad INT,
    precio DECIMAL(10,2)
);

CREATE TABLE Inventario.MovimientosInventario (
    id INT PRIMARY KEY,
    producto_id INT,
    tipo_movimiento VARCHAR(50),
    cantidad INT,
    fecha DATE
);

-- Esquema Ventas
CREATE TABLE Ventas.Clientes (
    id INT PRIMARY KEY,
    nombre VARCHAR(255),
    email VARCHAR(255)
);

CREATE TABLE Ventas.Ordenes (
    id INT PRIMARY KEY,
    cliente_id INT,
    fecha DATE,
    total DECIMAL(10,2)
);

-- Datos
INSERT INTO Inventario.Productos (id, nombre, cantidad, precio) VALUES
(1, 'Producto A', 100, 15.50),
(2, 'Producto B', 200, 25.75),
(3, 'Producto C', 150, 10.25);

INSERT INTO Inventario.MovimientosInventario (id, producto_id, tipo_movimiento, cantidad, fecha) VALUES
(1, 1, 'Entrada', 50, '2026-05-01'),
(2, 2, 'Salida', 10, '2026-05-02'),
(3, 3, 'Entrada', 30, '2026-05-03');

INSERT INTO Ventas.Clientes (id, nombre, email) VALUES
(1, 'Juan Pérez', 'juan@example.com'),
(2, 'María González', 'maria@example.com');

INSERT INTO Ventas.Ordenes (id, cliente_id, fecha, total) VALUES
(1, 1, '2026-05-01', 100.00),
(2, 2, '2026-05-02', 250.50);
