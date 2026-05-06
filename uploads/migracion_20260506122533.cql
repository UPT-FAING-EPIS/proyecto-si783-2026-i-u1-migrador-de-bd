-- CQL Script generado por MigradorBD
-- Fecha: 2026-05-06 12:25:33.132752

-- Cassandra keyspace (crear manualmente si es necesario)
-- USE migracion_keyspace;

CREATE TABLE IF NOT EXISTS roles (
  columna_1 text,
  PRIMARY KEY (id)
) WITH CLUSTERING ORDER BY (id DESC);

CREATE TABLE IF NOT EXISTS pais (
  columna_1 text,
  PRIMARY KEY (id)
) WITH CLUSTERING ORDER BY (id DESC);

CREATE TABLE IF NOT EXISTS personas (
  columna_1 text,
  PRIMARY KEY (id)
) WITH CLUSTERING ORDER BY (id DESC);

CREATE TABLE IF NOT EXISTS tbcargo (
  idcargo text,
  nombre text,
  estado text,
  PRIMARY KEY (id)
) WITH CLUSTERING ORDER BY (id DESC);

INSERT INTO tbcargo (idcargo, nombre, estado) VALUES (1, 'ADMINISTRADOR', 1);
INSERT INTO tbcargo (idcargo, nombre, estado) VALUES (2, 'VENDEDOR', 1);
INSERT INTO tbcargo (idcargo, nombre, estado) VALUES (3, 'ALMACENERO', 1);

CREATE TABLE IF NOT EXISTS tbcategoria (
  idcategoria text,
  nombre text,
  descripcion text,
  estado text,
  fecha_creacion text,
  PRIMARY KEY (id)
) WITH CLUSTERING ORDER BY (id DESC);

INSERT INTO tbcategoria (idcategoria, nombre, descripcion, estado, fecha_creacion) VALUES (1, 'Electrónicos', 'Productos electrónicos y tecnológicos', 1, '2025-10-21 07:32:14');
INSERT INTO tbcategoria (idcategoria, nombre, descripcion, estado, fecha_creacion) VALUES (2, 'Ropa', 'Prendas de vestir para hombres, mujeres y niños', 1, '2025-10-21 07:32:14');
INSERT INTO tbcategoria (idcategoria, nombre, descripcion, estado, fecha_creacion) VALUES (3, 'Hogar', 'Artículos para el hogar y decoración', 1, '2025-10-21 07:32:14');
INSERT INTO tbcategoria (idcategoria, nombre, descripcion, estado, fecha_creacion) VALUES (4, 'Deportes', 'Equipos y artículos deportivos', 1, '2025-10-21 07:32:14');

CREATE TABLE IF NOT EXISTS tbcliente (
  idcliente text,
  dni text,
  nombre text,
  apellido text,
  email text,
  telefono text,
  direccion text,
  clave text,
  estado text,
  fecha_registro text,
  PRIMARY KEY (id)
) WITH CLUSTERING ORDER BY (id DESC);

INSERT INTO tbcliente (idcliente, dni, nombre, apellido, email, telefono, direccion, clave, estado, fecha_registro) VALUES (1, '11111111', 'Alberto', 'Mamani', 'albertomamani@gmail.com', '963852741', 'Av. Callao', 'cliente123', 1, '2025-10-19 17:51:29');

CREATE TABLE IF NOT EXISTS tbproducto (
  idproducto text,
  codigo text,
  nombre text,
  descripcion text,
  precio text,
  stock text,
  idcategoria text,
  imagen text,
  estado text,
  fecha_creacion text,
  PRIMARY KEY (id)
) WITH CLUSTERING ORDER BY (id DESC);

INSERT INTO tbproducto (idproducto, codigo, nombre, descripcion, precio, stock, idcategoria, imagen, estado, fecha_creacion) VALUES (1, 'PROD001', 'Laptop HP 15"', 'Laptop HP 15.6 pulgadas, 8GB RAM, 256GB SSD', 1200.0, 10, 1, 'laptop.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO tbproducto (idproducto, codigo, nombre, descripcion, precio, stock, idcategoria, imagen, estado, fecha_creacion) VALUES (2, 'PROD002', 'Smartphone Samsung', 'Smartphone Samsung Galaxy A54 128GB', 450.0, 25, 1, 'smartphone.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO tbproducto (idproducto, codigo, nombre, descripcion, precio, stock, idcategoria, imagen, estado, fecha_creacion) VALUES (3, 'PROD003', 'Camiseta Nike', 'Camiseta deportiva Nike Dri-FIT', 35.0, 50, 2, 'camiseta.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO tbproducto (idproducto, codigo, nombre, descripcion, precio, stock, idcategoria, imagen, estado, fecha_creacion) VALUES (4, 'PROD004', 'Zapatos Deportivos', 'Zapatos deportivos Adidas Running', 85.0, 30, 2, 'zapatos.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO tbproducto (idproducto, codigo, nombre, descripcion, precio, stock, idcategoria, imagen, estado, fecha_creacion) VALUES (5, 'PROD005', 'Sofá 3 Plazas', 'Sofá moderno 3 plazas color gris', 650.0, 5, 3, 'sofa.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO tbproducto (idproducto, codigo, nombre, descripcion, precio, stock, idcategoria, imagen, estado, fecha_creacion) VALUES (6, 'PROD006', 'Pelota de Fútbol', 'Pelota de fútbol profesional tamaño 5', 25.0, 40, 4, 'pelota.jpg', 1, '2025-10-21 07:32:14');

CREATE TABLE IF NOT EXISTS tbusuario (
  idusuario text,
  dni text,
  nombre text,
  apellido text,
  email text,
  telefono text,
  direccion text,
  usuario text,
  clave text,
  idcargo text,
  estado text,
  fecha_registro text,
  PRIMARY KEY (id)
) WITH CLUSTERING ORDER BY (id DESC);

INSERT INTO tbusuario (idusuario, dni, nombre, apellido, email, telefono, direccion, usuario, clave, idcargo, estado, fecha_registro) VALUES (1, '12345678', 'Juan', 'Pérez', 'juan@dbtienda.com', '987654321', 'Av. Principal 123', 'admin', 'admin123', 1, 1, '2025-10-21 07:32:14');
INSERT INTO tbusuario (idusuario, dni, nombre, apellido, email, telefono, direccion, usuario, clave, idcargo, estado, fecha_registro) VALUES (2, '87654321', 'María', 'Gómez', 'maria@dbtienda.com', '987654322', 'Av. Secundaria 456', 'vendedor', 'vendedor123', 2, 1, '2025-10-21 07:32:14');
INSERT INTO tbusuario (idusuario, dni, nombre, apellido, email, telefono, direccion, usuario, clave, idcargo, estado, fecha_registro) VALUES (3, '11223344', 'Carlos', 'López', 'carlos@dbtienda.com', '987654323', 'Calle Los Olivos 789', 'almacen', 'almacen123', 3, 1, '2025-10-21 07:32:14');

