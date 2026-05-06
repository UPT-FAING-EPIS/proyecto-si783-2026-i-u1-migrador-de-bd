-- SQL Dump generado por MigradorBD
-- Fecha: 2026-05-06 12:29:25.223696

CREATE TABLE "roles" ("columna_1" TEXT);

CREATE TABLE "pais" ("columna_1" TEXT);

CREATE TABLE "personas" ("columna_1" TEXT);

CREATE TABLE tbcargo (
	idcargo BIGINT, 
	nombre TEXT, 
	estado BIGINT
);

INSERT INTO `tbcargo` (`idcargo`, `nombre`, `estado`) VALUES (1, 'ADMINISTRADOR', 1);
INSERT INTO `tbcargo` (`idcargo`, `nombre`, `estado`) VALUES (2, 'VENDEDOR', 1);
INSERT INTO `tbcargo` (`idcargo`, `nombre`, `estado`) VALUES (3, 'ALMACENERO', 1);

CREATE TABLE tbcategoria (
	idcategoria BIGINT, 
	nombre TEXT, 
	descripcion TEXT, 
	estado BIGINT, 
	fecha_creacion TEXT
);

INSERT INTO `tbcategoria` (`idcategoria`, `nombre`, `descripcion`, `estado`, `fecha_creacion`) VALUES (1, 'Electrónicos', 'Productos electrónicos y tecnológicos', 1, '2025-10-21 07:32:14');
INSERT INTO `tbcategoria` (`idcategoria`, `nombre`, `descripcion`, `estado`, `fecha_creacion`) VALUES (2, 'Ropa', 'Prendas de vestir para hombres, mujeres y niños', 1, '2025-10-21 07:32:14');
INSERT INTO `tbcategoria` (`idcategoria`, `nombre`, `descripcion`, `estado`, `fecha_creacion`) VALUES (3, 'Hogar', 'Artículos para el hogar y decoración', 1, '2025-10-21 07:32:14');
INSERT INTO `tbcategoria` (`idcategoria`, `nombre`, `descripcion`, `estado`, `fecha_creacion`) VALUES (4, 'Deportes', 'Equipos y artículos deportivos', 1, '2025-10-21 07:32:14');

CREATE TABLE tbcliente (
	idcliente BIGINT, 
	dni TEXT, 
	nombre TEXT, 
	apellido TEXT, 
	email TEXT, 
	telefono TEXT, 
	direccion TEXT, 
	clave TEXT, 
	estado BIGINT, 
	fecha_registro TEXT
);

INSERT INTO `tbcliente` (`idcliente`, `dni`, `nombre`, `apellido`, `email`, `telefono`, `direccion`, `clave`, `estado`, `fecha_registro`) VALUES (1, '11111111', 'Alberto', 'Mamani', 'albertomamani@gmail.com', '963852741', 'Av. Callao', 'cliente123', 1, '2025-10-19 17:51:29');

CREATE TABLE tbproducto (
	idproducto BIGINT, 
	codigo TEXT, 
	nombre TEXT, 
	descripcion TEXT, 
	precio FLOAT, 
	stock BIGINT, 
	idcategoria BIGINT, 
	imagen TEXT, 
	estado BIGINT, 
	fecha_creacion TEXT
);

INSERT INTO `tbproducto` (`idproducto`, `codigo`, `nombre`, `descripcion`, `precio`, `stock`, `idcategoria`, `imagen`, `estado`, `fecha_creacion`) VALUES (1, 'PROD001', 'Laptop HP 15"', 'Laptop HP 15.6 pulgadas, 8GB RAM, 256GB SSD', 1200.0, 10, 1, 'laptop.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO `tbproducto` (`idproducto`, `codigo`, `nombre`, `descripcion`, `precio`, `stock`, `idcategoria`, `imagen`, `estado`, `fecha_creacion`) VALUES (2, 'PROD002', 'Smartphone Samsung', 'Smartphone Samsung Galaxy A54 128GB', 450.0, 25, 1, 'smartphone.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO `tbproducto` (`idproducto`, `codigo`, `nombre`, `descripcion`, `precio`, `stock`, `idcategoria`, `imagen`, `estado`, `fecha_creacion`) VALUES (3, 'PROD003', 'Camiseta Nike', 'Camiseta deportiva Nike Dri-FIT', 35.0, 50, 2, 'camiseta.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO `tbproducto` (`idproducto`, `codigo`, `nombre`, `descripcion`, `precio`, `stock`, `idcategoria`, `imagen`, `estado`, `fecha_creacion`) VALUES (4, 'PROD004', 'Zapatos Deportivos', 'Zapatos deportivos Adidas Running', 85.0, 30, 2, 'zapatos.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO `tbproducto` (`idproducto`, `codigo`, `nombre`, `descripcion`, `precio`, `stock`, `idcategoria`, `imagen`, `estado`, `fecha_creacion`) VALUES (5, 'PROD005', 'Sofá 3 Plazas', 'Sofá moderno 3 plazas color gris', 650.0, 5, 3, 'sofa.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO `tbproducto` (`idproducto`, `codigo`, `nombre`, `descripcion`, `precio`, `stock`, `idcategoria`, `imagen`, `estado`, `fecha_creacion`) VALUES (6, 'PROD006', 'Pelota de Fútbol', 'Pelota de fútbol profesional tamaño 5', 25.0, 40, 4, 'pelota.jpg', 1, '2025-10-21 07:32:14');

CREATE TABLE tbusuario (
	idusuario BIGINT, 
	dni TEXT, 
	nombre TEXT, 
	apellido TEXT, 
	email TEXT, 
	telefono TEXT, 
	direccion TEXT, 
	usuario TEXT, 
	clave TEXT, 
	idcargo BIGINT, 
	estado BIGINT, 
	fecha_registro TEXT
);

INSERT INTO `tbusuario` (`idusuario`, `dni`, `nombre`, `apellido`, `email`, `telefono`, `direccion`, `usuario`, `clave`, `idcargo`, `estado`, `fecha_registro`) VALUES (1, '12345678', 'Juan', 'Pérez', 'juan@dbtienda.com', '987654321', 'Av. Principal 123', 'admin', 'admin123', 1, 1, '2025-10-21 07:32:14');
INSERT INTO `tbusuario` (`idusuario`, `dni`, `nombre`, `apellido`, `email`, `telefono`, `direccion`, `usuario`, `clave`, `idcargo`, `estado`, `fecha_registro`) VALUES (2, '87654321', 'María', 'Gómez', 'maria@dbtienda.com', '987654322', 'Av. Secundaria 456', 'vendedor', 'vendedor123', 2, 1, '2025-10-21 07:32:14');
INSERT INTO `tbusuario` (`idusuario`, `dni`, `nombre`, `apellido`, `email`, `telefono`, `direccion`, `usuario`, `clave`, `idcargo`, `estado`, `fecha_registro`) VALUES (3, '11223344', 'Carlos', 'López', 'carlos@dbtienda.com', '987654323', 'Calle Los Olivos 789', 'almacen', 'almacen123', 3, 1, '2025-10-21 07:32:14');

