-- --------------------------------------------------------
-- Host:                         127.0.0.1
-- Versión del servidor:         10.4.32-MariaDB - mariadb.org binary distribution
-- SO del servidor:              Win64
-- HeidiSQL Versión:             12.11.0.7065
-- --------------------------------------------------------

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8 */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


-- Volcando estructura de base de datos para dbtienda
CREATE DATABASE IF NOT EXISTS `dbtienda` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */;
USE `dbtienda`;

-- Volcando estructura para tabla dbtienda.tbcargo
CREATE TABLE IF NOT EXISTS `tbcargo` (
  `idcargo` int(11) NOT NULL AUTO_INCREMENT,
  `nombre` varchar(50) DEFAULT NULL,
  `estado` bit(1) DEFAULT b'1',
  PRIMARY KEY (`idcargo`),
  UNIQUE KEY `idcargo` (`idcargo`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Volcando datos para la tabla dbtienda.tbcargo: ~3 rows (aproximadamente)
INSERT INTO `tbcargo` (`idcargo`, `nombre`, `estado`) VALUES
	(1, 'ADMINISTRADOR', b'1'),
	(2, 'VENDEDOR', b'1'),
	(3, 'ALMACENERO', b'1');

-- Volcando estructura para tabla dbtienda.tbcategoria
CREATE TABLE IF NOT EXISTS `tbcategoria` (
  `idcategoria` int(11) NOT NULL AUTO_INCREMENT,
  `nombre` varchar(100) NOT NULL,
  `descripcion` text DEFAULT NULL,
  `estado` bit(1) DEFAULT b'1',
  `fecha_creacion` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`idcategoria`),
  UNIQUE KEY `nombre_unique` (`nombre`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Volcando datos para la tabla dbtienda.tbcategoria: ~4 rows (aproximadamente)
INSERT INTO `tbcategoria` (`idcategoria`, `nombre`, `descripcion`, `estado`, `fecha_creacion`) VALUES
	(1, 'Electrónicos', 'Productos electrónicos y tecnológicos', b'1', '2025-10-21 07:32:14'),
	(2, 'Ropa', 'Prendas de vestir para hombres, mujeres y niños', b'1', '2025-10-21 07:32:14'),
	(3, 'Hogar', 'Artículos para el hogar y decoración', b'1', '2025-10-21 07:32:14'),
	(4, 'Deportes', 'Equipos y artículos deportivos', b'1', '2025-10-21 07:32:14');

-- Volcando estructura para tabla dbtienda.tbcliente
CREATE TABLE IF NOT EXISTS `tbcliente` (
  `idcliente` int(11) NOT NULL AUTO_INCREMENT,
  `dni` varchar(20) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `apellido` varchar(100) NOT NULL,
  `email` varchar(150) DEFAULT NULL,
  `telefono` varchar(20) DEFAULT NULL,
  `direccion` text DEFAULT NULL,
  `clave` varchar(255) DEFAULT NULL,
  `estado` bit(1) DEFAULT b'1',
  `fecha_registro` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`idcliente`),
  UNIQUE KEY `dni_unique` (`dni`),
  UNIQUE KEY `email_unique` (`email`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Volcando datos para la tabla dbtienda.tbcliente: ~1 rows (aproximadamente)
INSERT INTO `tbcliente` (`idcliente`, `dni`, `nombre`, `apellido`, `email`, `telefono`, `direccion`, `clave`, `estado`, `fecha_registro`) VALUES
	(1, '11111111', 'Alberto', 'Mamani', 'albertomamani@gmail.com', '963852741', 'Av. Callao', 'cliente123', b'1', '2025-10-19 17:51:29');

-- Volcando estructura para tabla dbtienda.tbdetalle_venta
CREATE TABLE IF NOT EXISTS `tbdetalle_venta` (
  `iddetalle` int(11) NOT NULL AUTO_INCREMENT,
  `idventa` int(11) NOT NULL,
  `idproducto` int(11) NOT NULL,
  `cantidad` int(11) NOT NULL,
  `precio_unitario` decimal(10,2) NOT NULL,
  `subtotal` decimal(10,2) NOT NULL,
  PRIMARY KEY (`iddetalle`),
  KEY `fk_detalle_venta` (`idventa`),
  KEY `fk_detalle_producto` (`idproducto`),
  CONSTRAINT `fk_detalle_producto` FOREIGN KEY (`idproducto`) REFERENCES `tbproducto` (`idproducto`),
  CONSTRAINT `fk_detalle_venta` FOREIGN KEY (`idventa`) REFERENCES `tbventa` (`idventa`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Volcando datos para la tabla dbtienda.tbdetalle_venta: ~0 rows (aproximadamente)

-- Volcando estructura para tabla dbtienda.tbproducto
CREATE TABLE IF NOT EXISTS `tbproducto` (
  `idproducto` int(11) NOT NULL AUTO_INCREMENT,
  `codigo` varchar(50) NOT NULL,
  `nombre` varchar(250) NOT NULL,
  `descripcion` text DEFAULT NULL,
  `precio` decimal(10,2) NOT NULL DEFAULT 0.00,
  `stock` int(11) NOT NULL DEFAULT 0,
  `idcategoria` int(11) NOT NULL,
  `imagen` varchar(255) DEFAULT NULL,
  `estado` bit(1) DEFAULT b'1',
  `fecha_creacion` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`idproducto`),
  UNIQUE KEY `codigo_unique` (`codigo`),
  KEY `fk_producto_categoria` (`idcategoria`),
  CONSTRAINT `fk_producto_categoria` FOREIGN KEY (`idcategoria`) REFERENCES `tbcategoria` (`idcategoria`)
) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Volcando datos para la tabla dbtienda.tbproducto: ~6 rows (aproximadamente)
INSERT INTO `tbproducto` (`idproducto`, `codigo`, `nombre`, `descripcion`, `precio`, `stock`, `idcategoria`, `imagen`, `estado`, `fecha_creacion`) VALUES
	(1, 'PROD001', 'Laptop HP 15"', 'Laptop HP 15.6 pulgadas, 8GB RAM, 256GB SSD', 1200.00, 10, 1, 'laptop.jpg', b'1', '2025-10-21 07:32:14'),
	(2, 'PROD002', 'Smartphone Samsung', 'Smartphone Samsung Galaxy A54 128GB', 450.00, 25, 1, 'smartphone.jpg', b'1', '2025-10-21 07:32:14'),
	(3, 'PROD003', 'Camiseta Nike', 'Camiseta deportiva Nike Dri-FIT', 35.00, 50, 2, 'camiseta.jpg', b'1', '2025-10-21 07:32:14'),
	(4, 'PROD004', 'Zapatos Deportivos', 'Zapatos deportivos Adidas Running', 85.00, 30, 2, 'zapatos.jpg', b'1', '2025-10-21 07:32:14'),
	(5, 'PROD005', 'Sofá 3 Plazas', 'Sofá moderno 3 plazas color gris', 650.00, 5, 3, 'sofa.jpg', b'1', '2025-10-21 07:32:14'),
	(6, 'PROD006', 'Pelota de Fútbol', 'Pelota de fútbol profesional tamaño 5', 25.00, 40, 4, 'pelota.jpg', b'1', '2025-10-21 07:32:14');

-- Volcando estructura para tabla dbtienda.tbusuario
CREATE TABLE IF NOT EXISTS `tbusuario` (
  `idusuario` int(11) NOT NULL AUTO_INCREMENT,
  `dni` varchar(20) NOT NULL,
  `nombre` varchar(100) NOT NULL,
  `apellido` varchar(100) NOT NULL,
  `email` varchar(150) NOT NULL,
  `telefono` varchar(20) DEFAULT NULL,
  `direccion` text DEFAULT NULL,
  `usuario` varchar(50) NOT NULL,
  `clave` varchar(255) NOT NULL,
  `idcargo` int(11) NOT NULL,
  `estado` bit(1) DEFAULT b'1',
  `fecha_registro` datetime DEFAULT current_timestamp(),
  PRIMARY KEY (`idusuario`),
  UNIQUE KEY `dni_unique` (`dni`),
  UNIQUE KEY `email_unique` (`email`),
  UNIQUE KEY `usuario_unique` (`usuario`),
  KEY `fk_usuario_cargo` (`idcargo`),
  CONSTRAINT `fk_usuario_cargo` FOREIGN KEY (`idcargo`) REFERENCES `tbcargo` (`idcargo`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Volcando datos para la tabla dbtienda.tbusuario: ~3 rows (aproximadamente)
INSERT INTO `tbusuario` (`idusuario`, `dni`, `nombre`, `apellido`, `email`, `telefono`, `direccion`, `usuario`, `clave`, `idcargo`, `estado`, `fecha_registro`) VALUES
	(1, '12345678', 'Juan', 'Pérez', 'juan@dbtienda.com', '987654321', 'Av. Principal 123', 'admin', 'admin123', 1, b'1', '2025-10-21 07:32:14'),
	(2, '87654321', 'María', 'Gómez', 'maria@dbtienda.com', '987654322', 'Av. Secundaria 456', 'vendedor', 'vendedor123', 2, b'1', '2025-10-21 07:32:14'),
	(3, '11223344', 'Carlos', 'López', 'carlos@dbtienda.com', '987654323', 'Calle Los Olivos 789', 'almacen', 'almacen123', 3, b'1', '2025-10-21 07:32:14');

-- Volcando estructura para tabla dbtienda.tbventa
CREATE TABLE IF NOT EXISTS `tbventa` (
  `idventa` int(11) NOT NULL AUTO_INCREMENT,
  `idcliente` int(11) NOT NULL,
  `idusuario` int(11) NOT NULL,
  `fecha_venta` datetime DEFAULT current_timestamp(),
  `total` decimal(10,2) NOT NULL DEFAULT 0.00,
  `estado` varchar(20) DEFAULT 'PENDIENTE',
  `metodo_pago` varchar(50) DEFAULT NULL,
  `observaciones` text DEFAULT NULL,
  PRIMARY KEY (`idventa`),
  KEY `fk_venta_cliente` (`idcliente`),
  KEY `fk_venta_usuario` (`idusuario`),
  CONSTRAINT `fk_venta_cliente` FOREIGN KEY (`idcliente`) REFERENCES `tbcliente` (`idcliente`),
  CONSTRAINT `fk_venta_usuario` FOREIGN KEY (`idusuario`) REFERENCES `tbusuario` (`idusuario`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Volcando datos para la tabla dbtienda.tbventa: ~0 rows (aproximadamente)

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
