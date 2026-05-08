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


-- Volcando estructura de base de datos para pasarela_pagos
CREATE DATABASE IF NOT EXISTS `pasarela_pagos` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci */;
USE `pasarela_pagos`;

-- Volcando estructura para tabla pasarela_pagos.clientes
CREATE TABLE IF NOT EXISTS `clientes` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `email` varchar(255) NOT NULL,
  `nombre` varchar(255) DEFAULT NULL,
  `id_cliente_stripe` varchar(255) DEFAULT NULL,
  `fecha_registro` timestamp NOT NULL DEFAULT current_timestamp(),
  `activo` tinyint(1) DEFAULT 1,
  PRIMARY KEY (`id`),
  UNIQUE KEY `email` (`email`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Volcando datos para la tabla pasarela_pagos.clientes: ~6 rows (aproximadamente)
INSERT INTO `clientes` (`id`, `email`, `nombre`, `id_cliente_stripe`, `fecha_registro`, `activo`) VALUES
	(4, 'fabrizio.27pp@gmail.com', 'Cliente', NULL, '2025-11-24 18:40:40', 1),
	(5, 'fp2023077476@virtual.upt.pe', 'Cliente', NULL, '2025-11-24 19:01:02', 1),
	(6, 'fabperez@upt.pe', 'Cliente', NULL, '2025-11-24 19:21:02', 1),
	(7, 'cliente@ejemplo.com', 'Cliente', NULL, '2025-11-25 15:07:12', 1),
	(8, 'usher.rojas01@gmail.com', 'Cliente', NULL, '2025-11-25 15:38:57', 1),
	(9, 'jl2023076789@virtual.upt.pe', 'Cliente', NULL, '2025-11-25 16:27:29', 1);

-- Volcando estructura para tabla pasarela_pagos.transacciones
CREATE TABLE IF NOT EXISTS `transacciones` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `id_transaccion` varchar(255) NOT NULL,
  `monto` decimal(10,2) NOT NULL,
  `moneda` varchar(10) DEFAULT 'USD',
  `descripcion` text DEFAULT NULL,
  `email_cliente` varchar(255) DEFAULT NULL,
  `estado` varchar(50) NOT NULL,
  `proveedor_pago` varchar(50) DEFAULT 'STRIPE',
  `id_pago_proveedor` varchar(255) DEFAULT NULL,
  `metadatos` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`metadatos`)),
  `fecha_creacion` timestamp NOT NULL DEFAULT current_timestamp(),
  `fecha_actualizacion` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `telefono_cliente` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_transaccion` (`id_transaccion`),
  KEY `idx_estado` (`estado`),
  KEY `idx_fecha_creacion` (`fecha_creacion`),
  KEY `idx_email_cliente` (`email_cliente`)
) ENGINE=InnoDB AUTO_INCREMENT=63 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Volcando datos para la tabla pasarela_pagos.transacciones: ~51 rows (aproximadamente)
INSERT INTO `transacciones` (`id`, `id_transaccion`, `monto`, `moneda`, `descripcion`, `email_cliente`, `estado`, `proveedor_pago`, `id_pago_proveedor`, `metadatos`, `fecha_creacion`, `fecha_actualizacion`, `telefono_cliente`) VALUES
	(7, 'pi_5e7ad992-1411-', 50.00, 'USD', 'CATEC', 'fabrizio.27pp@gmail.com', 'COMPLETADO', 'SIMULADO', 'pi_5e7ad992-1411-', NULL, '2025-11-24 18:40:40', '2025-11-24 18:40:40', NULL),
	(8, 'pi_fea3d77e-283a-', 25.00, 'PEN', 'CATEC 2', 'fabrizio.27pp@gmail.com', 'COMPLETADO', 'SIMULADO', 'pi_fea3d77e-283a-', NULL, '2025-11-24 18:53:57', '2025-11-24 18:53:57', NULL),
	(9, 'pi_bfb4694f-ae2a-', 25.00, 'PEN', 'CATEC 4', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_bfb4694f-ae2a-', NULL, '2025-11-24 19:01:02', '2025-11-24 19:01:02', NULL),
	(10, 'pi_0665d7d1-40ac-', 50.00, 'PEN', 'catec 5', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_0665d7d1-40ac-', NULL, '2025-11-24 19:09:21', '2025-11-24 19:09:21', NULL),
	(11, 'pi_eb0b5acc-9416-', 25.00, 'PEN', 'DEVFEST', 'fabrizio.27pp@gmail.com', 'COMPLETADO', 'SIMULADO', 'pi_eb0b5acc-9416-', NULL, '2025-11-24 19:19:24', '2025-11-24 19:19:24', NULL),
	(12, 'pi_3490df53-52ea-', 25.00, 'PEN', 'CATEC 6', 'fabperez@upt.pe', 'FALLIDO', 'SIMULADO', 'pi_3490df53-52ea-', NULL, '2025-11-24 19:21:02', '2025-11-24 19:21:02', NULL),
	(13, 'pi_ceaabba2-4711-', 50.00, 'EUR', 'CATEC 6', 'fabperez@upt.pe', 'FALLIDO', 'SIMULADO', 'pi_ceaabba2-4711-', NULL, '2025-11-24 19:21:38', '2025-11-24 19:21:38', NULL),
	(14, 'pi_68c2e972-2980-', 50.00, 'USD', 'CATEC 6', 'fabperez@upt.pe', 'FALLIDO', 'SIMULADO', 'pi_68c2e972-2980-', NULL, '2025-11-24 19:23:23', '2025-11-24 19:23:23', NULL),
	(15, 'pi_d726e808-9159-', 50.00, 'PEN', 'Compra de producto', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_d726e808-9159-', NULL, '2025-11-24 19:25:36', '2025-11-24 19:25:36', NULL),
	(16, 'pi_dcdde465-eedc-', 25.00, 'PEN', 'CATEC 7', 'fabrizio.27pp@gmail.com', 'COMPLETADO', 'SIMULADO', 'pi_dcdde465-eedc-', NULL, '2025-11-25 03:41:55', '2025-11-25 03:41:55', NULL),
	(17, 'pi_3b2177a3-3440-', 25.00, 'PEN', 'CATEC 8', 'fabrizio.27pp@gmail.com', 'COMPLETADO', 'SIMULADO', 'pi_3b2177a3-3440-', NULL, '2025-11-25 03:50:32', '2025-11-25 03:50:32', NULL),
	(18, 'pi_9795e55a-38dc-', 50.00, 'PEN', 'CATEC 9', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_9795e55a-38dc-', NULL, '2025-11-25 03:51:58', '2025-11-25 03:51:58', NULL),
	(19, 'pi_52f5b5f0-8aec-', 50.00, 'PEN', 'CATEC 10', 'fabrizio.27pp@gmail.com', 'COMPLETADO', 'SIMULADO', 'pi_52f5b5f0-8aec-', NULL, '2025-11-25 04:03:46', '2025-11-25 04:03:46', NULL),
	(20, 'pi_f530bc06-bf19-', 25.00, 'PEN', 'Compra de producto', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_f530bc06-bf19-', NULL, '2025-11-25 04:08:13', '2025-11-25 04:08:13', NULL),
	(21, 'pi_13710427-2d4a-', 25.00, 'PEN', 'CATECC 11', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_13710427-2d4a-', NULL, '2025-11-25 04:20:20', '2025-11-25 04:20:20', NULL),
	(22, 'pi_d8248091-73ef-', 25.00, 'PEN', 'Compra de producto 2', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_d8248091-73ef-', NULL, '2025-11-25 04:26:23', '2025-11-25 04:26:23', NULL),
	(23, 'pi_93644614-d4c7-', 25.00, 'PEN', 'CATEC 12', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_93644614-d4c7-', NULL, '2025-11-25 15:06:31', '2025-11-25 15:06:31', '+51964501290'),
	(24, 'pi_c9e65713-b5e4-', 25.00, 'USD', 'CTAEC 13', 'cliente@ejemplo.com', 'COMPLETADO', 'SIMULADO', 'pi_c9e65713-b5e4-', NULL, '2025-11-25 15:07:12', '2025-11-25 15:07:12', '+51964501290'),
	(25, 'pi_68c4999e-f398-', 50.00, 'PEN', 'CATEC 14', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_68c4999e-f398-', NULL, '2025-11-25 15:10:32', '2025-11-25 15:10:32', '+51964501290'),
	(26, 'pi_fa0d8839-0087-', 50.00, 'PEN', 'CATEC 15', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_fa0d8839-0087-', NULL, '2025-11-25 15:13:24', '2025-11-25 15:13:24', '+51964501290'),
	(27, 'pi_dc2934f0-2932-', 50.00, 'PEN', 'fabrizio', 'fabrizio.27pp@gmail.com', 'COMPLETADO', 'SIMULADO', 'pi_dc2934f0-2932-', NULL, '2025-11-25 15:22:15', '2025-11-25 15:22:15', NULL),
	(28, 'pi_f22b11db-c306-', 50.00, 'PEN', 'CATEC 16', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_f22b11db-c306-', NULL, '2025-11-25 15:37:00', '2025-11-25 15:37:00', NULL),
	(29, 'pi_0339595a-f837-', 120.00, 'PEN', 'DEVFEST 4', 'usher.rojas01@gmail.com', 'COMPLETADO', 'SIMULADO', 'pi_0339595a-f837-', NULL, '2025-11-25 15:38:57', '2025-11-25 15:38:57', NULL),
	(30, 'pi_60cbeb52-8ed2-', 25.00, 'PEN', 'CTAEC 18', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_60cbeb52-8ed2-', NULL, '2025-11-25 15:47:01', '2025-11-25 15:47:01', NULL),
	(31, 'pi_b204cf6f-d77c-', 50.00, 'PEN', 'CATEC 19', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_b204cf6f-d77c-', NULL, '2025-11-25 15:47:32', '2025-11-25 15:47:32', NULL),
	(32, 'pi_011a683c-9634-', 50.00, 'PEN', 'CATEC 19', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_011a683c-9634-', NULL, '2025-11-25 15:50:48', '2025-11-25 15:50:48', NULL),
	(33, 'pi_2ea4a97a-1558-', 50.00, 'PEN', 'CATEC 18', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_2ea4a97a-1558-', NULL, '2025-11-25 15:51:20', '2025-11-25 15:51:20', NULL),
	(34, 'pi_7f4e21ea-65c2-', 50.00, 'PEN', 'CATEC 2026', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_7f4e21ea-65c2-', NULL, '2025-11-25 15:55:20', '2025-11-25 15:55:20', NULL),
	(35, 'pi_99be6c06-8ce2-', 50.00, 'PEN', 'CATEC 19', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_99be6c06-8ce2-', NULL, '2025-11-25 16:02:09', '2025-11-25 16:02:09', NULL),
	(36, 'pi_a06cef56-41b7-', 50.00, 'PEN', 'CATEC 18', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_a06cef56-41b7-', NULL, '2025-11-25 16:07:28', '2025-11-25 16:07:28', NULL),
	(37, 'pi_fe2954c3-fca1-', 120.00, 'PEN', 'CATEC 20', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_fe2954c3-fca1-', NULL, '2025-11-25 16:15:47', '2025-11-25 16:15:47', NULL),
	(38, 'pi_78674059-4c78-', 120.00, 'PEN', 'CATEC 26', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_78674059-4c78-', NULL, '2025-11-25 16:19:22', '2025-11-25 16:19:22', NULL),
	(39, 'pi_1896e324-f9d5-', 300.00, 'PEN', 'COMPRA USHER', 'usher.rojas01@gmail.com', 'COMPLETADO', 'SIMULADO', 'pi_1896e324-f9d5-', NULL, '2025-11-25 16:20:46', '2025-11-25 16:20:46', NULL),
	(40, 'pi_b69b2063-93ac-', 300.00, 'PEN', 'DEVFEST 56', 'jl2023076789@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_b69b2063-93ac-', NULL, '2025-11-25 16:27:29', '2025-11-25 16:27:29', NULL),
	(41, 'pi_5d362257-e7b3-', 300.00, 'PEN', 'CATEC 29', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_5d362257-e7b3-', NULL, '2025-11-25 16:34:47', '2025-11-25 16:34:47', NULL),
	(42, 'pi_ec136c8b-bf0a-', 300.00, 'EUR', 'CATEC 29', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_ec136c8b-bf0a-', NULL, '2025-11-25 16:44:28', '2025-11-25 16:44:28', NULL),
	(43, 'pi_9bba2d1f-918f-', 320.00, 'PEN', 'CATEC 30', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_9bba2d1f-918f-', NULL, '2025-11-25 16:47:54', '2025-11-25 16:47:54', NULL),
	(44, 'pi_ddbd518c-f5a6-', 320.00, 'PEN', 'CATEC 29', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_ddbd518c-f5a6-', NULL, '2025-11-25 16:50:40', '2025-11-25 16:50:40', NULL),
	(45, 'pi_dc1dbd09-c2de-', 320.00, 'PEN', 'CATEC 20', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_dc1dbd09-c2de-', NULL, '2025-11-25 16:53:11', '2025-11-25 16:53:11', NULL),
	(46, 'pi_8277473a-e91d-', 300.00, 'PEN', 'CATEC 23', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_8277473a-e91d-', NULL, '2025-11-25 17:03:26', '2025-11-25 17:03:26', NULL),
	(47, 'pi_8e1fd08d-7f37-', 25.00, 'PEN', 'Compra de producto', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_8e1fd08d-7f37-', NULL, '2025-11-25 17:04:29', '2025-11-25 17:04:29', NULL),
	(48, 'pi_2a9278dd-5350-', 320.00, 'PEN', 'CATEC 20', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_2a9278dd-5350-', NULL, '2025-11-25 17:08:52', '2025-11-25 17:08:52', NULL),
	(49, 'pi_4875b7bf-c62b-', 320.00, 'EUR', 'CATEC 25', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_4875b7bf-c62b-', NULL, '2025-11-25 17:20:09', '2025-11-25 17:20:09', NULL),
	(50, 'pi_51f05403-dc9d-', 320.00, 'PEN', 'DEVFEST 53', 'fabperez@upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_51f05403-dc9d-', NULL, '2025-11-25 17:22:25', '2025-11-25 17:22:25', NULL),
	(51, 'pi_bbf1df77-9648-', 320.00, 'PEN', 'PAGO JIMMY', 'jl2023076789@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_bbf1df77-9648-', NULL, '2025-11-25 17:24:42', '2025-11-25 17:24:42', NULL),
	(52, 'pi_8ac0d778-4732-', 320.00, 'EUR', 'PAGO JIMMY 2', 'jl2023076789@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_8ac0d778-4732-', NULL, '2025-11-25 17:29:50', '2025-11-25 17:29:50', NULL),
	(53, 'pi_87db08df-41de-', 320.00, 'USD', 'Compra de producto', 'jl2023076789@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_87db08df-41de-', NULL, '2025-11-25 17:33:58', '2025-11-25 17:33:58', NULL),
	(54, 'pi_33204bcb-d721-', 25.00, 'PEN', '4535243', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_33204bcb-d721-', NULL, '2025-11-25 17:50:55', '2025-11-25 17:50:55', NULL),
	(55, 'pi_24db69f5-2605-', 25.00, 'PEN', 'Compra de producto', 'cliente@ejemplo.com', 'COMPLETADO', 'SIMULADO', 'pi_24db69f5-2605-', NULL, '2025-11-25 18:00:31', '2025-11-25 18:00:31', NULL),
	(56, 'pi_3d502ab5-ae61-', 25.00, 'PEN', 'Compra de producto', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_3d502ab5-ae61-', NULL, '2025-11-25 18:02:34', '2025-11-25 18:02:34', NULL),
	(57, 'pi_c7633b88-8c0f-', 25.00, 'PEN', 'PRUEBA', 'fp2023077476@virtual.upt.pe', 'RECHAZADO', 'SIMULADO', 'pi_c7633b88-8c0f-', NULL, '2025-11-25 22:18:48', '2025-11-25 22:18:48', NULL),
	(58, 'pi_723fb0cc-c5fb-', 50.00, 'EUR', 'CATEC 89', 'fp2023077476@virtual.upt.pe', 'RECHAZADO', 'SIMULADO', 'pi_723fb0cc-c5fb-', NULL, '2025-11-26 17:20:21', '2025-11-26 17:20:21', NULL),
	(59, 'pi_bc9779ed-8fcc-', 25.00, 'PEN', 'PRUEBA 3', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_bc9779ed-8fcc-', NULL, '2025-11-26 17:22:53', '2025-11-26 17:22:53', NULL),
	(60, 'pi_c37fa506-b2c8-', 25.00, 'PEN', 'Compra de producto', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'SIMULADO', 'pi_c37fa506-b2c8-', NULL, '2025-11-26 17:26:40', '2025-11-26 17:26:40', NULL),
	(61, 'pi_5052ca11-a717-', 25.00, 'PEN', 'Compra de producto', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'TARJETA DE CRÉDITO', 'pi_5052ca11-a717-', NULL, '2025-11-26 17:32:46', '2025-11-26 17:32:46', NULL),
	(62, 'pi_8f806b5c-2f16-', 25.00, 'PEN', 'Compra de producto', 'fp2023077476@virtual.upt.pe', 'COMPLETADO', 'TARJETA DE CRÉDITO', 'pi_8f806b5c-2f16-', NULL, '2025-11-26 17:38:45', '2025-11-26 17:38:45', NULL);

-- Volcando estructura para tabla pasarela_pagos.webhook_logs
CREATE TABLE IF NOT EXISTS `webhook_logs` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `tipo_evento` varchar(255) NOT NULL,
  `id_evento` varchar(255) NOT NULL,
  `payload` text NOT NULL,
  `procesado` tinyint(1) DEFAULT 0,
  `fecha_recepcion` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `id_evento` (`id_evento`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- Volcando datos para la tabla pasarela_pagos.webhook_logs: ~0 rows (aproximadamente)

/*!40103 SET TIME_ZONE=IFNULL(@OLD_TIME_ZONE, 'system') */;
/*!40101 SET SQL_MODE=IFNULL(@OLD_SQL_MODE, '') */;
/*!40014 SET FOREIGN_KEY_CHECKS=IFNULL(@OLD_FOREIGN_KEY_CHECKS, 1) */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40111 SET SQL_NOTES=IFNULL(@OLD_SQL_NOTES, 1) */;
