-- SQL Export generado por MigradorBD
-- Fecha: 2026-05-06 23:49:07.783852
-- Motor destino: postgresql

CREATE TABLE IF NOT EXISTS "public"."tbcargo" ("idcargo" INTEGER, "nombre" TEXT, "estado" INTEGER);
INSERT INTO "public"."tbcargo" ("idcargo", "nombre", "estado") VALUES (1, 'ADMINISTRADOR', 1);
INSERT INTO "public"."tbcargo" ("idcargo", "nombre", "estado") VALUES (2, 'VENDEDOR', 1);
INSERT INTO "public"."tbcargo" ("idcargo", "nombre", "estado") VALUES (3, 'ALMACENERO', 1);

CREATE TABLE IF NOT EXISTS "public"."tbcategoria" ("idcategoria" INTEGER, "nombre" TEXT, "descripcion" TEXT, "estado" INTEGER, "fecha_creacion" TEXT);
INSERT INTO "public"."tbcategoria" ("idcategoria", "nombre", "descripcion", "estado", "fecha_creacion") VALUES (1, 'Electrónicos', 'Productos electrónicos y tecnológicos', 1, '2025-10-21 07:32:14');
INSERT INTO "public"."tbcategoria" ("idcategoria", "nombre", "descripcion", "estado", "fecha_creacion") VALUES (2, 'Ropa', 'Prendas de vestir para hombres, mujeres y niños', 1, '2025-10-21 07:32:14');
INSERT INTO "public"."tbcategoria" ("idcategoria", "nombre", "descripcion", "estado", "fecha_creacion") VALUES (3, 'Hogar', 'Artículos para el hogar y decoración', 1, '2025-10-21 07:32:14');
INSERT INTO "public"."tbcategoria" ("idcategoria", "nombre", "descripcion", "estado", "fecha_creacion") VALUES (4, 'Deportes', 'Equipos y artículos deportivos', 1, '2025-10-21 07:32:14');

CREATE TABLE IF NOT EXISTS "public"."tbcliente" ("idcliente" INTEGER, "dni" TEXT, "nombre" TEXT, "apellido" TEXT, "email" TEXT, "telefono" TEXT, "direccion" TEXT, "clave" TEXT, "estado" INTEGER, "fecha_registro" TEXT);
INSERT INTO "public"."tbcliente" ("idcliente", "dni", "nombre", "apellido", "email", "telefono", "direccion", "clave", "estado", "fecha_registro") VALUES (1, '11111111', 'Alberto', 'Mamani', 'albertomamani@gmail.com', '963852741', 'Av. Callao', 'cliente123', 1, '2025-10-19 17:51:29');

CREATE TABLE IF NOT EXISTS "public"."tbproducto" ("idproducto" INTEGER, "codigo" TEXT, "nombre" TEXT, "descripcion" TEXT, "precio" DOUBLE PRECISION, "stock" INTEGER, "idcategoria" INTEGER, "imagen" TEXT, "estado" INTEGER, "fecha_creacion" TEXT);
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES (1, 'PROD001', 'Laptop HP 15"', 'Laptop HP 15.6 pulgadas, 8GB RAM, 256GB SSD', 1200.0, 10, 1, 'laptop.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES (2, 'PROD002', 'Smartphone Samsung', 'Smartphone Samsung Galaxy A54 128GB', 450.0, 25, 1, 'smartphone.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES (3, 'PROD003', 'Camiseta Nike', 'Camiseta deportiva Nike Dri-FIT', 35.0, 50, 2, 'camiseta.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES (4, 'PROD004', 'Zapatos Deportivos', 'Zapatos deportivos Adidas Running', 85.0, 30, 2, 'zapatos.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES (5, 'PROD005', 'Sofá 3 Plazas', 'Sofá moderno 3 plazas color gris', 650.0, 5, 3, 'sofa.jpg', 1, '2025-10-21 07:32:14');
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES (6, 'PROD006', 'Pelota de Fútbol', 'Pelota de fútbol profesional tamaño 5', 25.0, 40, 4, 'pelota.jpg', 1, '2025-10-21 07:32:14');

CREATE TABLE IF NOT EXISTS "public"."tbusuario" ("idusuario" INTEGER, "dni" TEXT, "nombre" TEXT, "apellido" TEXT, "email" TEXT, "telefono" TEXT, "direccion" TEXT, "usuario" TEXT, "clave" TEXT, "idcargo" INTEGER, "estado" INTEGER, "fecha_registro" TEXT);
INSERT INTO "public"."tbusuario" ("idusuario", "dni", "nombre", "apellido", "email", "telefono", "direccion", "usuario", "clave", "idcargo", "estado", "fecha_registro") VALUES (1, '12345678', 'Juan', 'Pérez', 'juan@dbtienda.com', '987654321', 'Av. Principal 123', 'admin', 'admin123', 1, 1, '2025-10-21 07:32:14');
INSERT INTO "public"."tbusuario" ("idusuario", "dni", "nombre", "apellido", "email", "telefono", "direccion", "usuario", "clave", "idcargo", "estado", "fecha_registro") VALUES (2, '87654321', 'María', 'Gómez', 'maria@dbtienda.com', '987654322', 'Av. Secundaria 456', 'vendedor', 'vendedor123', 2, 1, '2025-10-21 07:32:14');
INSERT INTO "public"."tbusuario" ("idusuario", "dni", "nombre", "apellido", "email", "telefono", "direccion", "usuario", "clave", "idcargo", "estado", "fecha_registro") VALUES (3, '11223344', 'Carlos', 'López', 'carlos@dbtienda.com', '987654323', 'Calle Los Olivos 789', 'almacen', 'almacen123', 3, 1, '2025-10-21 07:32:14');

CREATE TABLE IF NOT EXISTS "public"."public_roles" ("nombre_rol" TEXT);

CREATE TABLE IF NOT EXISTS "public"."public_usuarios" ("usuario" TEXT, "contraseña" TEXT, "id_rol" TEXT);

CREATE TABLE IF NOT EXISTS "public"."roles" ("nombre_rol" TEXT);
INSERT INTO "public"."roles" ("nombre_rol") VALUES ('Administrador');
INSERT INTO "public"."roles" ("nombre_rol") VALUES ('Asistente');
INSERT INTO "public"."roles" ("nombre_rol") VALUES ('Participante');

CREATE TABLE IF NOT EXISTS "public"."usuarios" ("usuario" TEXT, "contraseña" TEXT, "id_rol" TEXT);
INSERT INTO "public"."usuarios" ("usuario", "contraseña", "id_rol") VALUES ('p_usuario', 'p_contraseña', 'p_id_rol');

CREATE TABLE IF NOT EXISTS "public"."Inventario_Productos" ("id" TEXT, "nombre" TEXT, "cantidad" TEXT, "precio" TEXT);
INSERT INTO "public"."Inventario_Productos" ("id", "nombre", "cantidad", "precio") VALUES ('1', 'Producto A', '100', '15.5');
INSERT INTO "public"."Inventario_Productos" ("id", "nombre", "cantidad", "precio") VALUES ('2', 'Producto B', '200', '25.75');
INSERT INTO "public"."Inventario_Productos" ("id", "nombre", "cantidad", "precio") VALUES ('3', 'Producto C', '150', '10.25');
INSERT INTO "public"."Inventario_Productos" ("id", "nombre", "cantidad", "precio") VALUES ('1', 'Producto A', '100', '15.5');
INSERT INTO "public"."Inventario_Productos" ("id", "nombre", "cantidad", "precio") VALUES ('2', 'Producto B', '200', '25.75');
INSERT INTO "public"."Inventario_Productos" ("id", "nombre", "cantidad", "precio") VALUES ('3', 'Producto C', '150', '10.25');

CREATE TABLE IF NOT EXISTS "public"."Inventario_MovimientosInventario" ("id" TEXT, "producto_id" TEXT, "tipo_movimiento" TEXT, "cantidad" TEXT, "fecha" TEXT);
INSERT INTO "public"."Inventario_MovimientosInventario" ("id", "producto_id", "tipo_movimiento", "cantidad", "fecha") VALUES ('1', '1', 'Entrada', '50', '2026-05-01');
INSERT INTO "public"."Inventario_MovimientosInventario" ("id", "producto_id", "tipo_movimiento", "cantidad", "fecha") VALUES ('2', '2', 'Salida', '10', '2026-05-02');
INSERT INTO "public"."Inventario_MovimientosInventario" ("id", "producto_id", "tipo_movimiento", "cantidad", "fecha") VALUES ('3', '3', 'Entrada', '30', '2026-05-03');
INSERT INTO "public"."Inventario_MovimientosInventario" ("id", "producto_id", "tipo_movimiento", "cantidad", "fecha") VALUES ('1', '1', 'Entrada', '50', '2026-05-01');
INSERT INTO "public"."Inventario_MovimientosInventario" ("id", "producto_id", "tipo_movimiento", "cantidad", "fecha") VALUES ('2', '2', 'Salida', '10', '2026-05-02');
INSERT INTO "public"."Inventario_MovimientosInventario" ("id", "producto_id", "tipo_movimiento", "cantidad", "fecha") VALUES ('3', '3', 'Entrada', '30', '2026-05-03');

CREATE TABLE IF NOT EXISTS "public"."Ventas_Clientes" ("id" TEXT, "nombre" TEXT, "email" TEXT);
INSERT INTO "public"."Ventas_Clientes" ("id", "nombre", "email") VALUES ('1', 'Juan Pérez', 'juan@example.com');
INSERT INTO "public"."Ventas_Clientes" ("id", "nombre", "email") VALUES ('2', 'María González', 'maria@example.com');
INSERT INTO "public"."Ventas_Clientes" ("id", "nombre", "email") VALUES ('1', 'Juan Pérez', 'juan@example.com');
INSERT INTO "public"."Ventas_Clientes" ("id", "nombre", "email") VALUES ('2', 'María González', 'maria@example.com');

CREATE TABLE IF NOT EXISTS "public"."Ventas_Ordenes" ("id" TEXT, "cliente_id" TEXT, "fecha" TEXT, "total" TEXT);
INSERT INTO "public"."Ventas_Ordenes" ("id", "cliente_id", "fecha", "total") VALUES ('1', '1', '2026-05-01', '100.0');
INSERT INTO "public"."Ventas_Ordenes" ("id", "cliente_id", "fecha", "total") VALUES ('2', '2', '2026-05-02', '250.5');
INSERT INTO "public"."Ventas_Ordenes" ("id", "cliente_id", "fecha", "total") VALUES ('1', '1', '2026-05-01', '100.0');
INSERT INTO "public"."Ventas_Ordenes" ("id", "cliente_id", "fecha", "total") VALUES ('2', '2', '2026-05-02', '250.5');

CREATE TABLE IF NOT EXISTS "public"."tbcargo" ("idcargo" TEXT, "nombre" TEXT, "estado" TEXT);
INSERT INTO "public"."tbcargo" ("idcargo", "nombre", "estado") VALUES ('1', 'ADMINISTRADOR', '1');
INSERT INTO "public"."tbcargo" ("idcargo", "nombre", "estado") VALUES ('2', 'VENDEDOR', '1');
INSERT INTO "public"."tbcargo" ("idcargo", "nombre", "estado") VALUES ('3', 'ALMACENERO', '1');

CREATE TABLE IF NOT EXISTS "public"."tbcategoria" ("idcategoria" TEXT, "nombre" TEXT, "descripcion" TEXT, "estado" TEXT, "fecha_creacion" TEXT);
INSERT INTO "public"."tbcategoria" ("idcategoria", "nombre", "descripcion", "estado", "fecha_creacion") VALUES ('1', 'Electrónicos', 'Productos electrónicos y tecnológicos', '1', '2025-10-21 07:32:14');
INSERT INTO "public"."tbcategoria" ("idcategoria", "nombre", "descripcion", "estado", "fecha_creacion") VALUES ('2', 'Ropa', 'Prendas de vestir para hombres, mujeres y niños', '1', '2025-10-21 07:32:14');
INSERT INTO "public"."tbcategoria" ("idcategoria", "nombre", "descripcion", "estado", "fecha_creacion") VALUES ('3', 'Hogar', 'Artículos para el hogar y decoración', '1', '2025-10-21 07:32:14');
INSERT INTO "public"."tbcategoria" ("idcategoria", "nombre", "descripcion", "estado", "fecha_creacion") VALUES ('4', 'Deportes', 'Equipos y artículos deportivos', '1', '2025-10-21 07:32:14');

CREATE TABLE IF NOT EXISTS "public"."tbcliente" ("idcliente" TEXT, "dni" TEXT, "nombre" TEXT, "apellido" TEXT, "email" TEXT, "telefono" TEXT, "direccion" TEXT, "clave" TEXT, "estado" TEXT, "fecha_registro" TEXT);
INSERT INTO "public"."tbcliente" ("idcliente", "dni", "nombre", "apellido", "email", "telefono", "direccion", "clave", "estado", "fecha_registro") VALUES ('1', '11111111', 'Alberto', 'Mamani', 'albertomamani@gmail.com', '963852741', 'Av. Callao', 'cliente123', '1', '2025-10-19 17:51:29');

CREATE TABLE IF NOT EXISTS "public"."tbproducto" ("idproducto" TEXT, "codigo" TEXT, "nombre" TEXT, "descripcion" TEXT, "precio" TEXT, "stock" TEXT, "idcategoria" TEXT, "imagen" TEXT, "estado" TEXT, "fecha_creacion" TEXT);
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES ('1', 'PROD001', 'Laptop HP 15"', 'Laptop HP 15.6 pulgadas, 8GB RAM, 256GB SSD', '1200.0', '10', '1', 'laptop.jpg', '1', '2025-10-21 07:32:14');
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES ('2', 'PROD002', 'Smartphone Samsung', 'Smartphone Samsung Galaxy A54 128GB', '450.0', '25', '1', 'smartphone.jpg', '1', '2025-10-21 07:32:14');
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES ('3', 'PROD003', 'Camiseta Nike', 'Camiseta deportiva Nike Dri-FIT', '35.0', '50', '2', 'camiseta.jpg', '1', '2025-10-21 07:32:14');
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES ('4', 'PROD004', 'Zapatos Deportivos', 'Zapatos deportivos Adidas Running', '85.0', '30', '2', 'zapatos.jpg', '1', '2025-10-21 07:32:14');
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES ('5', 'PROD005', 'Sofá 3 Plazas', 'Sofá moderno 3 plazas color gris', '650.0', '5', '3', 'sofa.jpg', '1', '2025-10-21 07:32:14');
INSERT INTO "public"."tbproducto" ("idproducto", "codigo", "nombre", "descripcion", "precio", "stock", "idcategoria", "imagen", "estado", "fecha_creacion") VALUES ('6', 'PROD006', 'Pelota de Fútbol', 'Pelota de fútbol profesional tamaño 5', '25.0', '40', '4', 'pelota.jpg', '1', '2025-10-21 07:32:14');

CREATE TABLE IF NOT EXISTS "public"."tbusuario" ("idusuario" TEXT, "dni" TEXT, "nombre" TEXT, "apellido" TEXT, "email" TEXT, "telefono" TEXT, "direccion" TEXT, "usuario" TEXT, "clave" TEXT, "idcargo" TEXT, "estado" TEXT, "fecha_registro" TEXT);
INSERT INTO "public"."tbusuario" ("idusuario", "dni", "nombre", "apellido", "email", "telefono", "direccion", "usuario", "clave", "idcargo", "estado", "fecha_registro") VALUES ('1', '12345678', 'Juan', 'Pérez', 'juan@dbtienda.com', '987654321', 'Av. Principal 123', 'admin', 'admin123', '1', '1', '2025-10-21 07:32:14');
INSERT INTO "public"."tbusuario" ("idusuario", "dni", "nombre", "apellido", "email", "telefono", "direccion", "usuario", "clave", "idcargo", "estado", "fecha_registro") VALUES ('2', '87654321', 'María', 'Gómez', 'maria@dbtienda.com', '987654322', 'Av. Secundaria 456', 'vendedor', 'vendedor123', '2', '1', '2025-10-21 07:32:14');
INSERT INTO "public"."tbusuario" ("idusuario", "dni", "nombre", "apellido", "email", "telefono", "direccion", "usuario", "clave", "idcargo", "estado", "fecha_registro") VALUES ('3', '11223344', 'Carlos', 'López', 'carlos@dbtienda.com', '987654323', 'Calle Los Olivos 789', 'almacen', 'almacen123', '3', '1', '2025-10-21 07:32:14');
