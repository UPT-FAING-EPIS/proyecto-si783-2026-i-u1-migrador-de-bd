-- =============================================
-- CREACIÓN DE LA BASE DE DATOS Y ESQUEMAS
-- =============================================

CREATE DATABASE BD_U2_G3;
GO

USE BD_U2_G3;
GO

-- Creación de esquemas para organizar las tablas
CREATE SCHEMA Catalogo;
GO

CREATE SCHEMA Inventario;
GO

CREATE SCHEMA Compras;
GO

CREATE SCHEMA Auditoria;
GO

CREATE SCHEMA Seguridad;
GO

-- =============================================
-- CREACIÓN DE TABLAS BASE (CATÁLOGO)
-- =============================================

-- Tabla de Usuarios/Sistema
CREATE TABLE Seguridad.Usuarios (
    IdUsuario INT IDENTITY(1,1) PRIMARY KEY,
    Usuario VARCHAR(50) NOT NULL UNIQUE,
    NombreCompleto VARCHAR(150) NOT NULL,
    Email VARCHAR(100),
    Rol VARCHAR(50) NOT NULL,
    Estado BIT DEFAULT 1,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    FechaUltimoAcceso DATETIME NULL
);

-- Tabla de Categorías de Productos
CREATE TABLE Catalogo.Categorias (
    IdCategoria INT IDENTITY(1,1) PRIMARY KEY,
    CodigoCategoria VARCHAR(10) NOT NULL UNIQUE,
    Nombre VARCHAR(100) NOT NULL,
    Descripcion VARCHAR(255),
    Estado BIT DEFAULT 1,
    UsuarioCreacion INT NOT NULL,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    
    CONSTRAINT FK_Categorias_Usuarios FOREIGN KEY (UsuarioCreacion) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- Tabla de Unidades de Medida
CREATE TABLE Catalogo.UnidadesMedida (
    IdUnidadMedida INT IDENTITY(1,1) PRIMARY KEY,
    CodigoUnidad VARCHAR(10) NOT NULL UNIQUE,
    Nombre VARCHAR(50) NOT NULL,
    Abreviatura VARCHAR(10) NOT NULL,
    Estado BIT DEFAULT 1,
    UsuarioCreacion INT NOT NULL,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    
    CONSTRAINT FK_UnidadesMedida_Usuarios FOREIGN KEY (UsuarioCreacion) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- Tabla de Almacenes
CREATE TABLE Catalogo.Almacenes (
    IdAlmacen INT IDENTITY(1,1) PRIMARY KEY,
    CodigoAlmacen VARCHAR(10) NOT NULL UNIQUE,
    Nombre VARCHAR(100) NOT NULL,
    Direccion VARCHAR(255),
    Ubicacion VARCHAR(100),
    Responsable VARCHAR(100),
    Telefono VARCHAR(15),
    Estado BIT DEFAULT 1,
    UsuarioCreacion INT NOT NULL,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    
    CONSTRAINT FK_Almacenes_Usuarios FOREIGN KEY (UsuarioCreacion) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- Tabla de Proveedores
CREATE TABLE Catalogo.Proveedores (
    IdProveedor INT IDENTITY(1,1) PRIMARY KEY,
    RUC VARCHAR(11) NOT NULL UNIQUE,
    RazonSocial VARCHAR(200) NOT NULL,
    NombreComercial VARCHAR(200),
    Direccion VARCHAR(255),
    Telefono VARCHAR(15),
    Email VARCHAR(100),
    Contacto VARCHAR(100),
    Estado BIT DEFAULT 1,
    UsuarioCreacion INT NOT NULL,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    
    CONSTRAINT FK_Proveedores_Usuarios FOREIGN KEY (UsuarioCreacion) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- =============================================
-- TABLAS MAESTRAS PRINCIPALES
-- =============================================

-- Tabla de Productos (MAESTRA)
CREATE TABLE Catalogo.Productos (
    IdProducto INT IDENTITY(1,1) PRIMARY KEY,
    CodigoProducto VARCHAR(20) NOT NULL UNIQUE,
    Nombre VARCHAR(150) NOT NULL,
    Descripcion VARCHAR(500),
    IdCategoria INT NOT NULL,
    IdUnidadMedida INT NOT NULL,
    PrecioUnitarioBase DECIMAL(10,2) NOT NULL,
    IdProveedorPrincipal INT,
    Estado BIT DEFAULT 1,
    UsuarioCreacion INT NOT NULL,
    FechaCreacion DATETIME DEFAULT GETDATE(),
    UsuarioModificacion INT NULL,
    FechaModificacion DATETIME NULL,
    
    CONSTRAINT FK_Productos_Categorias FOREIGN KEY (IdCategoria) 
        REFERENCES Catalogo.Categorias(IdCategoria),
    CONSTRAINT FK_Productos_UnidadesMedida FOREIGN KEY (IdUnidadMedida) 
        REFERENCES Catalogo.UnidadesMedida(IdUnidadMedida),
    CONSTRAINT FK_Productos_Proveedores FOREIGN KEY (IdProveedorPrincipal) 
        REFERENCES Catalogo.Proveedores(IdProveedor),
    CONSTRAINT FK_Productos_Usuarios_Creacion FOREIGN KEY (UsuarioCreacion) 
        REFERENCES Seguridad.Usuarios(IdUsuario),
    CONSTRAINT FK_Productos_Usuarios_Modificacion FOREIGN KEY (UsuarioModificacion) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- =============================================
-- TABLAS DETALLE/DINÁMICAS
-- =============================================

-- Tabla DETALLE de Stock por Almacén
CREATE TABLE Inventario.StockAlmacen (
    IdStock BIGINT IDENTITY(1,1) PRIMARY KEY,
    IdProducto INT NOT NULL,
    IdAlmacen INT NOT NULL,
    StockActual DECIMAL(10,2) DEFAULT 0,
    StockMinimo DECIMAL(10,2) DEFAULT 0,
    StockMaximo DECIMAL(10,2) DEFAULT 0,
    FechaUltimaEntrada DATETIME NULL,
    FechaUltimaSalida DATETIME NULL,
    UsuarioUltimaActualizacion INT NOT NULL,
    FechaUltimaActualizacion DATETIME DEFAULT GETDATE(),
    
    CONSTRAINT UK_StockAlmacen UNIQUE (IdProducto, IdAlmacen),
    CONSTRAINT FK_Stock_Productos FOREIGN KEY (IdProducto) 
        REFERENCES Catalogo.Productos(IdProducto),
    CONSTRAINT FK_Stock_Almacenes FOREIGN KEY (IdAlmacen) 
        REFERENCES Catalogo.Almacenes(IdAlmacen),
    CONSTRAINT FK_Stock_Usuarios FOREIGN KEY (UsuarioUltimaActualizacion) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- Tabla MAESTRA de Movimientos
CREATE TABLE Inventario.MovimientosInventario (
    IdMovimiento BIGINT IDENTITY(1,1) PRIMARY KEY,
    NumeroDocumento VARCHAR(20) NOT NULL UNIQUE,
    TipoMovimiento CHAR(1) NOT NULL CHECK (TipoMovimiento IN ('E', 'S')), -- E: Entrada, S: Salida
    TipoDocumento VARCHAR(20) NOT NULL, -- OC: Orden Compra, VS: Venta, AJ: Ajuste
    FechaMovimiento DATETIME NOT NULL,
    IdAlmacen INT NOT NULL,
    UsuarioResponsable INT NOT NULL,
    ObservacionesGenerales VARCHAR(500),
    EstadoMovimiento VARCHAR(20) DEFAULT 'ACTIVO' CHECK (EstadoMovimiento IN ('ACTIVO', 'ANULADO')),
    
    CONSTRAINT FK_Movimientos_Almacenes FOREIGN KEY (IdAlmacen) 
        REFERENCES Catalogo.Almacenes(IdAlmacen),
    CONSTRAINT FK_Movimientos_Usuarios FOREIGN KEY (UsuarioResponsable) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- Tabla DETALLE de Movimientos
CREATE TABLE Inventario.MovimientosInventarioDetalle (
    IdMovimientoDetalle BIGINT IDENTITY(1,1) PRIMARY KEY,
    IdMovimiento BIGINT NOT NULL,
    IdProducto INT NOT NULL,
    Cantidad DECIMAL(10,2) NOT NULL,
    PrecioUnitario DECIMAL(10,2) NOT NULL,
    Subtotal DECIMAL(12,2) NOT NULL,
    Lote VARCHAR(50) NULL,
    FechaVencimiento DATE NULL,
    
    CONSTRAINT FK_MovimientosDetalle_Movimiento FOREIGN KEY (IdMovimiento) 
        REFERENCES Inventario.MovimientosInventario(IdMovimiento),
    CONSTRAINT FK_MovimientosDetalle_Productos FOREIGN KEY (IdProducto) 
        REFERENCES Catalogo.Productos(IdProducto)
);

-- Tabla MAESTRA de Órdenes de Reposición
CREATE TABLE Compras.OrdenesReposicion (
    IdOrden INT IDENTITY(1,1) PRIMARY KEY,
    NumeroOrden VARCHAR(20) NOT NULL UNIQUE,
    FechaGeneracion DATETIME NOT NULL,
    FechaRequerida DATE NOT NULL,
    IdProveedor INT NOT NULL,
    EstadoOrden VARCHAR(20) DEFAULT 'PENDIENTE' 
        CHECK (EstadoOrden IN ('PENDIENTE', 'APROBADA', 'RECIBIDA', 'CANCELADA')),
    UsuarioGenerador INT NOT NULL,
    ObservacionesGenerales VARCHAR(500),
    FechaRecepcion DATETIME NULL,
    UsuarioRecepcion INT NULL,
    
    CONSTRAINT FK_Ordenes_Proveedores FOREIGN KEY (IdProveedor) 
        REFERENCES Catalogo.Proveedores(IdProveedor),
    CONSTRAINT FK_Ordenes_Usuarios_Generador FOREIGN KEY (UsuarioGenerador) 
        REFERENCES Seguridad.Usuarios(IdUsuario),
    CONSTRAINT FK_Ordenes_Usuarios_Recepcion FOREIGN KEY (UsuarioRecepcion) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- Tabla DETALLE de Órdenes de Reposición
CREATE TABLE Compras.OrdenesReposicionDetalle (
    IdOrdenDetalle BIGINT IDENTITY(1,1) PRIMARY KEY,
    IdOrden INT NOT NULL,
    IdProducto INT NOT NULL,
    CantidadSolicitada DECIMAL(10,2) NOT NULL,
    CantidadRecibida DECIMAL(10,2) DEFAULT 0,
    PrecioUnitario DECIMAL(10,2) NOT NULL,
    Subtotal DECIMAL(12,2) NOT NULL,
    Observaciones VARCHAR(255),
    
    CONSTRAINT FK_OrdenesDetalle_Orden FOREIGN KEY (IdOrden) 
        REFERENCES Compras.OrdenesReposicion(IdOrden),
    CONSTRAINT FK_OrdenesDetalle_Productos FOREIGN KEY (IdProducto) 
        REFERENCES Catalogo.Productos(IdProducto)
);

-- =============================================
-- TABLAS DE AUDITORÍA ESPECÍFICAS
-- =============================================

-- Auditoría para Productos
CREATE TABLE Auditoria.AuditoriaProductos (
    IdAuditoria BIGINT IDENTITY(1,1) PRIMARY KEY,
    IdProducto INT NOT NULL,
    Operacion VARCHAR(20) NOT NULL CHECK (Operacion IN ('INSERT', 'UPDATE', 'DELETE')),
    UsuarioAccion INT NOT NULL,
    FechaAccion DATETIME NOT NULL DEFAULT GETDATE(),
    ValoresAnteriores XML,
    ValoresNuevos XML,
    Descripcion VARCHAR(500),
    
    CONSTRAINT FK_AuditoriaProductos_Productos FOREIGN KEY (IdProducto) 
        REFERENCES Catalogo.Productos(IdProducto),
    CONSTRAINT FK_AuditoriaProductos_Usuarios FOREIGN KEY (UsuarioAccion) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- Auditoría para Movimientos
CREATE TABLE Auditoria.AuditoriaMovimientos (
    IdAuditoria BIGINT IDENTITY(1,1) PRIMARY KEY,
    IdMovimiento BIGINT NOT NULL,
    Operacion VARCHAR(20) NOT NULL CHECK (Operacion IN ('INSERT', 'UPDATE', 'DELETE', 'ANULAR')),
    UsuarioAccion INT NOT NULL,
    FechaAccion DATETIME NOT NULL DEFAULT GETDATE(),
    ValoresAnteriores XML,
    ValoresNuevos XML,
    Descripcion VARCHAR(500),
    
    CONSTRAINT FK_AuditoriaMovimientos_Movimientos FOREIGN KEY (IdMovimiento) 
        REFERENCES Inventario.MovimientosInventario(IdMovimiento),
    CONSTRAINT FK_AuditoriaMovimientos_Usuarios FOREIGN KEY (UsuarioAccion) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- Auditoría para Órdenes de Reposición
CREATE TABLE Auditoria.AuditoriaOrdenesReposicion (
    IdAuditoria BIGINT IDENTITY(1,1) PRIMARY KEY,
    IdOrden INT NOT NULL,
    Operacion VARCHAR(20) NOT NULL CHECK (Operacion IN ('INSERT', 'UPDATE', 'DELETE', 'APROBAR', 'RECIBIR')),
    UsuarioAccion INT NOT NULL,
    FechaAccion DATETIME NOT NULL DEFAULT GETDATE(),
    ValoresAnteriores XML,
    ValoresNuevos XML,
    Descripcion VARCHAR(500),
    
    CONSTRAINT FK_AuditoriaOrdenes_Ordenes FOREIGN KEY (IdOrden) 
        REFERENCES Compras.OrdenesReposicion(IdOrden),
    CONSTRAINT FK_AuditoriaOrdenes_Usuarios FOREIGN KEY (UsuarioAccion) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- Auditoría para Stock
CREATE TABLE Auditoria.AuditoriaStock (
    IdAuditoria BIGINT IDENTITY(1,1) PRIMARY KEY,
    IdStock BIGINT NOT NULL,
    Operacion VARCHAR(20) NOT NULL CHECK (Operacion IN ('UPDATE', 'AJUSTE')),
    UsuarioAccion INT NOT NULL,
    FechaAccion DATETIME NOT NULL DEFAULT GETDATE(),
    StockAnterior DECIMAL(10,2) NOT NULL,
    StockNuevo DECIMAL(10,2) NOT NULL,
    Diferencia DECIMAL(10,2) NOT NULL,
    Motivo VARCHAR(255),
    
    CONSTRAINT FK_AuditoriaStock_Stock FOREIGN KEY (IdStock) 
        REFERENCES Inventario.StockAlmacen(IdStock),
    CONSTRAINT FK_AuditoriaStock_Usuarios FOREIGN KEY (UsuarioAccion) 
        REFERENCES Seguridad.Usuarios(IdUsuario)
);

-- =============================================
-- INSERCIÓN DE DATOS BASE
-- =============================================

-- Insertar Usuarios primero (necesarios para las FK)
INSERT INTO Seguridad.Usuarios (Usuario, NombreCompleto, Email, Rol) VALUES
('admin', 'Administrador del Sistema', 'admin@logiperu.com', 'Administrador'),
('jperez', 'Juan Pérez Rojas', 'jperez@logiperu.com', 'Jefe de Almacén'),
('mlopez', 'María López García', 'mlopez@logiperu.com', 'Almacenero'),
('crodriguez', 'Carlos Rodríguez Vargas', 'crodriguez@logiperu.com', 'Almacenero'),
('agarcia', 'Ana García Mendoza', 'agarcia@logiperu.com', 'Almacenero'),
('fmartinez', 'Fernando Martínez Silva', 'fmartinez@logiperu.com', 'Compras');
select * from Seguridad.Usuarios;
-- Insertar Categorías
INSERT INTO Catalogo.Categorias (CodigoCategoria, Nombre, Descripcion, UsuarioCreacion) VALUES
('CAT-OFI', 'Materiales de Oficina', 'Productos para uso administrativo y de oficina', 1),
('CAT-LIM', 'Limpieza y Aseo', 'Productos de limpieza y aseo personal', 1),
('CAT-TEC', 'Tecnología', 'Equipos y suministros tecnológicos', 1),
('CAT-CON', 'Materiales de Construcción', 'Materiales para construcción y obra', 1),
('CAT-SEG', 'Seguridad Industrial', 'Equipos de protección personal', 1),
('CAT-FER', 'Ferretería', 'Herramientas y artículos de ferretería', 1);
select * from Catalogo.Categorias;
-- Insertar Unidades de Medida
INSERT INTO Catalogo.UnidadesMedida (CodigoUnidad, Nombre, Abreviatura, UsuarioCreacion) VALUES
('UND', 'Unidad', 'UND', 1),
('CJA', 'Caja', 'CJA', 1),
('PQT', 'Paquete', 'PQT', 1),
('KG', 'Kilogramo', 'KG', 1),
('LT', 'Litro', 'LT', 1),
('MT', 'Metro', 'MT', 1),
('GL', 'Galón', 'GL', 1),
('RLL', 'Rollo', 'RLL', 1);
select * from Catalogo.UnidadesMedida;
-- Insertar Almacenes
INSERT INTO Catalogo.Almacenes (CodigoAlmacen, Nombre, Direccion, Ubicacion, Responsable, Telefono, UsuarioCreacion) VALUES
('ALM-CEN', 'Almacén Central', 'Av. Argentina 3000, Callao', 'Lima', 'Juan Pérez', '014567890', 1),
('ALM-NTE', 'Almacén Norte', 'Av. Túpac Amaru 123, Independencia', 'Lima', 'María López', '014567891', 1),
('ALM-SUR', 'Almacén Sur', 'Av. Los Héroes 456, San Juan de Miraflores', 'Lima', 'Carlos Rodríguez', '014567892', 1),
('ALM-ARE', 'Almacén Arequipa', 'Av. Bolognesi 789, Centro de Arequipa', 'Arequipa', 'Ana García', '054321098', 1);
select * from Catalogo.Almacenes;
-- Insertar Proveedores
INSERT INTO Catalogo.Proveedores (RUC, RazonSocial, NombreComercial, Direccion, Telefono, Email, Contacto, UsuarioCreacion) VALUES
('20100123456', 'Distribuidora Office Perú S.A.', 'Office Perú', 'Av. La Marina 2345, San Miguel', '014512345', 'ventas@officeperu.com', 'Roberto Silva', 1),
('20100234567', 'Limpieza Total E.I.R.L.', 'Limpieza Total', 'Jr. Lampa 678, Cercado de Lima', '014523456', 'administracion@limpiezatotal.com', 'Susana Mendoza', 1),
('20100345678', 'TecnoImport Perú S.A.C.', 'TecnoImport', 'Av. Javier Prado 1234, San Isidro', '014534567', 'cotizaciones@tecnoimport.com', 'Miguel Ángel Torres', 1),
('20100456789', 'Ferremateriales Andinos S.A.', 'Ferremateriales Andinos', 'Av. Argentina 3456, Callao', '014545678', 'ventas@ferremateriales.com', 'Jorge Chávez', 1),
('20100567890', 'Seguridad Industrial Pro S.A.C.', 'Seguridad Pro', 'Jr. Carabaya 456, Cercado de Lima', '014556789', 'info@seguridadpro.com', 'Luis Fernández', 1);
select * from Catalogo.Proveedores;
-- Insertar Productos
INSERT INTO Catalogo.Productos (CodigoProducto, Nombre, Descripcion, IdCategoria, IdUnidadMedida, PrecioUnitarioBase, IdProveedorPrincipal, UsuarioCreacion) VALUES
('OFI-001', 'Resma de Papel A4', 'Resma de papel bond A4 80gr, 500 hojas', 1, 3, 25.50, 1, 1),
('OFI-002', 'Folder Manila A4', 'Folder manila tamaño A4, caja x 100 unidades', 1, 2, 45.00, 1, 1),
('OFI-003', 'Bolígrafo Azul', 'Bolígrafo punto medio tinta azul, caja x 50 unidades', 1, 2, 15.75, 1, 1),
('LIM-001', 'Jabón Líquido para Manos', 'Jabón líquido antibacterial 500ml', 2, 5, 8.90, 2, 1),
('LIM-002', 'Detergente Líquido', 'Detergente líquido concentrado 1 litro', 2, 5, 12.50, 2, 1),
('TEC-001', 'Mouse Inalámbrico', 'Mouse óptico inalámbrico USB', 3, 1, 35.00, 3, 1),
('TEC-002', 'Teclado USB', 'Teclado multimedia USB español', 3, 1, 45.00, 3, 1),
('CON-001', 'Cemento Sol', 'Cemento tipo I, bolsa 42.5kg', 4, 1, 22.00, 4, 1),
('CON-002', 'Ladrillo King Kong', 'Ladrillo King Kong 18 huecos', 4, 1, 1.20, 4, 1),
('SEG-001', 'Casco de Seguridad', 'Casco de seguridad industrial color amarillo', 5, 1, 28.50, 5, 1);
select * from Catalogo.Productos ;
-- Insertar Stock por Almacén
INSERT INTO Inventario.StockAlmacen (IdProducto, IdAlmacen, StockActual, StockMinimo, StockMaximo, UsuarioUltimaActualizacion) VALUES
(1, 1, 50, 20, 200, 1), (1, 2, 40, 15, 150, 1), (1, 3, 35, 10, 100, 1), (1, 4, 25, 5, 80, 1),
(2, 1, 30, 10, 80, 1), (2, 2, 25, 8, 60, 1), (2, 3, 15, 5, 40, 1), (2, 4, 10, 3, 30, 1),
(3, 1, 60, 25, 300, 1), (3, 2, 50, 20, 250, 1), (3, 3, 45, 15, 200, 1), (3, 4, 45, 15, 200, 1),
(4, 1, 25, 8, 100, 1), (4, 2, 20, 6, 80, 1), (4, 3, 15, 5, 60, 1), (4, 4, 15, 6, 60, 1);

-- Insertar Movimientos de Inventario (MAESTRA)
INSERT INTO Inventario.MovimientosInventario (NumeroDocumento, TipoMovimiento, TipoDocumento, FechaMovimiento, IdAlmacen, UsuarioResponsable, ObservacionesGenerales) VALUES
('OC-2023-001', 'E', 'ORDEN_COMPRA', '2023-01-15', 1, 2, 'Compra inicial de materiales de oficina'),
('VS-2023-045', 'S', 'VENTA', '2023-01-20', 1, 3, 'Venta a cliente corporativo'),
('OC-2023-078', 'E', 'ORDEN_COMPRA', '2023-03-10', 2, 4, 'Reposición almacén norte'),
('VS-2024-156', 'S', 'VENTA', '2024-04-10', 1, 3, 'Venta regular');

-- Insertar Detalle de Movimientos
INSERT INTO Inventario.MovimientosInventarioDetalle (IdMovimiento, IdProducto, Cantidad, PrecioUnitario, Subtotal) VALUES
(1, 1, 200, 23.50, 4700.00),
(1, 2, 100, 42.00, 4200.00),
(2, 1, 50, 25.50, 1275.00),
(3, 3, 300, 14.80, 4440.00),
(4, 1, 25, 25.50, 637.50);

-- Insertar Órdenes de Reposición (MAESTRA)
INSERT INTO Compras.OrdenesReposicion (NumeroOrden, FechaGeneracion, FechaRequerida, IdProveedor, EstadoOrden, UsuarioGenerador, ObservacionesGenerales) VALUES
('OR-2024-001', '2024-03-15', '2024-04-01', 1, 'RECIBIDA', 2, 'Reposición materiales oficina'),
('OR-2024-002', '2024-03-20', '2024-04-05', 2, 'APROBADA', 3, 'Productos limpieza'),
('OR-2024-003', '2024-04-01', '2024-04-20', 3, 'PENDIENTE', 4, 'Equipos tecnología');

-- Insertar Detalle de Órdenes
INSERT INTO Compras.OrdenesReposicionDetalle (IdOrden, IdProducto, CantidadSolicitada, PrecioUnitario, Subtotal) VALUES
(1, 2, 50, 44.50, 2225.00),
(1, 3, 100, 15.20, 1520.00),
(2, 4, 80, 8.50, 680.00),
(2, 5, 100, 12.00, 1200.00),
(3, 6, 30, 33.50, 1005.00);

-- =============================================
-- ÍNDICES 
-- ============================ =================

CREATE INDEX IX_Productos_Codigo ON Catalogo.Productos(CodigoProducto);
CREATE INDEX IX_Movimientos_Fecha ON Inventario.MovimientosInventario(FechaMovimiento);
CREATE INDEX IX_Movimientos_Documento ON Inventario.MovimientosInventario(NumeroDocumento);
CREATE INDEX IX_Ordenes_Numero ON Compras.OrdenesReposicion(NumeroOrden);
CREATE INDEX IX_Stock_ProductoAlmacen ON Inventario.StockAlmacen(IdProducto, IdAlmacen);
CREATE INDEX IX_MovimientosDetalle_Movimiento ON Inventario.MovimientosInventarioDetalle(IdMovimiento);
CREATE INDEX IX_OrdenesDetalle_Orden ON Compras.OrdenesReposicionDetalle(IdOrden);

