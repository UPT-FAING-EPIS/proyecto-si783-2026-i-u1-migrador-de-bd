-- CQL Script generado por MigradorBD
-- Fecha: 2026-05-07 14:17:34.306968

CREATE TABLE IF NOT EXISTS tbcargo (
  idcargo text,
  nombre text,
  estado text,
  PRIMARY KEY (idcargo)
);

CREATE TABLE IF NOT EXISTS tbcategoria (
  idcategoria text,
  nombre text,
  descripcion text,
  estado text,
  fecha_creacion text,
  PRIMARY KEY (idcategoria)
);

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
  PRIMARY KEY (idcliente)
);

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
  PRIMARY KEY (idproducto)
);

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
  PRIMARY KEY (idusuario)
);

CREATE TABLE IF NOT EXISTS public_personas (
  nombre text,
  apellido text,
  email text,
  telefono text,
  PRIMARY KEY (nombre)
);
INSERT INTO public_personas (nombre, apellido, email, telefono) VALUES ('Juan', 'Pérez', 'juan.perez@email.com', '+1234567890');
INSERT INTO public_personas (nombre, apellido, email, telefono) VALUES ('María', 'Gómez', 'maria.gomez@email.com', '+1234567891');
INSERT INTO public_personas (nombre, apellido, email, telefono) VALUES ('Carlos', 'López', 'carlos.lopez@email.com', '+1234567892');
INSERT INTO public_personas (nombre, apellido, email, telefono) VALUES ('Ana', 'Martínez', 'ana.martinez@email.com', '+1234567893');
INSERT INTO public_personas (nombre, apellido, email, telefono) VALUES ('Pedro', 'Rodríguez', 'pedro.rodriguez@email.com', '+1234567894');
INSERT INTO public_personas (nombre, apellido, email, telefono) VALUES ('Laura', 'Hernández', 'laura.hernandez@email.com', '+1234567895');
INSERT INTO public_personas (nombre, apellido, email, telefono) VALUES ('Miguel', 'Díaz', 'miguel.diaz@email.com', '+1234567896');
INSERT INTO public_personas (nombre, apellido, email, telefono) VALUES ('Sofía', 'Torres', 'sofia.torres@email.com', '+1234567897');
INSERT INTO public_personas (nombre, apellido, email, telefono) VALUES ('David', 'Ramírez', 'david.ramirez@email.com', '+1234567898');
INSERT INTO public_personas (nombre, apellido, email, telefono) VALUES ('Elena', 'Flores', 'elena.flores@email.com', '+1234567899');

-- Objetos no-tabulares
{"vistas": [], "triggers": [], "procedimientos": [], "funciones": [], "indices": []}