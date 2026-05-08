-- CQL Script generado por MigradorBD
-- Fecha: 2026-05-06 22:26:51.221887

-- Cassandra keyspace (crear manualmente si es necesario)
-- USE migracion_keyspace;

CREATE TABLE IF NOT EXISTS tbcargo (
  idcargo text,
  nombre text,
  estado text,
  PRIMARY KEY (id)
) WITH CLUSTERING ORDER BY (id DESC);

CREATE TABLE IF NOT EXISTS tbcategoria (
  idcategoria text,
  nombre text,
  descripcion text,
  estado text,
  fecha_creacion text,
  PRIMARY KEY (id)
) WITH CLUSTERING ORDER BY (id DESC);

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

