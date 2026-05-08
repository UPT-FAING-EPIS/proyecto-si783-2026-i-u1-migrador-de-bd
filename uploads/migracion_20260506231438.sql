-- SQL Dump generado por MigradorBD
-- Fecha: 2026-05-06 23:14:38.159451

CREATE TABLE "public.roles" ("nombre_rol" TEXT);

CREATE TABLE "public.usuarios" ("usuario" TEXT, "contraseña" TEXT, "id_rol" TEXT);

CREATE TABLE roles (
	nombre_rol TEXT
);

INSERT INTO `roles` (`nombre_rol`) VALUES ('Administrador');
INSERT INTO `roles` (`nombre_rol`) VALUES ('Asistente');
INSERT INTO `roles` (`nombre_rol`) VALUES ('Participante');

CREATE TABLE usuarios (
	usuario TEXT, 
	"contraseña" TEXT, 
	id_rol TEXT
);

INSERT INTO `usuarios` (`usuario`, `contraseña`, `id_rol`) VALUES ('p_usuario', 'p_contraseña', 'p_id_rol');

