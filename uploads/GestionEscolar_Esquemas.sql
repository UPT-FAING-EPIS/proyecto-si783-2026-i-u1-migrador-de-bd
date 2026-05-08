USE GestionEscolar;
GO

-- Crear esquemas
CREATE SCHEMA Academico;
GO
CREATE SCHEMA Administrativo;
GO

-- Mover tablas al esquema correspondiente
ALTER SCHEMA Administrativo TRANSFER dbo.Estudiantes;
ALTER SCHEMA Administrativo TRANSFER dbo.Matriculas;

ALTER SCHEMA Academico TRANSFER dbo.Profesores;
ALTER SCHEMA Academico TRANSFER dbo.Materias;
ALTER SCHEMA Academico TRANSFER dbo.Grupos;
ALTER SCHEMA Academico TRANSFER dbo.Calificaciones;