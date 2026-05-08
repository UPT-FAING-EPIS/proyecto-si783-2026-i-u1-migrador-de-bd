-- SQL Export generado por MigradorBD
-- Fecha: 2026-05-07 14:06:34.837221
-- Motor destino: mysql

CREATE SCHEMA IF NOT EXISTS `Academico`;
CREATE SCHEMA IF NOT EXISTS `Administrativo`;

CREATE TABLE IF NOT EXISTS `Administrativo`.`Estudiantes` (`DNI` VARCHAR(4000), `Nombres` VARCHAR(4000), `Apellidos` VARCHAR(4000), `FechaNacimiento` VARCHAR(4000), `Genero` VARCHAR(4000), `Direccion` VARCHAR(4000), `Telefono` VARCHAR(4000), `Email` VARCHAR(4000));
INSERT INTO `Administrativo`.`Estudiantes` (`DNI`, `Nombres`, `Apellidos`, `FechaNacimiento`, `Genero`, `Direccion`, `Telefono`, `Email`) VALUES ('@DNI', '@Nombres', '@Apellidos', '@FechaNacimiento', '@Genero', '@Direccion', '@Telefono', '@Email');

CREATE TABLE IF NOT EXISTS `Administrativo`.`Matriculas` (`IdEstudiante` VARCHAR(4000), `IdGrupo` VARCHAR(4000));
INSERT INTO `Administrativo`.`Matriculas` (`IdEstudiante`, `IdGrupo`) VALUES ('@IdEstudiante', '@IdGrupo');

CREATE TABLE IF NOT EXISTS `Academico`.`Calificaciones` (`IdMatricula` VARCHAR(4000), `Nota` VARCHAR(4000));
INSERT INTO `Academico`.`Calificaciones` (`IdMatricula`, `Nota`) VALUES ('@IdMatricula', '@Nota');

CREATE TABLE IF NOT EXISTS `Academico`.`Grupos` (`IdMateria` VARCHAR(4000), `NombreGrupo` VARCHAR(4000), `Semestre` VARCHAR(4000), `Aula` VARCHAR(4000));
INSERT INTO `Academico`.`Grupos` (`IdMateria`, `NombreGrupo`, `Semestre`, `Aula`) VALUES ('@IdMateria', '@NombreGrupo', '@Semestre', '@Aula');

-- Procedimientos importados
CREATE PROCEDURE Administrativo.RegistrarEstudiante
    @DNI CHAR(8),
    @Nombres NVARCHAR(50),
    @Apellidos NVARCHAR(50),
    @FechaNacimiento DATE,
    @Genero CHAR(1),
    @Direccion NVARCHAR(100) = NULL,
    @Telefono CHAR(9) = NULL,
    @Email NVARCHAR(100) = NULL
AS
BEGIN
    INSERT INTO Administrativo.Estudiantes (DNI, Nombres, Apellidos, FechaNacimiento, Genero, Direccion, Telefono, Email)
    VALUES (@DNI, @Nombres, @Apellidos, @FechaNacimiento, @Genero, @Direccion, @Telefono, @Email);
    
    SELECT SCOPE_IDENTITY() AS IdEstudiante;
END;
CREATE PROCEDURE Administrativo.MatricularEstudiante
    @IdEstudiante INT,
    @IdGrupo INT
AS
BEGIN
    INSERT INTO Administrativo.Matriculas (IdEstudiante, IdGrupo)
    VALUES (@IdEstudiante, @IdGrupo);
    
    SELECT SCOPE_IDENTITY() AS IdMatricula;
END;
CREATE PROCEDURE Academico.RegistrarCalificacion
    @IdMatricula INT,
    @Nota DECIMAL(5,2)
AS
BEGIN
    IF @Nota BETWEEN 0 AND 20
    BEGIN
        INSERT INTO Academico.Calificaciones (IdMatricula, Nota)
        VALUES (@IdMatricula, @Nota);
        
        SELECT SCOPE_IDENTITY() AS IdCalificacion;
    END
    ELSE
    BEGIN
        RAISERROR('La nota debe estar entre 0 y 20', 16, 1);
CREATE PROCEDURE Academico.ObtenerPromedioEstudiante
    @IdEstudiante INT
AS
BEGIN
    SELECT 
        e.Nombres + ' ' + e.Apellidos AS Estudiante,
        COUNT(c.IdCalificacion) AS TotalMaterias,
        AVG(c.Nota) AS PromedioGeneral
    FROM Administrativo.Estudiantes e
    INNER JOIN Administrativo.Matriculas mat ON e.IdEstudiante = mat.IdEstudiante
    INNER JOIN Academico.Calificaciones c ON mat.IdMatricula = c.IdMatricula
    WHERE e.IdEstudiante = @IdEstudiante
    GROUP BY e.Nombres, e.Apellidos;
END;
CREATE PROCEDURE Administrativo.ActualizarEstudiante
    @IdEstudiante INT,
    @Direccion NVARCHAR(100) = NULL,
    @Telefono CHAR(9) = NULL,
    @Email NVARCHAR(100) = NULL
AS
BEGIN
    UPDATE Administrativo.Estudiantes
    SET 
        Direccion = ISNULL(@Direccion, Direccion),
        Telefono = ISNULL(@Telefono, Telefono),
        Email = ISNULL(@Email, Email)
    WHERE IdEstudiante = @IdEstudiante;
END;
CREATE PROCEDURE Academico.CrearGrupo
    @IdMateria INT,
    @NombreGrupo NVARCHAR(50),
    @Semestre CHAR(6),
    @Aula NVARCHAR(20) = NULL
AS
BEGIN
    INSERT INTO Academico.Grupos (IdMateria, NombreGrupo, Semestre, Aula)
    VALUES (@IdMateria, @NombreGrupo, @Semestre, @Aula);
    
    SELECT SCOPE_IDENTITY() AS IdGrupo;
END;
CREATE PROCEDURE Academico.ObtenerEstudiantesPorGrupo
    @IdGrupo INT
AS
BEGIN
    SELECT 
        e.IdEstudiante,
        e.DNI,
        e.Nombres + ' ' + e.Apellidos AS Estudiante,
        e.Email,
        e.Telefono,
        mat.FechaMatricula
    FROM Administrativo.Estudiantes e
    INNER JOIN Administrativo.Matriculas mat ON e.IdEstudiante = mat.IdEstudiante
    WHERE mat.IdGrupo = @IdGrupo
    ORDER BY e.Apellidos, e.Nombres;
END;
CREATE PROCEDURE Academico.ObtenerCalificacionesPorGrupo
    @IdGrupo INT
AS
BEGIN
    SELECT 
        e.Nombres + ' ' + e.Apellidos AS Estudiante,
        m.Nombre AS Materia,
        g.NombreGrupo,
        c.Nota,
        c.FechaRegistro
    FROM Academico.Calificaciones c
    INNER JOIN Administrativo.Matriculas mat ON c.IdMatricula = mat.IdMatricula
    INNER JOIN Administrativo.Estudiantes e ON mat.IdEstudiante = e.IdEstudiante
    INNER JOIN Academico.Grupos g ON mat.IdGrupo = g.IdGrupo
    INNER JOIN Academico.Materias m ON g.IdMateria = m.IdMateria
    WHERE g.IdGrupo = @IdGrupo
    ORDER BY e.Apellidos, e.Nombres;
END;
CREATE PROCEDURE Administrativo.DarBajaMatricula
    @IdMatricula INT
AS
BEGIN
    UPDATE Administrativo.Matriculas
    SET Estado = 'Inactivo'
    WHERE IdMatricula = @IdMatricula;
END;
CREATE PROCEDURE Administrativo.ObtenerReporteEstudiante
    @IdEstudiante INT
AS
BEGIN
    -- Informacin del estudiante
    SELECT 
        DNI,
        Nombres + ' ' + Apellidos AS NombreCompleto,
        FechaNacimiento,
        Genero,
        Direccion,
        Telefono,
        Email
    FROM Administrativo.Estudiantes
    WHERE IdEstudiante = @IdEstudiante;
    
    -- Materias matriculadas
    SELECT 
        m.Nombre AS Materia,
        g.NombreGrupo,
        g.Semestre,
        g.Aula,
        p.Nombres + ' ' + p.Apellidos AS Profesor,
        mat.FechaMatricula,
        mat.Estado
    FROM Administrativo.Matriculas mat
    INNER JOIN Academico.Grupos g ON mat.IdGrupo = g.IdGrupo
    INNER JOIN Academico.Materias m ON g.IdMateria = m.IdMateria
    INNER JOIN Academico.Profesores p ON m.IdProfesor = p.IdProfesor
    WHERE mat.IdEstudiante = @IdEstudiante;
    
    -- Calificaciones
    SELECT 
        m.Nombre AS Materia,
        c.Nota,
        c.FechaRegistro
    FROM Academico.Calificaciones c
    INNER JOIN Administrativo.Matriculas mat ON c.IdMatricula = mat.IdMatricula
    INNER JOIN Academico.Grupos g ON mat.IdGrupo = g.IdGrupo
    INNER JOIN Academico.Materias m ON g.IdMateria = m.IdMateria
    WHERE mat.IdEstudiante = @IdEstudiante;
END;
