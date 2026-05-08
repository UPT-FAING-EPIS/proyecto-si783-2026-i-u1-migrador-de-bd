
-- PRODECIMEINTOS ALMACENADOS
USE GestionEscolar;
GO

-- 1. Procedimiento para registrar un nuevo estudiante
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
GO

-- 2. Procedimiento para matricular un estudiante en un grupo
CREATE PROCEDURE Administrativo.MatricularEstudiante
    @IdEstudiante INT,
    @IdGrupo INT
AS
BEGIN
    INSERT INTO Administrativo.Matriculas (IdEstudiante, IdGrupo)
    VALUES (@IdEstudiante, @IdGrupo);
    
    SELECT SCOPE_IDENTITY() AS IdMatricula;
END;
GO

-- 3. Procedimiento para registrar una calificación
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
    END
END;
GO

-- 4. Procedimiento para obtener el promedio de un estudiante
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
GO

-- 5. Procedimiento para actualizar datos de un estudiante
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
GO

-- 6. Procedimiento para crear un nuevo grupo
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
GO

-- 7. Procedimiento para obtener estudiantes por grupo
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
GO

-- 8. Procedimiento para obtener calificaciones por grupo
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
GO

-- 9. Procedimiento para dar de baja una matrícula
CREATE PROCEDURE Administrativo.DarBajaMatricula
    @IdMatricula INT
AS
BEGIN
    UPDATE Administrativo.Matriculas
    SET Estado = 'Inactivo'
    WHERE IdMatricula = @IdMatricula;
END;
GO

-- 10. Procedimiento para obtener el reporte completo de un estudiante
CREATE PROCEDURE Administrativo.ObtenerReporteEstudiante
    @IdEstudiante INT
AS
BEGIN
    -- Información del estudiante
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
GO