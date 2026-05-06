CREATE DATABASE FINANCIERA
ON PRIMARY (
    NAME = DATOS,
    FILENAME = '/var/opt/mssql/data/financiera_datos.mdf',
    SIZE = 50MB,
    FILEGROWTH = 10MB,
    MAXSIZE = UNLIMITED
),
FILEGROUP INDICES (
    NAME = INDICES,
    FILENAME = '/var/opt/mssql/data/financiera_indices.ndf',
    SIZE = 100MB,
    FILEGROWTH = 20MB,
    MAXSIZE = 1GB
),
FILEGROUP HISTORICO (
    NAME = HISTORICO,
    FILENAME = '/var/opt/mssql/data/financiera_historico.ndf',
    SIZE = 100MB,
    FILEGROWTH = 50MB,
    MAXSIZE = UNLIMITED
)
LOG ON (
    NAME = LOG,
    FILENAME = '/var/opt/mssql/data/financiera_log.ldf',
    SIZE = 10MB,
    FILEGROWTH = 10MB,
    MAXSIZE = UNLIMITED
);
