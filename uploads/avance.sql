create database SCRUM;
go
USE SCRUM;
go
CREATE TABLE roles
(
					id_rol int IDENTITY(1,1),
					rol varchar(20) not null,
					CONSTRAINT PK_roles PRIMARY KEY (id_rol)
)


CREATE TABLE pais
(
					id_pais int IDENTITY(1,1),
					pais varchar(25),
					CONSTRAINT PK_pais PRIMARY KEY (id_pais)
)


Create Table personas
					(
					 id_persona int IDENTITY(1,1),
					 apellido varchar(40) not null,
					 nombre varchar(40) not null,
					 sueldo smallmoney,
					 direccion varchar(100),
					 celular varchar(8),
					 sexo varchar(1),
					 nacimiento date,
					 id_rol int not null,
					 id_pais int not null,
					 CONSTRAINT PK_persona PRIMARY KEY (id_persona),
					 CONSTRAINT FK_rol FOREIGN KEY (id_rol)
					 REFERENCES roles(id_rol),
					 CONSTRAINT FK2_pais FOREIGN KEY (id_pais)
					 REFERENCES pais(id_pais)
					)


INSERT INTO roles VALUES ('ScrumMaster')
INSERT INTO roles VALUES ('ProductOwner')
INSERT INTO roles VALUES ('Team')
INSERT INTO roles VALUES ('Stakeholders')


INSERT INTO pais VALUES ('El Salvador')
INSERT INTO pais VALUES ('Guatemala')
INSERT INTO pais VALUES ('Costa Rica')
INSERT INTO pais VALUES ('Japon')
INSERT INTO pais VALUES ('Estados Unidos')
INSERT INTO pais VALUES ('Argentina')
INSERT INTO pais VALUES ('Mexico')


INSERT INTO personas VALUES ('Romero Peña','Sandra Y',1200,'','34567564','F','1972/3/3',3,1)
INSERT INTO personas VALUES ('Arevalo','Maria Johana',950,'','32356787','F','1976/2/16',3,3)
INSERT INTO personas VALUES ('Hernandez Mancia','Marielos',958,'','67567555','F','1986/7/4',3,4)
INSERT INTO personas VALUES ('Perez Juarez','Joaquin',850,'','34567564','M','1969/9/2',3,1)
INSERT INTO personas VALUES ('Solis Castro','Eduardo',600,'','12327523','M','1972/3/11',3,1)
INSERT INTO personas VALUES ('Ramos Nieto','Jorge',550,'','34567174','M','1972/10/4',4,1)
INSERT INTO personas VALUES ('Hernandez Carias','Johanna',1800,'','34234809','F','1952/3/4',1,1)
INSERT INTO personas VALUES ('Romero Peña','Manuel Alberto',0,'','34567342','M','1969/12/4',2,1)
INSERT INTO personas VALUES ('Garcia Alvarado','Roxana',1600,'','34345564','F','1970/4/4',1,1)
INSERT INTO personas VALUES ('Torres Castro','Alfredo',450,'','21467564','M','1980/2/9',3,5)
INSERT INTO personas VALUES ('Romero Peña','Verenice',400,'','34567589','F','1978/3/17',3,1)
INSERT INTO personas VALUES ('Martinez','Evelyn Roxana',340,'','34556764','F','1972/8/14',3,5)
INSERT INTO personas VALUES ('Siguenza','Eduardo',1120,'','21467564','M','1975/10/9',3,1)
INSERT INTO personas VALUES ('Alas Duran','Martin',1200,'','34562364','M','1972/3/4',3,1)
INSERT INTO personas VALUES ('Jerez M.','Jose Carlos',589,'','34563352','M','1972/3/6',4,1)
INSERT INTO personas VALUES ('Bolaños Cea','Sandra',244,'','67557564','F','1968/3/4',3,1)
INSERT INTO personas VALUES ('Cuestas D.','Jenny Maria',1089,'','34567564','F','1972/9/14',3,4)
INSERT INTO personas VALUES ('Romero Peña','Alexandra',1000,'','34565455','F','1972/7/24',3,1)
INSERT INTO personas VALUES ('Molina Nuñez','Mario',578,'','34567564','M','1962/3/7',3,3)
INSERT INTO personas VALUES ('Romero Peña','Jennifer',989,'','35657564','F','1977/10/21',3,3)
INSERT INTO personas VALUES ('Melendez A','Karla Maria',875,'','45656463','F','1956/12/1',4,1)
INSERT INTO personas VALUES ('Romero Peña','Andrea Lissette',0,'','34545343','F','1990/3/4',3,2)
INSERT INTO personas VALUES ('Melendez Torres','Wenceslado',270,'','34564563','M','1977/8/4',4,1)
INSERT INTO personas VALUES ('Vivas Rosales','Tere Maria',786,'','56345674','F','1964/2/7',3,1)



Select * from personas
Select * from pais