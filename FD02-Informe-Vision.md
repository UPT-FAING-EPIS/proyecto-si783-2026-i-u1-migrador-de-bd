<center>

[comment]: <img src="./media/media/image1.png" style="width:1.088in;height:1.46256in" alt="escudo.png" />

![./media/media/image1.png](./media/logo-upt.png)

**UNIVERSIDAD PRIVADA DE TACNA**

**FACULTAD DE INGENIERIA**

**Escuela Profesional de Ingeniería de Sistemas**

**Proyecto *Sistema DBMigrator: Sistema de Migración de Bases de Datos Heterogéneas***

Curso: *Base de Datos II*

Docente: *MAG. Patrick Cuadros Quiroga*

Integrantes:

***Halanocca Rojas, Usher Damiron (2023076795)*  
*LLica Mamani, Jimmy Mijair (2023076795)***

**Tacna – Perú**

***2026***

**  
**
</center>
<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

|CONTROL DE VERSIONES||||||
| :-: | :- | :- | :- | :- | :- |
|Versión|Hecha por|Revisada por|Aprobada por|Fecha|Motivo|
|1\.0|MPV|ELV|ARV|04/04/2026|Versión Original|












# Sistema *DBMigrator*

**Documento de Visión**

**Versión *1.0***
**

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

|CONTROL DE VERSIONES||||||
| :-: | :- | :- | :- | :- | :- |
|Versión|Hecha por|Revisada por|Aprobada por|Fecha|Motivo|
|1\.0|MPV|ELV|ARV|04/04/2026|Versión Original|


<div style="page-break-after: always; visibility: hidden">\pagebreak</div>


**INDICE GENERAL**
#
[1.	Introducción](#_Toc52661346)

1.1	Propósito

1.2	Alcance

1.3	Definiciones, Siglas y Abreviaturas

1.4	Referencias

1.5	Visión General

[2.	Posicionamiento](#_Toc52661347)

2.1	Oportunidad de negocio

2.2	Definición del problema

[3.	Descripción de los interesados y usuarios](#_Toc52661348)

3.1	Resumen de los interesados

3.2	Resumen de los usuarios

3.3	Entorno de usuario

3.4	Perfiles de los interesados

3.5	Perfiles de los Usuarios

3.6	Necesidades de los interesados y usuarios

[4.	Vista General del Producto](#_Toc52661349)

4.1	Perspectiva del producto

4.2	Resumen de capacidades

4.3	Suposiciones y dependencias

4.4	Costos y precios

4.5	Licenciamiento e instalación

[5.	Características del producto](#_Toc52661350)

[6.	Restricciones](#_Toc52661351)

[7.	Rangos de calidad](#_Toc52661352)

[8.	Precedencia y Prioridad](#_Toc52661353)

[9.	Otros requerimientos del producto](#_Toc52661354)

b) Estandares legales

c) Estandares de comunicación	](#_toc394513800)37

d) Estandaraes de cumplimiento de la plataforma	](#_toc394513800)42

e) Estandaraes de calidad y seguridad	](#_toc394513800)42

[CONCLUSIONES](#_Toc52661355)

[RECOMENDACIONES](#_Toc52661356)

[BIBLIOGRAFIA](#_Toc52661357)

[WEBGRAFIA](#_Toc52661358)


<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

**<u>Informe de Visión</u>**

1. <span id="_Toc52661346" class="anchor"></span>**Introducción**

    1.1	Propósito
       Este informe define la visión del Sistema DBMigrator, una herramienta de código abierto para migrar bases de datos entre gestores heterogéneos como MySQL,          Oracle y SQL Server. Su propósito es proporcionar una solución automatizada que garantice integridad, eficiencia y compatibilidad, reduciendo el esfuerzo           manual en procesos de modernización tecnológica empresarial.

    1.2	Alcance
       El sistema abarca análisis de estructuras, extracción por lotes, transformación de datos (mapeo de tipos y campos), carga optimizada (bulk insert/upsert),          validación post-migración y generación de reportes. Incluye soporte para checkpoints, reanudación y manejo de errores. No cubre migraciones en tiempo real          ni bases de datos NoSQL.

    1.3	Definiciones, Siglas y Abreviaturas
   
        DBMigrator: Sistema de Migración de Bases de Datos Heterogéneas.
        SGBD: Sistema Gestor de Bases de Datos.
        RDBMS: Relational Database Management System.
        SQLAlchemy: ORM y biblioteca de conectividad para Python.
        Checkpoint: Punto de guardado para reanudar migraciones interrumpidas.
        Upsert: Operación de inserción o actualización condicional.
        TIR: Tasa Interna de Retorno.
        VAN: Valor Actual Neto.

    1.4	Referencias
   
        Estudio de Factibilidad del Sistema DBMigrator (interno).
        Ley N° 29733: Protección de Datos Personales (Perú).
        Documentación SQLAlchemy: https://docs.sqlalchemy.org/. 
        IEEE Std 830-1998: Recommended Practice for Software Requirements Specifications.
   
    1.5	Visión General
       Este documento describe la oportunidad de negocio, problema, interesados, capacidades del producto, restricciones y análisis de factibilidad. Se estructura         en secciones de posicionamiento, interesados, producto, características, restricciones, calidad, prioridades y requerimientos adicionales, culminando en            conclusiones.

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

2. <span id="_Toc52661347" class="anchor"></span>**Posicionamiento**

    2.1	Oportunidad de negocio
        Organizaciones peruanas y globales migran datos entre SGBD heterogéneos para modernizar infraestructura, pero carecen de herramientas gratuitas y                   configurables. DBMigrator aprovecha Python y bibliotecas open-source para ofrecer ahorros de hasta S/ 3,600 por migración (72 horas-hombre), con TIR del            110% y VAN positivo de S/ 11,961.82 en 3 años.
   
    2.2	Definición del problema
        Las migraciones manuales generan riesgos de pérdida de datos, altos costos (semanas de TI), falta de estandarización y barreras a la modernización. No              existe una solución única para MySQL, Oracle y SQL Server con enfoque en lotes, checkpoints y validación automática.
   
<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

3. <span id="_Toc52661348" class="anchor"></span>**Vista General del Producto**

    3.1	Resumen de los interesados
   
        Desarrolladores (Halanocca Rojas, U.; LLica Mamani, J.): Implementan y mantienen.
        Equipos TI empresariales: Usan para migraciones.
   
    3.2	Resumen de los usuarios
   
        Administradores de bases de datos y desarrolladores que ejecutan migraciones vía CLI o scripts.
   
    3.3	Entorno de usuario
   
        Entornos Windows/Linux con hardware modesto (i5, 16GB RAM), redes locales y SGBD virtualizados. Soporte multiplataforma.
   
    3.4	Perfiles de los interesados
| Interesado      | Rol                    | Prioridad |
| --------------- | ---------------------- | --------- |
| Desarrolladores | Creadores/mantenedores | Alta      |
| Empresas TI     | Beneficiarios          | Media     |

    3.5	Perfiles de los Usuarios
   
        Admin DBA Junior: Configura YAML y ejecuta migraciones.
        Desarrollador Senior: Extiende con plugins de transformación.
   
    3.6	Necesidades de los interesados y usuarios
| Interesado/Usuario | Necesidad                      | Prioridad |
| ------------------ | ------------------------------ | --------- |
| Docente            | Calidad y documentación        | Alta      |
| Desarrolladores    | Modularidad y pair programming | Alta      |
| DBA                | Integridad datos, checkpoints  | Alta      |
| Empresas           | Bajo costo, open-source        | Media     |

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

4. <span id="_Toc52661349" class="anchor"></span>**Estudio de
    Factibilidad**

    4.1	Perspectiva del producto
        DBMigrator es una herramienta CLI innovadora que automatiza ETL (Extract-Transform-Load) entre SGBD, superando limitaciones manuales con mapeo flexible y           optimizaciones.
   
    4.2	Resumen de capacidades
   
        Conexión multi-SGBD (SQLAlchemy).
        Extracción por lotes con checkpoints.
        Mapeo/transformación (FieldMapper, Pandas).
        Carga bulk/upsert con validación.
        Reportes HTML/JSON.
   
    4.3	Suposiciones y dependencias
   
        Suposiciones: Acceso a credenciales seguras; volúmenes moderados.
        Dependencias: Python 3.9+, bibliotecas (SQLAlchemy, Pandas, Pydantic).
   
    4.4	Costos y precios
   
        Costo total: S/ 5,943.50 (personal S/ 5,120; operativos S/ 823.50). Gratuito como open-source.
   
    4.5	Licenciamiento e instalación
   
        Licencia MIT. Instalación: pip install dbmigrator; config via YAML.
<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

5. <span id="_Toc52661350" class="anchor"></span>**Características del producto**
   
Conectividad: Soporte MySQL, Oracle, SQL Server.
Extracción: BatchExtractor con reintentos y checkpoints.
Transformación: FieldMapper configurable; Pandas para complejas.
Carga: executemany/upsert; pooling conexiones.
Validación: Conteos, integridad referencial.
Interfaz: CLI con YAML; reportes automáticos.
Plan desarrollo: 21 issues en 16 días (Fases: Setup, Extracción, Transformación, Carga, Integración).

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

6. <span id="_Toc52661351" class="anchor"></span>**Restricciones**
   
Técnicas: Solo RDBMS relacionales; no real-time.
Organizativas: Comunicación stakeholders; rotación personal.
Cumplimiento: Ley 29733 (sanitización datos sensibles).

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

7. <span id="_Toc52661352" class="anchor"></span>**Rangos de Calidad**
   
Rendimiento: Migración 1M filas <1 hora (lotes 10k).
Disponibilidad: Reanudación 100% desde checkpoint.
Seguridad: SSL/TLS; logs sin datos sensibles.
Escalabilidad: Soporte volúmenes hasta 100M filas con optimizaciones.

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

8. <span id="_Toc52661353" class="anchor"></span>**Precedencia y Prioridad**
| Issue # | Título                          | Prioridad |
| ------- | ------------------------------- | --------- |
| 1-5     | Setup y conectividad            | Alta      |
| 6-10    | Extracción/checkpoints          | Alta      |
| 11-17   | Transformación/carga/validación | Alta      |
| 18-21   | Pipeline/pruebas/docs           | Media     |
<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

9. <span id="_Toc52661354" class="anchor"></span>**Otros requerimientos del producto**
    
b) Estándares legales
Cumplimiento Ley 29733; licencias open-source (MIT/BSD).

c) Estándares de comunicación
Logging estructurado (structlog JSON); reportes HTML.

d) Estándares de cumplimiento de la plataforma
Multiplataforma (Windows/Linux/macOS); GitHub para issues.

e) Estándares de calidad y seguridad
Pydantic validación; backoff exponencial reintentos; sanitización datos.

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

<span id="_Toc52661355" class="anchor"></span>**CONCLUSIONES**

El Sistema DBMigrator es viable técnica (ecosistema Python maduro), económica (B/C 3.01, VAN S/ 11,961.82, TIR 110%), operativa (modular), legal/social/ambiental (open-source, eficiente). Recomendado para desarrollo inmediato.

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

<span id="_Toc52661356" class="anchor"></span>**RECOMENDACIONES**

Iniciar Fase 1 (setup) con pair programming.
Probar con datasets reales tempranamente.
Publicar en GitHub post-desarrollo.

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

<span id="_Toc52661357" class="anchor"></span>**BIBLIOGRAFIA**

IEEE Std 830-1998. Recommended Practice for Software Requirements Specifications.
Ley N° 29733. Protección de Datos Personales, Perú.

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

<span id="_Toc52661358" class="anchor"></span>**WEBGRAFIA**

SQLAlchemy Docs: https://docs.sqlalchemy.org/
Pandas: https://pandas.pydata.org/docs/
GitHub Repo: https://github.com/UPT-FAING-EPIS/proyecto-si783-2026-i-u1-migrador-de-bd

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>
