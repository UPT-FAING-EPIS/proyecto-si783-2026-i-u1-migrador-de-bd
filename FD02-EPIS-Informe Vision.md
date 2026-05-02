**![C:\\Users\\EPIS\\Documents\\upt.png][image1]**

**UNIVERSIDAD PRIVADA DE TACNA**

**FACULTAD DE INGENIERÍA**

**Escuela Profesional de Ingeniería de Sistemas**

 **Proyecto *Migrador de DB***

Curso: *Base de Datos II*

Docente: *Mag. Patrick Cuadros Quiroga*

Integrantes:

***Halanocca Rojas, Usher Damiron	(2023076795)***  
***LLica Mamani, Jimmy Mijair 		(2023076789)***

**Tacna – Perú**  
***2026***

| CONTROL DE VERSIONES |  |  |  |  |  |
| :---: | :---: | :---: | :---: | :---: | ----- |
| Versión | Hecha por | Revisada por | Aprobada por | Fecha | Motivo |
| 1.0 | UHR / JLM  | PCQ  | PCQ  | 29/03/2026 | Versión Original |

# 

# 

# 

# 

# 

# 

# 

# 

# 

# 

# 

# **Sistema Migrador de DB** 

# **Documento de Visión**

# 

# **Versión *1.0***

| CONTROL DE VERSIONES |  |  |  |  |  |
| :---: | :---: | :---: | :---: | :---: | ----- |
| Versión | Hecha por | Revisada por | Aprobada por | Fecha | Motivo |
| 1.0 | UHR / JLM  | PCQ  | PCQ  | 29/03/2026 | Versión Original |

**INDICE GENERAL**

1\.	Introducción	1

1.1	Propósito	1

1.2	Alcance	1

1.3	Definiciones, Siglas y Abreviaturas	1

1.4	Referencias	1

1.5	Visión General	1

2\.	Posicionamiento	1

2.1	Oportunidad de negocio	1

2.2	Definición del problema	2

3\.	Descripción de los interesados y usuarios	3

3.1	Resumen de los interesados	3

3.2	Resumen de los usuarios	3

3.3	Entorno de usuario	4

3.4	Perfiles de los interesados	4

3.5	Perfiles de los Usuarios	4

3.6	Necesidades de los interesados y usuarios	6

4\.	Vista General del Producto	7

4.1	Perspectiva del producto	7

4.2	Resumen de capacidades	8

4.3	Suposiciones y dependencias	8

4.4	Costos y precios	9

4.5	Licenciamiento e instalación	9

5\.	Características del producto	9

6\.	Restricciones	10

7\.	Rangos de calidad	10

8\.	Precedencia y Prioridad	10

9\.	Otros requerimientos del producto	10

	[b) Estandares legales](#heading=h.12ihm3cw4uzn)	32

	[c) Estandares de comunicación](#heading=h.12ihm3cw4uzn)	37

	[d) Estandaraes de cumplimiento de la plataforma](#heading=h.12ihm3cw4uzn)	42

	[e) Estandaraes de calidad y seguridad](#heading=h.12ihm3cw4uzn)	42

[CONCLUSIONES](#heading=h.eihmmu2ey4u8)	46

[RECOMENDACIONES](#heading=h.d1l3lms39y1w)	46

[BIBLIOGRAFIA](#heading=h.g2ld0ikthccd)	46

[WEBGRAFIA](#heading=h.pjfs3ewls21r)	46

### **1\. Introducción**

#### **1.1 Propósito**

El propósito de este documento es definir de manera clara y concisa la visión, los requisitos funcionales y no funcionales del Sistema Migrador de Base de Datos, una herramienta de escritorio diseñada para automatizar la migración de datos entre 21 motores de bases de datos diferentes, tanto relacionales como NoSQL. Este documento sirve como base para el acuerdo entre el equipo de desarrollo y los interesados del proyecto, estableciendo el alcance, las funcionalidades y las restricciones que guiarán el desarrollo del sistema.

### **1.2 Alcance**

#### **1.2 Alcance**

El Sistema Migrador de Base de Datos será una aplicación de escritorio desarrollada en Python que permitirá la migración automatizada de bases de datos completas entre diferentes motores. El alcance incluye: la configuración de conexiones a bases de datos origen y destino mediante interfaz gráfica; el descubrimiento automático de esquemas (tablas, columnas, tipos de datos, claves primarias y foráneas); la generación y ejecución de scripts DDL para recrear la estructura en el destino; la migración de datos tabla por tabla con procesamiento por lotes; la conversión automática de tipos de datos entre motores; el monitoreo en tiempo real del progreso con capacidad de pausa y reanudación; la validación post-migración mediante comparación de conteos; y la generación de reportes detallados. El sistema es de uso local y no incluye interfaz web ni servicios en la nube. 

#### **1.3 Definiciones, Siglas y Abreviaturas**

| Término | Definición |
| :---- | :---- |
| API | Interfaz de Programación de Aplicaciones (Application Programming Interface) |
| CLI | Interfaz de Línea de Comandos (Command Line Interface) |
| DDL | Lenguaje de Definición de Datos (Data Definition Language) |
| ETL | Extraer, Transformar, Cargar (Extract, Transform, Load) |
| FK | Clave Foránea (Foreign Key) |
| GUI | Interfaz Gráfica de Usuario (Graphical User Interface) |
| JSON | Notación de Objetos JavaScript (JavaScript Object Notation) |
| NoSQL | No solo SQL (bases de datos no relacionales) |
| ORM | Mapeo Objeto-Relacional (Object-Relational Mapping) |
| PK | Clave Primaria (Primary Key) |
| RDBMS | Sistema de Gestión de Bases de Datos Relacionales (Relational Database Management System) |
| SQL | Lenguaje de Consulta Estructurado (Structured Query Language) |
| SQLAlchemy | Biblioteca ORM para Python que proporciona abstracción de bases de datos |
| Tkinter | Biblioteca estándar de Python para interfaces gráficas |
| UML | Lenguaje Unificado de Modelado (Unified Modeling Language) |
| YAML | YAML no es un lenguaje de marcado (YAML Ain't Markup Language) |
| Upsert | Operación que inserta un registro o lo actualiza si ya existe (Update \+ Insert) |

#### **1.4 Referencias**

* Documento de Especificación de Requerimientos de Software (ERS), Versión 1.0.  
* Documento de Arquitectura de Software, Versión 1.0.  
* Documentación oficial de SQLAlchemy 2.0. [https://docs.sqlalchemy.org/en/20/](https://docs.sqlalchemy.org/en/20/)  
* Documentación oficial de Pandas. [https://pandas.pydata.org/docs/](https://pandas.pydata.org/docs/)  
* Ley de Protección de Datos Personales, Ley N.º 29733 (Perú).

#### **1.5 Visión General**

El documento se estructura en nueve secciones principales que detallan: el posicionamiento del producto en el mercado, los perfiles de los interesados y usuarios, una vista general de las capacidades del sistema, un desglose detallado de los requisitos funcionales, las restricciones técnicas y de proyecto, los rangos de calidad esperados, la priorización de funcionalidades, y los estándares legales, de comunicación y de calidad que regirán el desarrollo. 

### **2\. Posicionamiento**

#### **2.1 Oportunidad de negocio**

En la actualidad, las organizaciones de todos los sectores enfrentan la necesidad de migrar sus bases de datos entre diferentes plataformas por múltiples razones: actualizaciones tecnológicas, migración a la nube, consolidación de sistemas tras fusiones empresariales, o cambio de proveedores de software. Sin embargo, el mercado carece de una herramienta unificada, accesible y gratuita que soporte la migración entre una amplia variedad de motores de bases de datos, tanto relacionales como NoSQL.

El Sistema Migrador de Base de Datos representa una oportunidad para llenar este vacío, ofreciendo una solución de escritorio intuitiva que automatiza el proceso ETL completo, desde el descubrimiento del esquema hasta la validación post-migración, soportando 21 motores diferentes. Al ser una aplicación local que no requiere servicios en la nube, elimina las barreras de costo y conectividad que limitan a otras soluciones del mercado.

#### **2.2 Definición del problema**

### Las organizaciones que necesitan migrar sus bases de datos entre diferentes motores enfrentan ineficiencias operativas y riesgos de pérdida de datos debido a la falta de herramientas automatizadas y accesibles.

### El problema de | La migración manual de bases de datos entre motores heterogéneos Afecta a | Administradores de bases de datos, ingenieros de software, arquitectos de sistemas y organizaciones que modernizan su infraestructura tecnológica Cuyo impacto es | Procesos de migración que consumen de 3 a 5 días hábiles, alta probabilidad de errores humanos en la conversión de tipos de datos, pérdida de integridad referencial, falta de trazabilidad del proceso, y costos elevados por el uso de herramientas especializadas o consultoría externa Una solución exitosa | Permitiría automatizar completamente el proceso de migración, reduciendo el tiempo en al menos un 60%, eliminando errores de conversión manual, garantizando la integridad de los datos mediante validación automática, y proporcionando una interfaz gráfica intuitiva que no requiera conocimientos técnicos avanzados

### 

### **3\. Descripción de los interesados y usuarios**

#### **3.1 Resumen de los interesados**

| Nombre | Representa | Rol |
| :---- | :---- | :---- |
| Administrador de Base de Datos | Personal técnico responsable de la gestión de bases de datos | Usuario principal y validador funcional |
| Ingeniero de Software | Desarrolladores que necesitan migrar datos entre entornos | Usuario secundario y proveedor de retroalimentación |
| Arquitecto de Sistemas | Responsables de la infraestructura tecnológica | Interesado en la compatibilidad y rendimiento |
| Equipo de Desarrollo | Estudiantes de Ingeniería de Sistemas | Proveedor de la solución y responsable técnico |

#### 

#### **3.2 Resumen de los usuarios**

| Nombre | Rol | Descripción |
| :---- | :---- | :---- |
| Usuario Administrador | Administrador de BD / Ingeniero de Software | Acceso completo a todas las funcionalidades del sistema: configuración de conexiones, descubrimiento de esquemas, ejecución de migraciones, monitoreo de progreso y generación de reportes |
| Usuario Técnico | Desarrollador / Arquitecto | Acceso a funcionalidades avanzadas: modificación de archivos YAML, extensión de reglas de transformación, adición de nuevos motores de bases de datos |

#### 

#### **3.3 Entorno de usuario**

El sistema será una aplicación de escritorio ejecutable en sistemas operativos Windows 10/11 y Linux (Ubuntu 20.04+). La interfaz gráfica estará construida con Tkinter, proporcionando una experiencia nativa en cada plataforma. La aplicación se ejecutará localmente en la máquina del usuario, requiriendo únicamente conectividad de red hacia las bases de datos origen y destino configuradas. La carga de trabajo esperada es de un usuario concurrente por instancia de la aplicación. 

#### **3.4 Perfiles de los interesados**

#### Administrador de Base de Datos

| Aspecto | Descripción |
| :---- | :---- |
| Representante | Administrador de Base de Datos |
| Tipo | Interesado principal y usuario experto |
| Responsabilidades | Configurar y ejecutar migraciones, validar la integridad de los datos transferidos, interpretar reportes de migración |
| Grado de participación | Alto, especialmente en las etapas de prueba y validación del producto |
| Comentarios | Requiere información detallada sobre el progreso de la migración y la capacidad de pausar/reanudar procesos largos |

#### 

#### Ingeniero de Software

| Aspecto | Descripción |
| :---- | :---- |
| Representante | Ingeniero de Software / Desarrollador |
| Tipo | Usuario secundario |
| Responsabilidades | Utilizar el sistema para migrar datos entre entornos de desarrollo, testing y producción |
| Grado de participación | Medio, como usuario ocasional |
| Comentarios | Valora la capacidad de extender el sistema con nuevas reglas de transformación y soporte para motores adicionales |

#### 

#### **3.5 Perfiles de los Usuarios**

#### Usuario Administrador

| Aspecto | Descripción |
| :---- | :---- |
| Representante | Administrador de Base de Datos |
| Rol | Usuario Administrador |
| Responsabilidades | Configurar conexiones a bases de datos, descubrir esquemas automáticamente, ejecutar migraciones completas, monitorear el progreso, generar reportes finales |
| Nivel de experiencia | Avanzado en bases de datos; medio en herramientas de software |
| Necesidades principales | Una interfaz clara con pestañas organizadas, barra de progreso visible, estadísticas en tiempo real, capacidad de pausar y reanudar migraciones |

#### 

#### Usuario Técnico

| Aspecto | Descripción |
| :---- | :---- |
| Representante | Ingeniero de Software / Arquitecto de Sistemas |
| Rol | Usuario Técnico |
| Responsabilidades | Modificar archivos de configuración YAML, agregar reglas de transformación personalizadas, extender el soporte para nuevos motores |
| Nivel de experiencia | Avanzado en programación Python; alto en bases de datos |
| Necesidades principales | Acceso a archivos de configuración editables, documentación clara de la arquitectura modular, posibilidad de inyectar código personalizado |

#### 

#### 

#### **3.6 Necesidades de los interesados y usuarios**

| Necesidad | Prioridad | Preocupaciones | Solución Propuesta |
| :---- | :---- | :---- | :---- |
| Migrar datos entre diferentes motores de BD | Crítica | Que la conversión de tipos de datos sea incorrecta | Descubrimiento automático de esquemas con tabla de mapeo de tipos entre los 21 motores soportados |
| Monitorear el progreso de la migración | Crítica | No saber en qué estado se encuentra el proceso | Interfaz gráfica con barra de progreso general, barra por tabla, y estadísticas en tiempo real |
| Pausar y reanudar migraciones largas | Alta | Perder el progreso ante una interrupción | Sistema de puntos de control que guarda el estado después de cada tabla migrada |
| Validar que los datos se migraron correctamente | Alta | Registros perdidos o corruptos sin detectar | Validación post-migración con comparación automática de conteos entre origen y destino |

### **4\. Vista General del Producto**

#### **4.1 Perspectiva del producto**

El Sistema Migrador de Base de Datos es una aplicación de escritorio independiente que se comunica directamente con las bases de datos origen y destino a través de drivers nativos y SQLAlchemy. No depende de servicios externos ni requiere conexión a internet. El sistema actúa como una capa de abstracción entre diferentes motores de bases de datos, permitiendo la migración tanto homogénea (mismo motor) como heterogénea (diferentes motores).

El producto incluye componentes de interfaz gráfica (Tkinter), motor ETL (SQLAlchemy \+ Pandas), sistema de logging (Structlog), y archivos de configuración (YAML). Se integra con los motores de base de datos a través de sus respectivos drivers Python.

#### **4.2 Resumen de capacidades**

A continuación se presenta la lista de las capacidades principales del sistema, las cuales se detallarán en los requisitos funcionales de la sección 5\. 

| Capacidad | Descripción Breve |
| :---- | :---- |
| Configuración de Conexiones | Interfaz gráfica para configurar conexiones a 21 motores de bases de datos con prueba de conexión |
| Descubrimiento Automático de Esquema | Detección automática de todas las tablas, columnas, tipos, claves primarias y foráneas |
| Generación de Estructura | Creación automática de scripts DDL y ejecución en la base de datos destino |
| Migración de Datos por Lotes | Extracción, transformación y carga de datos en lotes configurables |
| Conversión de Tipos de Datos | Mapeo automático de tipos entre diferentes motores de bases de datos |

#### 

#### **4.3 Suposiciones y dependencias**

Suposiciones:

* El usuario tiene instalado Python 3.8 o superior en su sistema  
* El usuario dispone de las credenciales de acceso a las bases de datos origen y destino  
* Las bases de datos origen y destino son accesibles desde la máquina del usuario a través de la red  
* El usuario tiene permisos de lectura en la base de datos origen y permisos de escritura en la base de datos destino

Dependencias:

* La funcionalidad de conexión a cada motor depende de la disponibilidad del driver Python correspondiente  
* La conversión de tipos de datos entre motores depende de la tabla de mapeo interno, la cual cubre los tipos más comunes  
* La generación de scripts DDL asume que el usuario tiene permisos para crear tablas en la base de datos destino

#### **4.4 Costos y precios**

El Sistema Migrador de Base de Datos se distribuye como software de código abierto bajo licencia MIT, sin costo de adquisición. Los únicos costos asociados son los de desarrollo inicial, asumidos por el equipo de desarrollo como parte del proyecto académico del curso de Base de Datos II. 

#### **4.5 Licenciamiento e instalación**

El producto se entregará bajo la licencia MIT, que permite el uso, modificación y distribución libre del software. La instalación consiste en:

1. Clonar o descargar el repositorio del código fuente  
2. Crear un entorno virtual de Python  
3. Instalar las dependencias mediante **pip install \-r requirements.txt**  
4. Ejecutar la aplicación con python **interfaz\_grafica.py**

No se requiere instalación de servicios adicionales ni configuración de servidores.

### **5\. Características del producto**

A continuación, se detallan todos los requerimientos funcionales (RF) del sistema.

| ID | Descripción del Requerimiento Funcional |
| :---- | :---- |
| RF-01 | El sistema debe permitir configurar la conexión a la base de datos origen especificando motor (21 opciones), host, puerto, usuario, contraseña y nombre de base de datos, con un botón de prueba de conexión |
| RF-02 | El sistema debe permitir configurar la conexión a la base de datos destino con los mismos parámetros que el origen |
| RF-03 | El sistema debe validar las conexiones a cada base de datos mostrando un mensaje de éxito o error detallado |
| RF-04 | El sistema debe descubrir automáticamente todas las tablas de la base de datos origen, incluyendo columnas con sus tipos de datos, nulabilidad, claves primarias, claves foráneas e índices |
| RF-05 | El sistema debe guardar el esquema descubierto en un archivo JSON para referencia futura |
| RF-06 | El sistema debe generar automáticamente el script SQL DDL para recrear la estructura descubierta en la base de datos destino |
| RF-07 | El sistema debe ejecutar el script DDL en la base de datos destino, creando todas las tablas con sus restricciones |
| RF-08 | El sistema debe convertir automáticamente los tipos de datos entre el motor origen y el motor destino según una tabla de mapeo interno |
| RF-09 | El sistema debe migrar automáticamente todas las tablas de la base de datos origen al destino, procesando cada tabla secuencialmente |
| RF-10 | El sistema debe extraer datos en lotes configurables para optimizar el rendimiento y evitar sobrecarga de memoria |
| RF-11 | El sistema debe aplicar transformaciones automáticas a los datos durante la migración (mapeo de campos, limpieza de datos) |
| RF-12 | El sistema debe cargar los datos en la base de datos destino utilizando inserción masiva (bulk insert) |

### 

### **6\. Restricciones**

Técnicas:

* El sistema debe ser desarrollado en Python 3.8 o superior, utilizando únicamente bibliotecas de código abierto  
* La interfaz gráfica debe utilizar Tkinter, incluido en la biblioteca estándar de Python  
* La aplicación debe ser compatible con sistemas operativos Windows 10/11 y Linux Ubuntu 20.04+  
* Las consultas a las bases de datos deben utilizar SQLAlchemy como capa de abstracción

Cronograma:

* La duración total del proyecto no debe exceder el período académico del curso de Base de Datos II

Presupuesto:

* El costo total de desarrollo se limita al tiempo de programación del equipo de dos integrantes, sin inversión en infraestructura ni licencias

### **7\. Rangos de calidad**

* Seguridad: El sistema debe proteger las credenciales de conexión mediante variables de entorno y campos de contraseña ocultos en la interfaz gráfica. Las credenciales nunca se almacenarán en texto plano en los archivos de configuración.

* Usabilidad: La interfaz debe ser intuitiva, con pestañas organizadas (Configuración, Migración, Registro de Actividad), colores claros con texto oscuro para legibilidad, y botones descriptivos. Un usuario sin capacitación previa debe poder completar una migración en menos de 10 minutos.

* Rendimiento: El sistema debe procesar datos en lotes configurables para evitar sobrecarga de memoria. El descubrimiento de esquema para una base de datos de 50 tablas debe completarse en menos de 30 segundos. La migración de 100,000 registros debe completarse en menos de 10 minutos en condiciones de red local.

* Disponibilidad: La aplicación debe estar disponible como ejecutable independiente, sin depender de servicios externos ni conexión a internet. El sistema debe manejar errores de red con reintentos automáticos, sin detener todo el proceso de migración.

* Mantenibilidad: El código fuente debe estar organizado en paquetes modulares separados por funcionalidad (extracción, transformación, carga, utilidades) con nombres en español. La adición de un nuevo motor de base de datos debe requerir únicamente agregar una entrada en el diccionario de motores soportados.


  


  


  


  

### **8\. Precedencia y Prioridad**

Los requerimientos funcionales se priorizan de la siguiente manera para gestionar su implementación en el marco del cronograma de 4 meses:

| ID | Requerimiento Funcional | Prioridad | Justificación |
| :---- | :---- | :---- | :---- |
| RF-01 | Configuración conexión origen | Alta | Es la base para cualquier operación del sistema |
| RF-02 | Configuración conexión destino | Alta | Requisito fundamental para la migración |
| RF-03 | Prueba de conexión | Alta | Permite validar la conectividad antes de iniciar |
| RF-04 | Descubrimiento automático de esquema | Alta | Componente central de la propuesta de valor |
| RF-05 | Guardar esquema en JSON | Media | Funcionalidad de respaldo, no crítica para la migración |
| RF-06 | Generación de script DDL | Alta | Requisito para crear la estructura en el destino |
| RF-07 | Ejecución de script DDL | Alta | Sin esto no se pueden migrar los datos |
| RF-08 | Conversión de tipos de datos | Alta | Crítico para migraciones heterogéneas |
| RF-09 | Migración tabla por tabla | Alta | Funcionalidad principal del sistema |
| RF-10 | Extracción por lotes | Alta | Necesario para manejar grandes volúmenes de datos |

### 

### **9\. Otros requerimientos del producto**

#### **a) Estándares aplicables**

El sistema se desarrollará siguiendo los estándares de la comunidad Python (PEP 8 para estilo de código, PEP 257 para documentación). La interfaz gráfica seguirá principios de usabilidad básicos: consistencia visual, retroalimentación inmediata, prevención de errores y minimización de la carga cognitiva. La documentación del código se realizará en español para facilitar el mantenimiento por parte del equipo de desarrollo. 

#### **b) Estándares legales**

El sistema cumplirá con la Ley de Protección de Datos Personales (Ley N.º 29733\) del Perú en lo que respecta al manejo de credenciales y datos de conexión. Las contraseñas se almacenarán exclusivamente en variables de entorno, nunca en texto plano dentro del código fuente o archivos de configuración. El software se distribuye bajo licencia MIT, permitiendo su uso, modificación y distribución libre. 

#### **c) Estándares de comunicación**

Toda comunicación entre los componentes del sistema (interfaz gráfica, orquestador, módulos ETL) se realizará mediante llamadas directas a funciones y métodos, utilizando estructuras de datos nativas de Python (diccionarios, listas). La comunicación con las bases de datos externas se realizará a través de SQLAlchemy, utilizando los protocolos nativos de cada motor. La configuración del sistema se almacenará en formato YAML, siguiendo la especificación YAML 1.2. 

#### **d) Estándares de cumplimiento de la plataforma**

El sistema se desplegará como aplicación de escritorio Python, ejecutable desde el código fuente o mediante un entorno virtual. Se garantiza la compatibilidad con:

* Windows 10/11 con Python 3.8+  
* Linux Ubuntu 20.04+ con Python 3.8+  
* Los 21 motores de bases de datos especificados en el documento de requerimientos

#### **e) Estándares de calidad y seguridad**

* Desarrollo: Se utilizará un sistema de control de versiones Git para la gestión del código fuente. Se implementarán pruebas unitarias para los módulos de extracción, transformación y carga. Se realizarán pruebas de integración con bases de datos SQLite para validar el flujo completo de migración.

* Seguridad: Los campos de contraseña en la interfaz gráfica utilizarán caracteres ocultos. Las credenciales en archivos de configuración YAML utilizarán variables de entorno. No se registrarán contraseñas en los archivos de log.

* Calidad de código: El código fuente estará organizado en paquetes modulares con responsabilidades claramente definidas. Los nombres de variables, funciones y archivos estarán en español para facilitar la comprensión y mantenimiento.

### **CONCLUSIONES**

1. El Documento de Visión establece una base sólida y detallada para el desarrollo del Sistema Migrador de Base de Datos, definiendo claramente el problema que resuelve y la propuesta de valor que ofrece.  
2. Se ha logrado un entendimiento común entre el equipo de desarrollo y los interesados, plasmando las necesidades de migración de bases de datos en 20 requerimientos funcionales priorizados que cubren el ciclo completo del proceso ETL.  
3. La viabilidad del proyecto se ve reforzada por una definición precisa del producto que delimita su alcance (aplicación de escritorio, 21 motores soportados), funcionalidades principales (descubrimiento automático, migración por lotes, validación post-migración) y restricciones técnicas (Python, Tkinter, código abierto).  
4. La arquitectura modular propuesta y los estándares de calidad definidos garantizan que el sistema será mantenible, extensible y capaz de evolucionar para soportar nuevos motores de bases de datos en el futuro.

### **RECOMENDACIONES**

1. Se recomienda a los interesados (docente del curso y equipo de desarrollo) una revisión y aprobación formal de este documento para establecer la línea base de los requisitos.  
2. Se sugiere iniciar la fase de implementación priorizando los requerimientos de alta prioridad: configuración de conexiones (RF-01, RF-02, RF-03), descubrimiento de esquema (RF-04), generación de estructura (RF-06, RF-07), conversión de tipos (RF-08) y migración de datos (RF-09, RF-10).  
3. Se recomienda realizar pruebas tempranas con bases de datos SQLite (incluida en Python) para validar el flujo completo de migración antes de probar con motores más complejos.  
4. Se sugiere documentar el proceso de adición de nuevos motores de bases de datos para facilitar la extensibilidad futura del sistema.  
   

### **BIBLIOGRAFÍA**

* Elmasri, R., & Navathe, S. B. (2016). *Fundamentals of Database Systems* (7th ed.). Pearson.  
* Garcia-Molina, H., Ullman, J. D., & Widom, J. (2009). *Database Systems: The Complete Book* (2nd ed.). Pearson.  
* Copeland, R. (2013). *MongoDB Applied Design Patterns*. O'Reilly Media.  
* Date, C. J. (2011). *SQL and Relational Theory: How to Write Accurate SQL Code* (2nd ed.). O'Reilly Media.

### **WEBGRAFÍA**

* SQLAlchemy Documentation. (2024). *SQLAlchemy 2.0 Documentation*. [https://docs.sqlalchemy.org/en/20/](https://docs.sqlalchemy.org/en/20/)  
* Pandas Development Team. (2024). *Pandas: Python Data Analysis Library*. [https://pandas.pydata.org/docs/](https://pandas.pydata.org/docs/)  
* PostgreSQL Global Development Group. (2024). *PostgreSQL 16 Documentation*. [https://www.postgresql.org/docs/16/index.html](https://www.postgresql.org/docs/16/index.html)  
* MongoDB Inc. (2024). *MongoDB Manual*. [https://docs.mongodb.com/manual/](https://docs.mongodb.com/manual/)  
* Redis Ltd. (2024). *Redis Documentation*. [https://redis.io/documentation](https://redis.io/documentation)  
* Elasticsearch B.V. (2024). *Elasticsearch Guide \[8.11\]*. [https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)  
* Apache Cassandra. (2024). *Cassandra Documentation*. [https://cassandra.apache.org/doc/latest/](https://cassandra.apache.org/doc/latest/)

[image1]: <data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAGkAAACNCAYAAAC0V1SuAAAmiUlEQVR4Xu1dB3wTR9Y3NUd6wAVy5NIAyaZqV5ILxfTejDHYkgyhhDRaKoQk+FIuBdIIYEs2hEC+L5eQdpfkUi53Id+lgG3ZOEBooYWOaaHjOt97szur0awkS8YYO+f/7/d+Ws28ae9NeVN2NiysAQ2oKcT3jM9mz3EJcUt5v1iL5T7+P6Jx48bpV11/9SD4nd7i+hb3g1Mz1atJs2v+kMbzNqCGEN89fhb+9neMWJDQP+FmfI4bG9di+GMTdsFjIy9mFdeG3/hE0qLplS1vb7MnxfVgpWXS4CPNrrrqmdTlj5KmTZt2F/kbcAnoM2bQo/H9eg7rNaRf59F/nnIOnBr1SRo4I9X5cGV8fLxF5OdxQ9uI5+xvP04GPpVO+j9uJ6Nfu584/mceGfz0xArw/oPI34BqYtgjjr3Qxc1IeWNWaeLogV+OXzq7JG3FY6WJgxPbgnfjuLi46WIYhqYtWlivaXV99tjM2cS+ai7plpJIsCWhooyDLftF/gZUE8PnTjgwYNLo87Y355BBU8eQgZOSykbMnbB51JOTLvZPG7ZI5BfQ/Oaud74zLvuhSlRM8pKZJPHBsVRJ2MKaNWvWWQzQgGqgX+qQp9Og9ic9N40Kd9RTk8nAqcknunfvHtcjsUf/hISE68QwAm5v0qTJ8Ph7hp/G8DHD42g8SIZBlj1NrmoyUAzQgCDQc3CfufgbK8u94afR+CWzybjFs8n4zAc1AdtXPU762Ucs9A7pF81u69Epb/iLd5OeM5JIlzE9aRx9HhlX2W9uWonI3IAgMPwRx5nBgwdfBYZBOow5LeN7Jnw+av7k80nP3n0eu79eIwZko78Yrgq0aHVHm/eZkpE6j+5Bf8EvRmRuQABA99Ut5dXplX3HDJwOSpo8YHLSd+g+YOLon0XeUNH8muYPmtMHkF6zk+n41Hl0d6qkFi2vf1vkbUAA9Jsw6gUU3Jjn7zk5cGrS1/aVcyuHPmj7EkzxF/skD5xsiotrJ4YJBU2uvnowxo9dX/f7R1El3finyEPXtLzGr5XYAAH9J478JG0ZmMlgfUEXR8YufKA0LfsRMjJjysGUxbMuhPmZvAaLxk2bPhphvKUYlZMILQpNcmxVt5gNRSJvA/wALDZa00dnTCG2FXM0Y2Hsyw9UyrJ8tchfDdwU0eGWb7CrMw6xko4j4mn83cb33icyNiAAhtyf8tcBk5Iqh85MJdDdkf624cvDMsIai3zVRBOgcFTMyAX3kL6PpVIltUvsuh3cbxSZGyDAYrF0jEtUxpw4q/XZ+O7xc2ITYkfi/54Deyd7c18CmjR6DVcgbG/NIWnQWrEiDHt+Clp5A0TWBgjoNazvNqvVen2PIX3u7jGgx9jeSYPmDZw2ds2gqcnv9h0/7A2R/xLQqPdDY0m/uTa68hA/bbhiijcJ6ycyNkDAyCcnnwWzOzI166Hy/qnDHh41f8qxVNdDFSjA2NjYaJH/EjAa40xeOpPcltCRKsgGrQncm4uMDRAwdHbaiaSnp252vD2PDHlgfCUoqXzM8/eSpOfuPinyXgoaX9V0RtKi6VQ5g+anK4ZDat9DYQ1jUtVI6NPHgKb3uEUzybg3ZpERjykCFPlqAL0HzZ9ATGl96eq4BL9qOreIjHUOZE1iU9HtUuB2yeFALrdTOl24RCbrX5dJ0avw+5pM8H++UzoGfs7cxZZWLMyge1PysSUNmpZMsBX1ThqAO6toVPTqHhvboxrLQTrcdGvkLlQKKskycRBVEExsTzP/LcsSrsvLkl7Md8oHCpbq8+12yhfA772CZSa6AVlTAFmxHWT/2PisNHD3isRL3gzLW2q+Jd8ln9j4jJkcSbSS41KsXzrS20o2PmsmkMHdLO2MjIzG3fv2esOdLS3OmmuhSzY9hvT9rHv/XrMSRw5I9U4tdJhS+5Y+s3Iyee1xE1VQ2puPkebNmxsJCWvkdkm5Py2QyaGBgfN9ND6WbH7STKCcJYVZ3bqKaVQHhS9JtEIGRHGf2KGFr8prRfdQADXsy81PyeSYVV+wQISFRuG4l1uGg5X38NZplrmo4O0zzdjivkl+YdrFvuOHroT509/ENENB06aNn3g7ZxjZOsdMK8j7M/qSHtNHV/YeFH2q8HW5srhnYOWIdMwcS7Y9jJVMKhTTCgWFS+UVR7vpz2vo8FvnHjcV97BiraZbBaHCnWXOOpoQWiFF2nm3hby4KLnsYGyC5rY3xUJysy3E/vr9JaPnTzkgphssVq8Oa/Lt690qd02xaHEfk+PIo67JZCsIWsxLKHTMYsXK9JWYZjDIXyoNOzjYSo5J1jjRzyeOmazlv463YjNeIPr5A8kIa1zgkv9zYKheQScHDCFn5s0nFz/7nJQWrielefnk/PK3yG/jbDpeRntnDian5j5BTsT10NyO9Iol7iyZTJg3vlprbFDxbgAhlqIwtLQsCeTU1HvJrwvsujww+i0phZzLdJKSH9eR0vVFpOSrr8nZZ/5CTg4ZoeM93CeW5GdJO4IaW1TkZ8n37LxHqTSin1+cfmDmFxjgwHBsUdKpvExzJ5GHx/ocuXtBplRaLLSgswteJpUVFaSysjIgXXh3ta6wleXlmn958VGPn1lRFA7YMOkNeg/oq4VdrgEFVRyN8+SxbMdOr3yIeTj3xlJdXn3R+WUrvMIdM1tJwRK5Mj9bHivmg8dP2da2BU7p119TFQWdHJu6VeShcGdJj4huFaWlB7UELbFk+2yzYtG4pJ1A78CYMyfPKWVAS/s/ENjFHfdBIrInk6em3EMqzp/XFSYQIf+JvoO0OET/yrIycvrue5U8wVhXkClX2MYk3IV7T2L+RaAxArW7tLinEvdvyamk4sIFXRonrN2p/4n4XqTi5Emdf0AqLSVnHpvnpazdEyyQT6kMDRGgZ8GKfRRa2JtQWbZB/skWGBOPxnn4y8+eKxfznuc0jQj75T4z2ffDy+d1iUIhTvTo45UoEg7k+0cDjbD6NAxO3z9TX4BgCVodSxNMXBwXSUFOHDm170eNp+Rf31B/rDzYooZPHe3GwkDBh0HFeYARdCETyOqUJgSsRKhUF4q7K/k7v2KlFte5o1vJ+hW9aDpItAzmeFJ58aI+b0HS2aef08kEWz/2SvtAbof76ocDrBwVx47p4jq65W/ntzxuJmHHJeuF3XdZyLZPp/nsmi58/HdyPNYzLvijE30GkLI9v3qFrSi7SA66s0nRyn5UCIVvKPMNVAAKZ+8PC4BHqNHQzZ1I6KXFi5ViwwtmcqjII9zy3XvoWIKCX5clV7ihVe1LspKS/TtJ6YlDlA4NsJKiV2Tynze6VeAzxlXqLtDiOL7jK+p/qL+1UisHxCm2sIqKMnKwIAfK0FfJN+R//SJFqetX9IYyLCRlF097hwGBnxw2SicjHUGFOO9a5lPuu7+ZT7bPMoMhEVsZdtQU+zwGODjESmtt2YXfdAGQyn/dS2sJdgV8QqcmTKaGgBc/JPrL5zNoQdBS47tCnnanWyjP7m+e8i7k4cPkMAh290RP2C1zreSnVQNIeVmJwldSQoWK3QbjqTh7TouDpVG4GKYCsXGk4vgJ1a+CbHovmWx6zmPd7bFbaE0v/WmDVz725y5WyjA1QBmgS0Plbf4oHfLm3QLLtm4jp++b7sV/AvJ8Zs4TpGzzFi9erezlpaD8RLJ3nFKxjpliv6T9HmjrJHWA7qvoZZns+fYZXeBg6fzxHVQwWGixQP5o71gLKYQKcu74LzSOXd88SXZNttA5DCpy8xOKaVzcA/NnJod/epvylZ07SedVLB5fSvp1PMSz8T3qdmLHP6EVmKkFhn44t8FKdBjSwcF768eTKF/J2WKy/s2eSiXxkV9fhK0VVyNOHyzUySRYOuB2kUJopThfxDhBQaVrEtUVoE0xMc3R7GYJ7rFB7YA5yekDQgupgn75YiZVslgAmiA0W/gtOirFLj9uil0C6X153GQt43l+WmgmxT9/QH6er5+vFEA3czA+jj7vH2mlte3csW1evL6UhGMntogN7wwHRai10xxbWrRAvohjBZ/GL/dbyKn9ubRbxjGPuUPeKyCvayDPmUDZ8LyOugl5RMKVk58/AMPERxfmj86f2EUKl8WTndO4uRvIqzIurqXHhAAQWb4axqfTGhMUYPNcMxUG1sCKcrWb8UG/7f0BWkK8d+sxQVwm64ribt3ar05JwZ1QnyBQQaDLncPC7bjXTDY8r1cSdjfbwMr8IcNSetAcRwW0C8ZSvlL4UtIeB7SQR8xqdxVXunmeZefmeWZaPjGNrY9ZqMWluZmsrx8IsF0Pk5pGB03xtyIfyK6YhTvUz0oKnWBYbftMJyuNKspBbt/TLnzT02YvIwxb0BGLpbWYHgUk2hhq+iY+4xh466NmUpCFg6WZbP7QTnb+63Gy699PwLON9tmbMszUglHDlBzqEh8pxl0VMG2opdsxjh33WmDQ9NPVQDpYqG2z5O0wOz9PxxyrUjl8KQkXR49aYkt2TLFs+Oklb5OXJ1T45sdVBZmsR0li6OuXR2JiroXwZ1mcWDmofFankF3/mkdltuXjiaQgWx3HnjRrXZtGJusRwrq4QDhiMier3ZNXBNgF4Fxj/2gL2ZcEfT3OO9QB9YApjkzpOOpvkdH2JyIN9nnVpbmdhn53SIqjaaCAd8BM/EgvTwvFCfPOKRby5cLYyk6WcQsk89hXcjPNdHA/u/kHcm5PEaW9KVYafkLysE/u6Jb63Id/ia/4ZbqF8Otyxd2tVDkw8SR7YOwrBrcFnQdtudQyjI4ZvXKnSV3SkpWxVJNZou+pC9JJyTpb1EWVOCpZbDB+nBIj4+kwCPSeTiNJpNFRo3Sn0Ubu6jiKPDdiIPkoI4F8t8RCvltsIe883YMk9Bqr44/tMZYMGTRaoz59x+h4usaOI8ue7Enj+R7i+/uz8eT5MQPI3ZD/jtGpOv5LpeSOSeSApIyjfskUe+6IKW6GKPtqI7LDhC5iRhqoeiTKtsbgpaRo272ifwMCI8Jg/2uDkuo4al1JEdGOgJtTUV0c10QYHX8PN9rfxP8woH4YbrBPRjdKBsenrQ3p5kijfRX+B/93Iw02+kIXuH2AblHGtFTGHwn8GGfLdva2+MvSaQ35gLDrIwzpLubmQUZjLr3P/hid1h7y/rkWpzHtIZ4b8jdLSdfRqW37CX+k+Tek0zy37pgeDfniwtq/wDAQX9Dn9OqIkkgjyMjHkcb0+WHytGYgvGMRHWzp6APPO/AX/EvaxoxtiXG1bjcpAgo8B/z+cW2HtHCWefhfBO4PoKUUFpPSXElzwlTFz3Ei3JieiM8t26fGRhjt31B3o93ne0pqnI0i29v633DrxBtbx9hjgBfit41pZbD35nmhAlzP8nCzMb0VKHYSVLI24HaqTZvhV0d0HH8nPG+NMtj6gEKfRz4oT9n1UB4WB+TnQmQHuwPyf565aX61riSjXbcXD27FETGOweBfSfmN9kzM2E0d77ol3GgbrrhhBqGGxzjoKVUozNegjFnhHWxD4LlY5TkUbpwo4zO2GohHie/21CjgPQT+76vxx9G8GBwJ+F8P0gj9ZagwqADmCoo+c0Nn2008JwPyt42Z0hJ41oehcqNB4Ib0dZx/OeSfzmOgTDLGFW5wzFR8U5pExKS0hjwWtpHTwlkYBtpb1AElzUUFhEenmvB/VMcJfaAQ20CIe8KUNyGo0EBxLlb7IMxFcFsDAt+MYdENniu052hbf/Cnm2EQz9pWtIt0nKIJKrxJGGd4tJ1eIcAjIub+ayH9ElQq30UGEhLk7VzraFsv1lIg/FtA2r0Ralj6Vge4H6I9gdFBe4mW0EXibzh0q4yfR51Qkg+gUsoiOzgexD/hxvEdINwR6qPOqlEhIMBIKmhjegfFzVMIKNhLOEaFpaQ0gd/TEH4X88cuUeGx2dSa7wUIOxKEvpH+gdakuQcQElSEdyDMWc9/+8bIaMdo5V9GY9aqW8IYhT0HxLUvUHw8II+1rSQbFVBVUFqF9ryQjgcdbN2hgGeVrsxBbr114h+gAP8BgXwLbrfzhQBFbABhv4itCPnC1NYYbkxLBLcSrLUQ7p8oNBaGAZUL6XwdDi0DWyy60XAwboi8DJEdbKNQUco/rBiOSiw3/ouCLhtbWhjmweD4Fd0gTzdiflgrCoQroCSlFleFm+8cr50Ijew0JYqnVobJ1+EvGgft2s24Cp/RmKBurHv0cktpQg0SeEZDo5Vh5HWYp3btfB+W5NNq3W5sBO8m8nqgpEEf0WgB3hu6TqRHj2mLV/PL4oiKgoqmloWLxCdASe/VSSU1wIMroaSG90tDBMitlpVksNfcouB/Ca6AktIblBQiwJBZ3aCkOo5aV5Jnll23gG834G5ovks6IfpdaUQa0xuUhMjPka3b37Zen++S78nPkoNe/KwNgNzeb1BSmKKkolVdrlm9OqVJvlP6VvS/kqh1JUX6WCsLBlGGCX0gbL9QSYzHH5iS6LNTomtq/oDLUGI6wRBbhQ8V9UZJdCmfxRECifH4g5eSXPKnoj+PiA6O18V0giKDo1rjXWStK8mYHvrJlrDaVVKeS35L8PZC7SvJ/kGo5QkZ3kpSVrZDRW0qKd8prxD9eTQoyQ9qU0lgiq8W/Xk0KMkPalNJBc7AL2j/FyjJ7nWII1jUppKguwt4t2qtK8lg/zDU8oSMmlASD13hOYowOD4S+YMBUxK+WAwm+GeifyBABdos5kMjg+OMyB8qII5aVpLB9rDoHyp0guDoUpQEyulT4JK/DPUCjAYl+YBOEBxVW0lOKRZaUUV1Lr74HSopvU4qKc/VbUjukhjf7/ZUgcutJCwTi0/0qzF4b1U4dFcLhAqdIDgKRUl4eVRRtvV2fIaWNDFvmZkeKVuXZe36+aJ2Ps8/+MLvUEn2R0X/UKETBEehKAmR75Lz3U7pc+jmHnFnS33zXdJP8BzSHUQNSvIBnSA4ClVJiFyn9BS7k8HtsqaI/lWhQUk+oBMER9VREprcTEkwR6InUEPB5VcSnpOvTSV1sD0m+ocKnSA4CkVJa/BaGpe8H6/TcTvNs6G7i8vHixBd0lbcVxL5/aFBST6gEwRHISkp49Y/MAPBnSn1zF2hvLm9aUnMtXh1mje3fzQoyQd0guAoFCUhIMxyhexvep7Tl4l8gfD7U5LRMYf3K3Cap+VnSe/CeKARjBHKYXk/0AmCo2ooSRdHpPoKTrCoSSXhshR0vY+uU6cGCIj/byw+nrdG4U9JhdnyBI9V5U18eBE6QXDkpaTExKb4jlEgEsOrVCnyicResUHUhJJys6UkGBv3sPJ7K8lR20pK167/hBb0XZ7TNB4ydTVP0JICrkroBMERryTl9Uc9Tw2RtuVyKUoqcMmPwdysgikHDJcKcHut7iipin0bf9AJgqP6pKQCp6kfTKTLqXKy5ef93cN6RZW0LlOKY8syPPCiQNGNh04QHNUnJTHgFgm0oC+g3L9sV61NoSVp5fCEqmEIY9LjvB9krA0ubsIcZTijqiaUOkFwVB+VxGMTTANAJquuqJLC8c3wS4ROEBzVdyX5AsT/CYtP9Ksx+FNSfla3tI1O6U7s8njCvpoPL0InCI7qk5IKckztClym5HVOaWBepjkVnidDK5oJ49S89Vldtdc0r4SSnmDuYNk5cp2mXmuzYtsjwQBqhEyeqykTHC+/wFOjgUgMr1KlyCfSH9vZ8dPcFNVVElbQNSu60lc28YJezxREOuXm7ge/okpigyR7hoxVUkunimO+OkFwVB8ns9CixmgmuI/pxxVQUrqmJIbcbEs3nB/4y6QInSA4qm9Kgq4tTym3fAZ+bxD9EREG+6csPtGvxsArCTL9JO8HmfvI08yVTEJ/fBvPI0InCI7qk5LWLIm5Vq2Y+Gkf7fOqeGm8l3V3JZXkzpa/xjFIpaNQq/YiqZn2C50gOKpPSkLkOSUn9h4ieSvJceWU5A/rFlmvF9146ATBUX1Tkj/g95jYc+0rqYP9Kea+LlOSeT4GyEmjNRn+L33VCYKj+qYkML2TwfQeyf5DT/JDvtOUjV/DYW5Qps9YfMytxuGlJKN9PnNHExyP9OpJKuPDi9AJgqP6pKQ8lzkVlLILytuGuW1aHdM8P1uegSY5c7sCSkr3UhJnNNT8VkUQEMOrVCtKcmfKKf6OjkFreoE9R9a+khwZzJ3vd3n8mCPcEC9AJwiOcJVB5A8EMbxKISkJ+Lf4iEMle0AjKN8lnYfeoxJa1CH4PZmvzhXXrLhVu1/8iirJF3Kd0kT8IqbozkMvCC+h0Oszg4U+PKXQlGSw/+IjDkbaHXu+gC0JFHOS60VKfuSWhBB4QyaLj3evUVSlJOiTO4Mp+lXNdHe2kPaoxPAqhaQk6GLp3XV+6DeR3xc2ZHa+CQ/A4POmjJjma3NitRvBal1JMDH7M3N3Z0mv8IrBVYc8p/lVaPbLueA6RCi3OorCUMjg2C7yB4IuvEIhKQn4T/iIg5X3mMjvD0XLzCaQw88oC36edGWVpLwLdB9+VxYUE/R7SzDuHBaFwcVfKvIHghhepRCVZK/wEQfLz0aRnwea2iADW766Q8uoziiJBx5UhC4vFzL4FoxJq0R/HhF4K6QPgVSnIGJYlUJUki68RmD5fSzyM7izpUFaL+KUSpE2qN0cP6GH3uHz6pQtJHgryfa06M8DctEIlBTw4kI0DkRh8CTyB4IYVqUaU1KkjzGYAa1bUEwf6En+DQpLgnJ/wPyg0mofvK99JUU7NCXhmwz4kUNsRd8vS7jue6d0J0zihoJ7lj/zHAFKesmHMDQKy/Act6oKYliVglaSev+3GN5D0emjxDAMWO6978W1wGesnFRhWXjuUP7Au7urDSUZbJ1ZIqCkZ5g7ZOZpvh/miVk6vhAZPbGfThgcRd3hCPo7TGJYlYJWUpTBMclHeA+1t90hhmHYtCQRV8FxhcXLUMJz6EWrPN+Sgnguv5JadUgzskR4JYF1Z8ddSMjkYVDYp9DcX4X/r8Dzexve8ZigItqot+r7J3uSGMYf9GEpBa0ksDTX+givkcjPYw0oiZ+0+gPfvYt+NYabO951C0sEBtJFzB0HTraQWJgV277AaXLgc4FLXgCKu5Px6UDv+tYLhKPvxSD+4CMsUtBK8hE26HjWZ8m9oSVVKEaDfAB+/w4Vd36ey5xesCz+VsYHxta3LE4+fI2CXvHMMg4JMveCTEkGhfy7cLk1BvvjdW90uR1a0jJ4PpeXafbbTSAgnjIfQtFI5PcHMZxKAYXLENkpNcpHWI3ACqUfHPYH/Oo1GA1HoMznxe5+82JLK8YHFXtnqOWqFrSMG9Sb8sOUgRPPQCPxvIh12VKc6MYDupl3RKF4CcjguEsM4wtiOJWCUhL/toMviogJnIc8l/T+mjXKlgwaStir4Pfi8Zjx9kWeO8tx7hdKvqoNLvM+17Iwk1CDUqAVrcNFRijAP0QeHrwx4ofwAx5VWnk+wgUnDP+H/TWKiEnxa/wgoKz/63bK+9a8ppwY8gctToPd74p6jUDLuMHhdSUMKKYIF1TF5g5dgNbi/AGFKQrGS0gG+z/FMCLEMCpVqSToyn72EY4n+vmEmgAX5z7Rr0YRwS3ns4+CIEAZh1TFYN+8BhcaqTsMoJ7QvhGpvPQlCseL2sjT/H7vFSHyqxRQSTgh9xHGi8Kj7ePEcNVBRMzE1ixOPNol+tcowPTOYYmhCc3c2Rzhx1fiWuACK9DbeHMjKs4T2jdQAaJwfFHEnco3l3xB5FXJr5Juikn/kw9+gZSPkNQEoDcYq5XDaJ8o+tcoWuEdqp6CeF2nhkYCvgICZui/85XzZ7iWdZTn8QfoPt/WC0lP0JI/57+FxCDyqaRTEn6NLIJb6AxE4pfKLgW8ceLv+0o1hwz8hpBWEC8F0O4uS3qR/S9c3i0i3yV9ku8y3cPz+QMMqCtFQQUidavDDTV+tuinUiXEuQwUezTQCrcvwq+MifnzBTzLARXzfdFdBOQBP+ZF4xb9LgtwK5lLUFuby3MqllzRwi7XKGtY2nHjF2BiN1mLIAAg7v2iwGqbIoK8kgfKNwOXhJRnaUue0+zIx01Pp/Qqf/SafnbIE7/uY1yXBZHG9BUsUfwIIXPH1/Fx1o3rdYWubgm0ZWXL36IfuBdpEQRGoyp2SC8zBX+PH5SpDKgcKueroKDF7MQuGE7rc50mM+NDpbP4L/t4xIBvOXCF+o73A8VQywW6vU9QSbnZlo5QkM7Yqni+KoBfG/uPXoCXlSpDWSv8DqxXKNN/cAzmphvfu+lb59LXPC/ErfUO/j7IdVnAEoVa4nW2DhTyMH6GgGUc3Qpc5mS655IlhXYCKDp9lA9h1jhB7T6SkhL8zSkicpXJO33VR+ne5f/x+CY25dLRvhVYKwDlfM0SD2+nfA0TsTanUxTLbK7Tor1EVoAHCMHtq4XK/ajBI6MxpKWZ/TVJYHGdxW/4iSlWBdyCyHNJ74AyzsI0Y/e6TKknuhcsMbXD3YB1b3t2Y6MMjpmeNH1/C/eyIcow/jatsEbHbt4P50poOOAzbiNjYTxdgvwuzxsscPANx8+J+hB2qEStwuiqz7L7A3Rnp3BBmSsTtp5s9CvMkYfyvJDeKS3dKpaXLgsioatjGWjTxrMigOe/ccERMv8hV4iPsAa68e2DLO+ChAr8fix+CxbM68XqOYkDaHFG8CvqmDeD4wz6KTz4Zc60+JiYlOZifKEA8p7m5t4/gh4inZWR34VF3HTHtBu4/Bzk/WoNUUb7s57aaac1CQHWzjxNOS75TFGmsjvpxks4wCJC9005MQFPt9ZVaNsR3PmNH9+La4FWLZTb6y4jaLF5mpJC+ChKDcNr0w6tN23OBIW4kJdl1j46gm9esKO3uU5pKp4BYH71CaCklawc8HsAJ+zojpudbu5Gf/rZVU029Fu7fs96XHZAC9Ju+Ygypmcyd5i8aq+B4JvZrGXlZnfthm64lVG0zNyJ8dQn4HgLytrI9RZ/KViWcHOhy5zIeMCSW8fkEh7Eu1yXFTgYcq2J4IdymV9htpxG91qUMalkA1h+6P79MsN16FbgNI/wxFT/oFwool5Xw23JeC8Ye75YfUUBmfEsWBq8D9rjJBb7a7ZMQjIy8KQnnVOgsnje+ghcZYGy7MfJLXMDxRxg8oAWpV3vc2WhfDxe27jjvQpyTCPwbDQ+5+fggQ3FcIAxi05s1c+6fQ1uPfhwdQFQwR6CbjtZdBfhXiqrH7AXJ+D4Mfuqd5VrDWDmzmGZa9UhtS/vB332E6z/VgnPVeMhQrwNnw7CPH9dAeRvmJJf6f9EP3+A8pdrSopO7y/6X2k0AiPiHOuH27YdS090MkBhm63N6nrb+hVdb8RnqKVfghDuVMYr5eox+H0G6JuqXoiuTWgt3yWXFGV2CWjo4F6Vp5tzbBD96wTwxKmWSYNjr+iPp2gKlePHzxZkS0fh930UwNrXFYMC17yo0rKlX8FSShDDX25AdzwADICfoSv+Ic/VjVpq+HIyGjjM7MajamI4FY0iuM8O+dqYrDPg9/Ij8RCHsHBZ4FIWH1krggngS/S/MpPHVkUPu7td5n/A3Mqr27xcULtdgtaa8l9equRFLsb/WKHo5U6gOLwdEvi/48+44yqG1s3hgjN39qPOgu+XxXU9BLSSIVDQDSgINByUeQftUrTtDNwCyHVJl/fAhjrBdGdJd9H0YQLOPCB/eHkTyVtuvgOU9ypzh5ak7TwryGjMt6A2Rrvk7V9HccOfbDfx29W+ToAWKK9t/ozPeKKICilHpuYqfgOJ1uQs+UM+TF6WNAvci8B9Ct4FzvsFizylgtD3pkApO5k75OdlmqZLpldpF2Z1wxUErDhz3fROWWklhBvI+Bmg5ezheo5y0b9OIzw63cRlHudPXgJHqOelE9WuxdOKXPIJVUndef7cbEsHdF8PVhe2RKAcekulE7cNTAvd2ebZaJggL/jdB4KdVYj3zmVLz0G4cjaXYffwqUpwsfg5I4Fef5aPS1su6X7q55RWMj6GSKPN60Xoa7nTU/UGkQabzUtRRscakQdqbAwI5Te2jASC+VgRlKQ7IevGL7qAH149ADU+G5+VMHTCvD93iaU1uhXih0XUt+/yXabpuNqBz9q3/mC+psaHbyRiZRhL/2dJ42ja2dJf6X+ntM/P2xKNoNJt4ssWXl+6OV+AwswSWtRmkUd9Uw4/BvIbFZpTOujr5TNw34L+GWAlggC3MSWpYZSuE4XslMoKlpq64DN+CQY/VK/y0K1+fMEAf9V0S9Fv02rtQAn9n+s0Dc7PMv1ZTdoDnLgrWyBamVobq/eZ7ToF6Bam8YXCI1biPIqBv4tHBLYIbDXQBdG33UGgLyM/VYxLXr1OaZXYCl/JXWlppSqMHtoEtyP4H+dg2OJofC55t2b2K8ppht9egt8zBTmmLt6pK1MMz1xQoagYR61YoLWC8A72cV4tCgsY7YgV+QJBEab8V7xHDg+4oBvu5aiK2Qu/lYWubkO8+F0yXUtcm2OKVvmOfQKGALoVqCd6QEE2VdEXsdWx8DwiDOkpYv5bxthjRL56Dzy5KR5SBPP1f8OC2GtxZ0pxao33umtcXcEgeK4PFIhvGp7Uwij8edp/p7K9wG4OA2vxxS3LEq4rzKFK+hPjE4CTVG3bgebZ4CjB+aDI+LsBzsT5w5WUDPZStAZFXh759EUAj4AZ8MYRtRX0UK04bR1QbUm/sP9ocPBxFDhl97psq3ZRrojWPlqPr5WU3yvwXN0qUQAwIO9oGzPF77Y6Ggx4lSbv5la36tfj/a/aKoZ5DPUDQwSUVAFK0Sw0aD3aa6T+ENl+0h2Ql+Ni/urOtkMtgr5A5nn7jSN7Eb4iKfL7A+7pbHzTfAs+Q2sal58tP7329diogmWmW0XeQGhlsBv8nJ49hVcIiPz/TYA5h+MFH4LBwy3FOGAjjxioxqDshU2lZ/D0eaiMMNoCXhjy34WYlOb4Rp8PQalkz1Rm9DWwiZaY2BRbBnRfu/TpKMoBWoUHbcSgDQjDQ5CDr4J5VMB3lZS5iu19XNFo3cFuueOOaT7v4UbgqR3owhIi6GUa9i8g7vNifN7KsS8MS8yo+6vYdQWRMQ678m6RTpg1TPR1G5/zowYEi0Q89G5/U31pzIeQq0P20+Ht04c2dGmXCWgVQgubDvQvtRWc8qHASvp2ncF+Ep73RdAvx9inXv5XIWse/w+foSb5XBuwgQAAAABJRU5ErkJggg==>