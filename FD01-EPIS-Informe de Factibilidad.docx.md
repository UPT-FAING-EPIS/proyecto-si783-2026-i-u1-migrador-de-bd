![C:\\Users\\EPIS\\Documents\\upt.png][image1]

**UNIVERSIDAD PRIVADA DE TACNA**

**FACULTAD DE INGENIERÍA**

**Escuela Profesional de Ingeniería de Sistemas**

 **Proyecto Migrador DB**

Curso: *Base de Datos II*

Docente: *Mag. Patrick Cuadros Quiroga*

Integrantes:

***Halanocca Rojas, Usher Damiron (2023076795)***  
***LLica Mamani, Jimmy Mijair (2023076789)***

**Tacna – Perú**  
***2026***

# **Sistema *Migrador de DB***

# **Informe de Factibilidad**

# 

# **Versión *1.0***

| CONTROL DE VERSIONES |  |  |  |  |  |
| :---: | :---: | :---: | :---: | :---: | ----- |
| Versión | Hecha por | Revisada por | Aprobada por | Fecha | Motivo |
| 1.0 | UHR / JLM  | PCQ  | PCQ  | 29/04/2026  | Versión Original |

**INDICE GENERAL**

[1\.	Descripción del Proyecto	3](#descripción-del-proyecto)

[2\.	Riesgos	3](#riesgos)

[3\.	Análisis de la Situación actual	3](#análisis-de-la-situación-actual)

[4\.	Estudio de Factibilidad	3](#estudio-de-factibilidad)

[4.1	Factibilidad Técnica	4](#factibilidad-técnica)

[4.2	Factibilidad económica	4](#factibilidad-económica)

[4.3	Factibilidad Operativa	4](#factibilidad-operativa)

[4.4	Factibilidad Legal	4](#factibilidad-legal)

[4.5	Factibilidad Social	5](#factibilidad-social)

[4.6	Factibilidad Ambiental	5](#factibilidad-ambiental)

[5\.	Análisis Financiero	5](#análisis-financiero)

[6\.	Conclusiones	5](#conclusiones)

**Informe de Factibilidad**

1. Descripción del Proyecto  
   1. Nombre del proyecto

     Sistema Migrador de Base de Datos 

2. Duración del proyecto

   El proyecto tendrá una duración estimada de 4 meses, correspondiente al período académico del curso de Base de Datos II, con inicio en marzo de 2026 y finalización en junio de 2026\. 

   3. Descripción 

   El Sistema Migrador de Base de Datos consiste en una aplicación de escritorio desarrollada en Python que automatiza el proceso de migración de datos entre 21 motores de bases de datos diferentes. El sistema implementa un proceso ETL (Extract, Transform, Load) completo que incluye: descubrimiento automático de esquemas, generación de scripts DDL, conversión de tipos de datos entre motores, migración de datos por lotes, validación post-migración y generación de reportes.

   La importancia del proyecto radica en que actualmente las organizaciones enfrentan grandes desafíos al migrar bases de datos entre diferentes plataformas, requiriendo conocimientos técnicos especializados, invirtiendo entre 3 a 5 días hábiles por migración, y exponiéndose a errores humanos en la conversión de tipos de datos. El sistema propuesto reduce el tiempo de migración en al menos un 60%, elimina errores de conversión manual y proporciona una interfaz gráfica intuitiva accesible para usuarios sin conocimientos técnicos avanzados.

   El proyecto se desarrolla en el contexto académico del curso de Base de Datos II de la Universidad Privada de Tacna, como una solución de código abierto que puede ser utilizada por la comunidad de ingeniería de sistemas.

1.4 Objetivos

       1.4.1 Objetivo general

Desarrollar un sistema de migración de bases de datos automatizado que permita transferir datos y estructuras entre 21 motores de bases de datos diferentes, reduciendo el esfuerzo manual y garantizando la integridad de la información. 

        1.4.2 Objetivos Específicos  
  	 

| Código | Objetivo Específico | Resultado Esperado |
| :---- | :---- | :---- |
| OE01 | Implementar un módulo de conexión multi-motor utilizando SQLAlchemy | Soporte verificado para los 21 motores de bases de datos |
| OE02 | Desarrollar un descubridor automático de esquemas | Detección del 100% de tablas, columnas, PKs y FKs de la BD origen |
| OE03 | Implementar un motor de transformación con conversión de tipos | Migración exitosa entre motores heterogéneos (ej. MySQL a MongoDB) |
| OE04 | Diseñar una interfaz gráfica intuitiva con Tkinter | Usuarios sin capacitación completan una migración en menos de 10 minutos |
| OE05 | Implementar sistema de puntos de control | Reanudación de migración desde la última tabla completada tras interrupción |
| OE06 | Desarrollar validador post-migración | Detección del 100% de diferencias en conteos de registros |

2. Riesgos

| ID | Riesgo | Probabilidad | Impacto | Estrategia de Mitigación |
| :---- | :---- | :---- | :---- | :---- |
| R01 | Incompatibilidad de drivers con versiones de Python | Media | Alto | Utilizar Python 3.8+ y probar compatibilidad de cada driver al inicio |
| R02 | Fallos de conexión con bases de datos remotas | Alta | Medio | Implementar reintentos con backoff exponencial y pruebas locales con SQLite |
| R03 | Diferencias inesperadas en tipos de datos entre motores | Media | Alto | Documentar tabla de mapeo de tipos y permitir configuración manual |
| R04 | Sobrecarga de memoria con grandes volúmenes de datos | Media | Alto | Implementar procesamiento por lotes configurables |
| R05 | Limitaciones de tiempo del período académico | Alta | Alto | Priorizar requerimientos de alta prioridad y usar metodología iterativa |
| R06 | Falta de acceso a motores de BD específicos para pruebas | Media | Medio | Probar con SQLite local y simular conexiones para motores no disponibles |

3. Análisis de la Situación actual  
   1. Planteamiento del problema

Antecedentes:

Actualmente, las organizaciones que necesitan migrar sus bases de datos entre diferentes motores realizan el proceso de manera predominantemente manual. El flujo de trabajo típico incluye:

1. Análisis manual de la estructura de la base de datos origen  
2. Escritura manual de scripts DDL para recrear la estructura en el destino  
3. Conversión manual de tipos de datos entre motores  
4. Extracción de datos mediante consultas SQL personalizadas  
5. Transformación de datos en herramientas externas (Excel, scripts Python)  
6. Inserción manual de datos en la base de datos destino  
7. Verificación manual de conteos y corrección de errores

Problemática identificada:

* Procesos manuales propensos a errores: La intervención humana en cada etapa introduce riesgos de pérdida o corrupción de datos  
* Tiempo excesivo: Una migración de base de datos mediana consume entre 3 a 5 días hábiles  
* Requisito de conocimientos especializados: Se necesita experiencia en ambos motores de bases de datos  
* Falta de trazabilidad: No existe registro centralizado del proceso de migración  
* Herramientas limitadas: Las soluciones existentes suelen ser específicas para ciertos motores o requieren licencias costosas

Necesidad que será resuelta:

El Sistema Migrador de Base de Datos automatizará completamente el proceso ETL, permitiendo que usuarios sin conocimientos especializados puedan ejecutar migraciones complejas en minutos en lugar de días, con validación automática de integridad y generación de reportes detallados.

2. Consideraciones de hardware y software

Hardware requerido:

| Componente | Especificación Mínima | Especificación Recomendada |
| :---- | :---- | :---- |
| Procesador | Intel Core i3 o equivalente | Intel Core i5 o superior |
| Memoria RAM | 4 GB | 8 GB o superior |
| Almacenamiento | 500 MB libres | 2 GB libres (para logs y reportes) |
| Red | Conexión a red local | Conexión estable a servidores de BD |

Software requerido:

| Componente | Especificación |
| :---- | :---- |
| Sistema Operativo | Windows 10/11 o Linux Ubuntu 20.04+ |
| Python | Versión 3.8 o superior |
| Bibliotecas Python | SQLAlchemy 2.0, Pandas 2.1, Pydantic 2.5, PyYAML 6.0, Structlog 23.2 |
| Drivers de BD | Según motores a utilizar (psycopg2, mysql-connector, pymongo, etc.) |
| Control de versiones | Git |

Tecnologías seleccionadas:

| Tecnología | Justificación |
| :---- | :---- |
| Python 3.8+ | Lenguaje versátil con amplio soporte de bibliotecas para bases de datos |
| SQLAlchemy 2.0 | ORM maduro que proporciona abstracción unificada para múltiples motores |
| Pandas 2.1 | Biblioteca optimizada para manipulación y transformación de datos |
| Tkinter | Biblioteca estándar de Python para GUI, sin dependencias externas |
| PyYAML | Formato legible para archivos de configuración |
| Structlog | Logging estructurado con salida JSON para trazabilidad |

4. Estudio de Factibilidad

El estudio de factibilidad se realizó mediante el análisis de los recursos tecnológicos disponibles, la evaluación de costos de desarrollo, la identificación de requisitos operativos y la verificación de cumplimiento legal. El presente estudio fue aprobado por el equipo de desarrollo y revisado por el docente del curso. 

1. Factibilidad Técnica

Evaluación de tecnología existente:

El proyecto se desarrollará utilizando tecnologías de código abierto ampliamente probadas en la industria:

| Recurso Técnico | Disponibilidad | Aplicabilidad |
| :---- | :---- | :---- |
| Python 3.8+ | Disponible gratuitamente | Lenguaje principal de desarrollo |
| SQLAlchemy | Biblioteca de código abierto | Abstracción de conexión a 21 motores de BD |
| Pandas | Biblioteca de código abierto | Procesamiento eficiente de datos por lotes |
| Tkinter | Incluido en Python estándar | Interfaz gráfica sin dependencias externas |
| Equipos de desarrollo | Computadoras personales del equipo | Suficientes para desarrollo y pruebas |

Infraestructura técnica:

El sistema no requiere servidores dedicados ni infraestructura en la nube. Se ejecuta localmente en la máquina del usuario y se comunica directamente con las bases de datos origen y destino a través de la red. Para las pruebas, se utilizará SQLite como base de datos embebida, eliminando la necesidad de instalar motores adicionales durante el desarrollo.

2. Factibilidad Económica

El propósito del estudio de viabilidad económica es determinar los beneficios del proyecto en contraposición con los costos. Dado que el sistema se desarrolla como proyecto académico, los costos se limitan al tiempo de programación del equipo. 

1. Costos Generales 

| Concepto | Cantidad | Costo Unitario (S/) | Costo Total (S/) |
| :---- | :---- | :---- | :---- |
| Computadora portátil (2 unidades) | 2 | 2,500.00 | 5,000.00 |
| Material de oficina (papeles, útiles) | 1 lote | 100.00 | 100.00 |
| Total Costos Generales |  |  | 5,100.00 |

   2. Costos operativos durante el desarrollo 

| Concepto | Costo Mensual (S/) | Meses | Costo Total (S/) |
| :---- | :---- | :---- | :---- |
| Electricidad | 50.00 | 4 | 200.00 |
| Internet | 80.00 | 4 | 320.00 |
| Transporte | 60.00 | 4 | 240.00 |
| Total Costos Operativos |  |  | 760.00 |

      3. Costos del ambiente

| Concepto | Costo (S/) |
| :---- | :---- |
| Sistema operativo (Windows/Linux) | 0.00 (software existente) |
| IDE de desarrollo (VS Code) | 0.00 (gratuito) |
| Git y GitHub | 0.00 (gratuito) |
| Bibliotecas Python | 0.00 (código abierto) |
| Total Costos del Ambiente | 0.00 |

      4. Costos de personal

|     Rol | Responsable | Horas Semanales | Semanas | Costo Hora (S/) | Costo Total (S/) |
| :---- | :---- | :---- | :---- | :---- | :---- |
| Desarrollador Backend | Halanocca Rojas, Usher Damiron | 15 | 16 | 15.00 | 3,600.00 |
| Desarrollador Frontend | LLica Mamani, Jimmy Mijair | 15 | 16 | 15.00 | 3,600.00 |
| Total Costos de Personal |  |  |  |  | 7,200.00 |

Horario de trabajo: Lunes a viernes, 3 horas diarias (7:00 PM \- 10:00 PM) 

5. Costos totales del desarrollo del sistema 

| Rubro | Costo (S/) |
| :---- | :---- |
| Costos Generales | 5,100.00 |
| Costos Operativos | 760.00 |
| Costos del Ambiente | 0.00 |
| Costos de Personal | 7,200.00 |
| COSTO TOTAL DEL PROYECTO | 13,060.00 |

El costo total del proyecto asciende a S/ 13,060.00, financiado por el equipo de desarrollo como parte de su inversión educativa. No se requiere inversión externa ni pago de licencias. 

3. Factibilidad Operativa

Beneficios del producto:

* Reducción del tiempo de migración en al menos 60%  
* Eliminación de errores humanos en conversión de tipos de datos  
* Interfaz gráfica intuitiva que no requiere capacitación extensa  
* Validación automática de integridad de datos  
* Generación de reportes para auditoría

Capacidad de mantenimiento:

El sistema está diseñado con arquitectura modular que facilita:

* Corrección de errores en módulos específicos sin afectar el sistema completo  
* Adición de nuevos motores de bases de datos sin modificar el núcleo  
* Documentación en español para facilitar el mantenimiento

Lista de interesados:

| Interesado | Rol | Impacto |
| :---- | :---- | :---- |
| Administradores de BD | Usuarios principales | Alto \- Usan el sistema diariamente |
| Ingenieros de software | Usuarios secundarios | Medio \- Usan el sistema ocasionalmente |
| Comunidad de desarrollo | Beneficiarios indirectos | Medio \- Acceden al código abierto |
| Docente del curso | Evaluador académico | Alto \- Califica el proyecto |

4. Factibilidad Legal

El proyecto no presenta conflictos con restricciones legales:

* Licencias de software: Todas las tecnologías utilizadas son de código abierto con licencias permisivas (MIT, BSD, Apache 2.0)  
* Protección de datos: El sistema no almacena datos de usuarios finales; solo procesa credenciales de conexión que se protegen mediante variables de entorno  
* Ley de Protección de Datos Personales (Ley N.º 29733): El sistema cumple al no almacenar datos personales y proteger las credenciales de acceso  
* Propiedad intelectual: El código fuente es original del equipo de desarrollo y se distribuye bajo licencia MIT

  5. Factibilidad Social

El proyecto tiene un impacto social positivo:

* Democratización tecnológica: Proporciona una herramienta gratuita que reduce la barrera de entrada para migraciones de bases de datos  
* Educación: Sirve como material de estudio para estudiantes de ingeniería de sistemas  
* Código abierto: La licencia MIT permite que la comunidad pueda mejorar y extender el sistema  
* Ética profesional: El desarrollo sigue principios de transparencia, calidad y responsabilidad

No se identifican impactos sociales negativos. El proyecto no genera exclusión digital ni afecta a comunidades vulnerables.

6. Factibilidad Ambiental

El proyecto es ambientalmente viable y no genera impactos negativos significativos en el medio ambiente. 

5. Análisis Financiero

   1. Justificación de la Inversión

*5.1.1 Beneficios* del Proyecto

Beneficios tangibles:

| Beneficio | Descripción | Estimación |
| :---- | :---- | :---- |
| Reducción de tiempo | Migración de 5 días a 2 horas en promedio | Ahorro de 38 horas-hombre por migración |
| Reducción de errores | Eliminación de correcciones manuales post-migración | Ahorro de 8 horas-hombre por migración |
| Reutilización | El sistema puede usarse para múltiples migraciones | Sin costo adicional por migración |

Beneficios intangibles:

| Beneficio | Descripción |
| :---- | :---- |
| Confiabilidad | Validación automática que garantiza integridad de datos |
| Trazabilidad | Registro completo del proceso de migración para auditoría |
| Accesibilidad | Interfaz gráfica que permite a personal no técnico ejecutar migraciones |
| Escalabilidad | Arquitectura modular que permite agregar nuevos motores de BD |
| Valor educativo | Material de referencia para estudiantes de ingeniería de sistemas |

5.1.2 Criterios de Inversión  
   
*5.1.2.1 Relación Beneficio/Costo (B/C)*

Para un escenario de uso típico (10 migraciones al año):

| Concepto | Valor |
| :---- | :---- |
| Costo del proyecto | S/ 13,060.00 |
| Ahorro por migración (46 horas-hombre a S/ 15.00) | S/ 690.00 |
| Ahorro anual (10 migraciones) | S/ 6,900.00 |
| Relación B/C (primer año) | 0.53 |
| Relación B/C (2 años) | 1.06 |

Considerando que el sistema tiene una vida útil estimada de 3 años sin modificaciones mayores:

| Período | Beneficio Acumulado | Relación B/C |
| :---- | :---- | :---- |
| Año 1 | S/ 6,900.00 | 0.53 |
| Año 2 | S/ 13,800.00 | 1.06 |
| Año 3 | S/ 20,700.00 | 1.59 |

Interpretación: La relación B/C supera 1 a partir del segundo año, indicando que el proyecto es rentable a mediano plazo.

                    *5.1.2.2 Valor Actual Neto (VAN)*

Considerando una tasa de descuento del 10% (costo de oportunidad del capital):

| Año | Flujo de caja | Factor de Descuento (10%) | Valor Presente |
| :---- | :---- | :---- | :---- |
| 0 | 0 | 1.000  | \-S/ 13,060.00  |
| 1 | 0 | 0.909 | S/ 6,272.10  |
| 2 | S/ 6,900.00  | 0.826 | S/ 5,699.40 |
| 3 | S/ 6,900.00  | 0.751 | S/ 5,181.90  |
| VAN |  |  | S/ 4,093.40  |

Interpretación: El VAN es mayor que cero (S/ 4,093.40), lo que indica que el proyecto genera valor positivo y se acepta.

*5.1.2.3 Tasa Interna de Retorno (TIR)*

La TIR calculada para el flujo de caja del proyecto es aproximadamente 27.5%.

Considerando un Costo de Oportunidad del Capital (COK) del 10%:

| Indicador | Valor | Condición | Resultado |
| :---- | :---- | :---- | :---- |
| TIR | 27.5% | TIR \> COK (27.5% \> 10%) | Se acepta el proyecto |

Interpretación: La TIR supera significativamente el costo de oportunidad, confirmando la rentabilidad del proyecto.

6. Conclusiones  
* Factibilidad técnica confirmada: El proyecto utiliza tecnologías de código abierto maduras y disponibles gratuitamente. El equipo posee las competencias técnicas necesarias para el desarrollo. No se requiere inversión en infraestructura adicional.  
* Factibilidad económica demostrada: El costo total del proyecto asciende a S/ 13,060.00, financiado por el equipo como inversión educativa. Los criterios financieros (B/C \> 1 a partir del año 2, VAN positivo de S/ 4,093.40, TIR de 27.5% \> COK del 10%) confirman la rentabilidad del proyecto a mediano plazo.  
* Factibilidad operativa asegurada: Los usuarios objetivo pueden adoptar el sistema sin cambios significativos en sus procesos. La interfaz gráfica intuitiva minimiza la necesidad de capacitación. La arquitectura modular facilita el mantenimiento y la extensibilidad.  
* Factibilidad legal verificada: El proyecto cumple con la Ley de Protección de Datos Personales (Ley N.º 29733\) y utiliza exclusivamente software de código abierto con licencias permisivas. No existen conflictos legales identificados.  
* Factibilidad social favorable: El proyecto genera beneficios para la comunidad técnica al proporcionar una herramienta gratuita y de código abierto para migración de bases de datos, democratizando el acceso a esta tecnología.  
* Factibilidad ambiental positiva: El proyecto minimiza el impacto ambiental al eliminar el uso de papel, reducir el consumo energético mediante automatización, y no requerir hardware especializado.

[image1]: <data:image/png;base64,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>