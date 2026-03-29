<center>

[comment]: <img src="./media/media/image1.png" style="width:1.088in;height:1.46256in" alt="escudo.png" />

![./media/media/image1.png](./media/logo-upt.png)

**UNIVERSIDAD PRIVADA DE TACNA**

**FACULTAD DE INGENIERIA**


**Escuela Profesional de Ingeniería de Sistemas**



**Proyecto *Migrador de DB***

Curso: *Base de Datos II*

Docente: *Mag. Patrick Cuadros Quiroga*

Integrantes:

***Halanocca Rojas, Usher Damiron (2023076795)***

***LLica Mamani, Jimmy Mijair (2023076789)***



**Tacna – Perú**

***2026***


</center>
<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

Sistema *DBMigrator*

Informe de Factibilidad

Versión *1.0*

|CONTROL DE VERSIONES||||||
| :-: | :- | :- | :- | :- | :- |
|Versión|Hecha por|Revisada por|Aprobada por|Fecha|Motivo|
|1\.0|MPV|ELV|ARV|10/10/2026|Versión Original|

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

# **INDICE GENERAL**

[1. Descripción del Proyecto](#_Toc52661346)

[2. Riesgos](#_Toc52661347)

[3. Análisis de la Situación actual](#_Toc52661348)

[4. Estudio de Factibilidad](#_Toc52661349)

[4.1 Factibilidad Técnica](#_Toc52661350)

[4.2 Factibilidad económica](#_Toc52661351)

[4.3 Factibilidad Operativa](#_Toc52661352)

[4.4 Factibilidad Legal](#_Toc52661353)

[4.5 Factibilidad Social](#_Toc52661354)

[4.6 Factibilidad Ambiental](#_Toc52661355)

[5. Análisis Financiero](#_Toc52661356)

[6. Conclusiones](#_Toc52661357)


<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

**<u>Informe de Factibilidad</u>**

**1. <span id="_Toc52661346" class="anchor"></span>Descripción del Proyecto**

    1.1. Nombre del proyecto
         Sistema DBMigrator: Sistema de Migración de Bases de Datos Heterogéneas

    1.2. Duración del proyecto

        El proyecto tendrá una duración estimada de 4 meses, comprendiendo las fases de análisis, diseño, desarrollo, pruebas e implementación.

    1.3. Descripción

        El presente proyecto consiste en el desarrollo de un sistema capaz de migrar bases de datos entre diferentes gestores, tales como MySQL, Oracle y SQL Server.
        La necesidad de este sistema surge debido a que muchas organizaciones trabajan con múltiples sistemas de gestión de bases de datos, lo que dificulta la interoperabilidad, la migración de información y la                   modernización tecnológica.
        El sistema permitirá convertir estructuras (tablas, relaciones, tipos de datos) y datos de un motor de base de datos a otro, garantizando integridad, compatibilidad y eficiencia en el proceso.
        Este proyecto se desenvolverá en el contexto de la gestión de datos empresariales, facilitando la transición entre tecnologías y reduciendo el esfuerzo manual requerido en procesos de migración.

    1.4. Objetivos


        1.4.1 Objetivo general
           Desarrollar un sistema que permita la migración eficiente y segura de bases de datos entre diferentes gestores, garantizando la integridad y consistencia de la información.
   
        1.4.2 Objetivos Específicos
            Analizar las diferencias estructurales entre los sistemas gestores de bases de datos (MySQL, Oracle, SQL Server).
            Diseñar un módulo de conversión de estructuras de base de datos.
            Implementar la transferencia de datos entre distintos motores de bases de datos.
            Validar la integridad de los datos migrados.
            Desarrollar una interfaz que facilite el uso del sistema por parte del usuario.

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

**2. <span id="_Toc52661347" class="anchor"></span>Riesgos**


    | **Categoría** | **Riesgo** | **Probabilidad** | **Impacto** | **Estrategia de Mitigación** |
    | :--- | :--- | :--- | :--- | :--- |
    | **Técnico** | Incompatibilidad de tipos de datos entre el origen y el destino no documentada. | Media | Alto | Realizar un mapeo de tipos de datos exhaustivo en la fase de análisis. Implementar un módulo de transformación (FieldMapper) flexible y configurable. |
    | **Técnico** | Volumen de datos excesivo que causa tiempos de migración prolongados o fallos por timeout. | Alta | Alto | Implementar extracción y carga por lotes (BatchExtractor). Utilizar `executemany` para inserciones masivas. Optimizar queries y utilizar pooling de conexiones. |
    | **Técnico** | Fallo de conexión de red durante la migración, dejando el proceso incompleto. | Media | Medio | Implementar un robusto sistema de checkpoint y reanudación. Incluir mecanismos de reintentos con backoff exponencial. |
    | **Organizativo** | Cambio en el alcance o los requerimientos a mitad del desarrollo. | Media | Medio | Mantener una comunicación fluida con los stakeholders. |
    | **Personal** | Rotación de personal o indisponibilidad de los integrantes clave. | Baja | Alto | Fomentar el pair programming y mantener una documentación actualizada del código y la arquitectura. Asegurar que el conocimiento no esté centralizado. |
    | **Cumplimiento** | Incumplimiento de normativas de protección de datos (ej. Ley N° 29733, Perú) al migrar datos sensibles. | Baja | Alto | Incluir funciones de sanitización y anonimización de datos en el pipeline. Asegurar que las conexiones sean seguras (SSL/TLS) y los logs no contengan información sensible. |


<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

**3. <span id="_Toc52661348" class="anchor"></span>Análisis de la Situación actual**
    
    3.1. Planteamiento del problema


        Actualmente, las organizaciones enfrentan el desafío de gestionar información distribuida en diversos sistemas de bases de datos. La migración de datos entre estos sistemas es una tarea compleja, manual y costosa que implica:
            - **Riesgo de pérdida de datos:** Los procesos manuales de exportación/importación son propensos a errores humanos, como el truncamiento de datos, la pérdida de relaciones o la corrupción de la información.
            - **Alto costo de tiempo y recursos:** Los equipos de TI dedican semanas o meses a planificar, ejecutar y validar migraciones, desviando su atención de tareas críticas para el negocio.
            - **Falta de estandarización:** No existe una herramienta única y gratuita que gestione eficazmente la migración entre múltiples motores (MySQL, PostgreSQL, SQL Server) con un enfoque de código abierto y configurable.
            - **Dificultad en la modernización:** La dependencia de un motor de base de datos específico limita la capacidad de la organización para modernizar su infraestructura tecnológica y adoptar soluciones más eficientes o rentables.
        
        Este proyecto nace para proporcionar una solución automatizada, confiable y extensible que mitigue estos problemas, permitiendo migraciones seguras y eficientes con un costo mínimo.


    **3.2. Consideraciones de hardware y software**

    - **Hardware Existente y Alcanzable:**
      - **Servidor de Desarrollo:** Un equipo de escritorio o laptop con procesador Intel Core i5 o superior, 16GB de RAM y 256GB de SSD. Será suficiente para las fases de desarrollo y pruebas con volúmenes de datos moderados.
        - **Servidores de Base de Datos:** Se utilizarán instancias locales o virtualizadas de los motores de base de datos (MySQL, PostgreSQL, SQL Server) para las pruebas de integración.
        - **Red:** Acceso a una red local de alta velocidad para las pruebas de migración y conectividad entre los diferentes componentes.
            
    - **Software a Utilizar en el Desarrollo:**
      - **Lenguaje de Programación:** Python 3.9+ por su amplia gama de bibliotecas para conectividad de bases de datos (`SQLAlchemy`), procesamiento de datos (`pandas`) y desarrollo rápido.
      - **Sistema Operativo:** Windows, Linux (Ubuntu) o macOS, dado que Python y las bibliotecas elegidas son multiplataforma.
      - **Bibliotecas Principales:**
        - `SQLAlchemy` y `psycopg2`, `mysql-connector-python`, `pyodbc` para la conectividad.
        - `pandas` para transformaciones de datos complejas.
        - `pydantic` para la validación de esquemas de datos.
        - `structlog` para logging estructurado.
        - `PyYAML` para la gestión de archivos de configuración.
        - **Control de Versiones:** Git y GitHub para la gestión del código fuente, issues y el plan de trabajo.
        - **Documentación:** Markdown para la documentación técnica y del usuario.

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

4. <span id="_Toc52661349" class="anchor"></span>**Estudio de
    Factibilidad**

    Describir los resultados que esperan alcanzar del estudio de factibilidad, las actividades que se realizaron para preparar la evaluación de factibilidad y por quien fue aprobado.

    4.1. <span id="_Toc52661350" class="anchor"></span>**Factibilidad Técnica**

    - **Evaluación de Tecnología Actual:** Los lenguajes y bibliotecas propuestos (Python, SQLAlchemy, pandas) son de código abierto, ampliamente utilizados y cuentan con una comunidad de soporte activa. Los gestores de bases de datos objetivo (MySQL, PostgreSQL, SQL Server) tienen drivers estables y bien documentados.
    - **Plan de Desarrollo Detallado:** Para asegurar un desarrollo ordenado y medible, el proyecto se ha dividido en 21 issues secuenciales, como se muestra a continuación:

    | # | Título | Descripción | Etiquetas | Dependencias |
    |---|--------|-------------|----------|--------------|
    | **Fase 1: Setup y Conectividad (Días 1-3)** |
    | 1 | Estructura base del proyecto | Crear estructura de carpetas, archivos iniciales, requirements.txt, y configuración inicial de logging. | `core-extract`, `documentation` | — |
    | 2 | Configuración de conexión a BD origen (SQLAlchemy) | Implementar clase `DatabaseConnector` con soporte para múltiples motores (PostgreSQL, MySQL, SQL Server). Usar YAML para credenciales. | `core-extract` | #1 |
    | 3 | Configuración de conexión a BD destino | Similar a #2, pero para el destino. Incluir pool de conexiones y timeouts. | `core-extract` | #1 |
    | 4 | Sistema de logging estructurado | Configurar `structlog` o `logging` con JSON para trazabilidad. Incluir nivel de detalle por módulo. | `core-extract`, `documentation` | #1 |
    | 5 | Definición de esquemas de transformación (Pydantic) | Crear modelos Pydantic para datos origen y destino. Validación de tipos y campos obligatorios. | `transform-load` | #1 |
    | **Fase 2: Extracción y Control de Estado (Días 4-6)** |
    | 6 | Extractor con manejo de lotes | Crear clase `BatchExtractor` que extraiga datos en lotes configurables. Debe soportar consultas SQL personalizadas. | `core-extract` | #2 |
    | 7 | Sistema de checkpoint | Guardar estado de extracción (último ID, timestamp, lote) en tabla de control o archivo. | `core-extract` | #6 |
    | 8 | Mecanismo de reanudación | Implementar lógica para reanudar desde el último checkpoint en caso de interrupción. | `core-extract` | #7 |
    | 9 | Manejo de errores en extracción | Capturar excepciones de red/BD, reintentos con backoff, y registrar errores sin detener todo el proceso. | `core-extract`, `bug` | #6 |
    | 10 | Utilidades de validación y sanitización | Funciones para validar tipos básicos, sanitizar strings, y verificar integridad de datos antes de pasar a transformación. | `core-extract` | #6 |
    | **Fase 3: Transformación y Mapeo (Días 7-10)** |
    | 11 | Motor de mapeo de campos | Implementar `FieldMapper` que permita definir mapeos campo a campo con transformaciones simples (renombrar, concatenar, etc.). | `transform-load` | #5 |
    | 12 | Transformaciones complejas con Pandas | Crear módulo `ComplexTransformer` para operaciones como agregaciones, joins, limpieza de datos y normalización. | `transform-load` | #11 |
    | 13 | Reglas de negocio personalizables | Permitir inyectar funciones de transformación definidas por el usuario. Ejemplo: cálculos de impuestos, formato de fechas. | `transform-load`, `enhancement` | #12 |
    | **Fase 4: Carga Optimizada y Validación (Días 11-14)** |
    | 14 | Estrategia de bulk insert | Implementar carga masiva usando `executemany` o métodos nativos del driver. Configurar tamaño de lote. | `transform-load` | #3, #13 |
    | 15 | Soporte para upsert | Lógica de actualización si existe, inserción si no. Usar cláusulas específicas según motor (ON CONFLICT, MERGE). | `transform-load` | #14 |
    | 16 | Optimización de queries de carga | Revisar índices, evitar bloqueos largos, usar transacciones por lotes. | `transform-load`, `enhancement` | #14 |
    | 17 | Validación post-migración | Comparar conteos de registros origen vs destino, verificar integridad referencial, y generar diferencias. | `transform-load`, `high-priority` | #15 |
    | **Fase 5: Integración, Reportes y Cierre (Días 15-16)** |
    | 18 | Pipeline orquestador | Script principal que ejecuta extracción → transformación → carga en secuencia, respetando checkpoints. | `core-extract`, `transform-load` | #8, #15 |
    | 19 | Generación de reporte de migración | Crear reporte HTML/JSON con métricas: filas extraídas, transformadas, cargadas, errores, tiempo total. | `documentation` | #9, #17 |
    | 20 | Pruebas de integración end-to-end | Probar con datos reales (muestra pequeña) y validar que el pipeline completo funcione correctamente. | `bug`, `high-priority` | #18 |
    | 21 | Documentación final | README actualizado, diagrama de arquitectura, guía de configuración y ejemplos de uso. | `documentation` | #20 |

    Este plan de trabajo secuencial garantiza que el desarrollo sea progresivo, controlable y se pueda realizar un seguimiento claro del avance. Todos los componentes tecnológicos seleccionados son maduros y adecuados para los objetivos planteados.

    4.2. <span id="_Toc52661351" class="anchor"></span>**Factibilidad Económica**


        El proyecto se considera económicamente factible debido a que los costos de desarrollo son bajos y los beneficios esperados (ahorro de tiempo y recursos en futuras migraciones) superan ampliamente la inversión.

        4.2.1. Costos Generales

            Son los gastos de oficina y materiales de consumo durante el desarrollo.
    
            | Concepto | Unidad | Cantidad | Costo Unitario (S/.) | Costo Total (S/.) |
            | :--- | :--- | :--- | :--- | :--- |
            | Papel Bond A4 (millar) | Millar | 1 | 30.00 | 30.00 |
            | Lapiceros | Unidad | 5 | 1.50 | 7.50 |
            | Cuaderno de apuntes | Unidad | 2 | 8.00 | 16.00 |
            | Impresiones (estimado) | Unidad | 100 | 0.50 | 50.00 |
            | **Total** | | | | **S/ 103.50** |

        4.2.2. Costos operativos durante el desarrollo

           Costos asociados al uso de servicios durante el periodo de desarrollo (4 meses).

            | Concepto | Unidad | Cantidad (meses) | Costo Unitario (S/./mes) | Costo Total (S/.) |
            | :--- | :--- | :--- | :--- | :--- |
            | Energía Eléctrica (estimado) | Mes | 4 | 100.00 | 400.00 |
            | Internet Banda Ancha | Mes | 4 | 80.00 | 320.00 |
            | **Total** | | | | **S/ 720.00** |

        4.2.3. Costos del ambiente

            Se asume que los equipos de cómputo (laptops/desktops) son de propiedad de los integrantes, por lo que no se incurre en un costo adicional. No se requiere la compra de servidores ni infraestructura de red compleja para la fase de desarrollo y pruebas, ya que se utilizarán entornos virtualizados y servicios en la nube con capa gratuita.

            | Concepto | Observación | Costo Total (S/.) |
            | :--- | :--- | :--- |
            | Equipos de Desarrollo | Propiedad de los desarrolladores. | 0.00 |
            | Licencias de Software | Todo el software es de código abierto. | 0.00 |
            | **Total** | | **S/ 0.00** |

        4.2.4. Costos de personal

            Este proyecto será desarrollado por los dos integrantes del equipo como parte de su formación académica, por lo que no se incurre en un costo de personal real para el proyecto. Sin embargo, para el análisis financiero, se asigna un costo de oportunidad basado en una tarifa de mercado para desarrolladores junior.

            | Rol | Horas estimadas | Tarifa por hora (S/.) | Costo Total (S/.) |
            | :--- | :--- | :--- | :--- |
            | Desarrollador (Halanocca Rojas, U.) | 128 horas (4h/día x 32 días) | 20.00 | 2560.00 |
            | Desarrollador (LLica Mamani, J.) | 128 horas (4h/día x 32 días) | 20.00 | 2560.00 |
            | **Total** | | | **S/ 5120.00** |

        4.2.5.  Costos totales del desarrollo del sistema

            | Concepto | Costo (S/.) |
            | :--- | :--- |
            | Costos Generales | 103.50 |
            | Costos Operativos | 720.00 |
            | Costos del Ambiente | 0.00 |
            | Costos de Personal (Costo de Oportunidad) | 5120.00 |
            | **Costo Total del Proyecto** | **S/ 5943.50** |

            La inversión total estimada para el desarrollo del proyecto asciende a **S/ 5943.50**. Este costo es considerado bajo para un sistema de esta naturaleza. La inversión será asumida por los desarrolladores como parte de su formación.

    4.3. <span id="_Toc52661352" class="anchor"></span>Factibilidad Operativa


        El proyecto es operativamente factible.

        - **Beneficios para los Interesados:** El sistema será de gran utilidad para el área de TI de cualquier organización que necesite migrar sus datos. Reduce drásticamente el tiempo, el riesgo y el costo asociado a estos procesos. El producto final es una herramienta de línea de comandos, fácil de integrar en scripts de automatización.
        - **Capacidad de Mantenimiento:** El sistema se desarrollará con un código modular y bien documentado. Los interesados (o su equipo de TI) podrán mantenerlo, extenderlo o adaptarlo a nuevas necesidades gracias a su arquitectura basada en plugins y la documentación final proporcionada.
        - **Lista de Interesados (Stakeholders):**
            - **Docente del Curso (Mag. Patrick Cuadros Quiroga):** Principal interesado en el éxito académico y la calidad del proyecto.
            - **Desarrolladores (Halanocca Rojas, U. / LLica Mamani, J.):** Interesados en el aprendizaje, la implementación exitosa y la calidad del código.
            - **Futuros Usuarios Finales:** Administradores de bases de datos y equipos de TI que podrán utilizar la herramienta para optimizar sus operaciones.

    4.4. <span id="_Toc52661353" class="anchor"></span>Factibilidad Legal


        El proyecto es legalmente factible.

        - **Software de Código Abierto:** Todas las herramientas y bibliotecas a utilizar (Python, SQLAlchemy, pandas, etc.) poseen licencias de código abierto (como MIT, BSD, PSF) que permiten su uso, modificación y distribución sin restricciones legales para este tipo de proyecto académico y comercial.
        - **Protección de Datos:** El desarrollo considerará buenas prácticas para el manejo de datos, aunque la responsabilidad final del cumplimiento de normativas como la Ley N° 29733 (Ley de Protección de Datos Personales del Perú) recae en el usuario final que implemente la herramienta con datos reales. El sistema se diseñará para facilitar este cumplimiento mediante funciones de sanitización y el uso de conexiones seguras.
        - **Propiedad Intelectual:** El código fuente desarrollado será de propiedad de los autores, quienes podrán decidir su licencia de distribución (ej. GPL, MIT) al finalizar el proyecto.

    4.5. <span id="_Toc52661354" class="anchor"></span>Factibilidad Social


        El proyecto es socialmente factible.

        - **Impacto Social Positivo:** Al facilitar la migración de datos, el sistema contribuye a la modernización de pequeñas y medianas empresas que no cuentan con los recursos para adquirir costosas herramientas comerciales. Esto puede promover el uso de software de código abierto y la independencia tecnológica.
        - **Códigos de Conducta y Ética:** El desarrollo se adherirá a los principios éticos de la ingeniería de software, garantizando la integridad de los datos y la transparencia del proceso de migración. No se utilizará para acceder o manipular datos de forma no autorizada.

    4.6. <span id="_Toc52661355" class="anchor"></span>**Factibilidad Ambiental**


        El proyecto es ambientalmente factible y tiene un impacto positivo.

        - **Reducción del Consumo de Recursos:** Al optimizar los procesos de migración, se reduce la necesidad de ejecutar múltiples procesos manuales largos que consumen energía en los servidores y estaciones de trabajo.
        - **Eficiencia Energética:** El desarrollo y la ejecución del sistema en entornos de código abierto no requiere hardware de alta gama, evitando la obsolescencia programada y el consumo energético excesivo.
        - **Digitalización:** El sistema promueve la gestión de datos en entornos digitales, reduciendo la necesidad de documentación y procesos en papel.

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

**5. <span id="_Toc52661356" class="anchor"></span>Análisis Financiero**


    **5.1. Justificación de la Inversión**


        **5.1.1. Beneficios del Proyecto**

            - **Beneficios Tangibles:**
                - **Ahorro en horas-hombre:** Una migración manual de una base de datos de tamaño medio puede tomar 2 semanas (80 horas) de un especialista. Con DBMigrator, este tiempo se reduce a menos de 1 día (8 horas). Si el costo de un especialista es de S/ 50/hora, el ahorro por migración es de S/ 3,600 (72 horas ahorradas). Si se prevé realizar al menos 2 migraciones en el primer año, el ahorro total sería de S/ 7,200.
                - **Reducción de errores:** La automatización reduce la posibilidad de errores costosos que podrían llevar a pérdida de datos o interrupciones del servicio.
            - **Beneficios Intangibles:**
                - **Disponibilidad de recurso humano:** Libera al equipo de TI de tareas repetitivas para que puedan enfocarse en proyectos estratégicos.
                - **Mejora en la toma de decisiones:** Permite modernizar la infraestructura de datos de forma más ágil, facilitando la adopción de nuevas tecnologías.
                - **Cumplimiento de requerimientos:** Facilita procesos de migración requeridos por fusiones, adquisiciones o cambios de proveedor.
                - **Valor agregado al conocimiento:** La herramienta se convierte en un activo de código abierto que puede ser utilizado y mejorado por la comunidad.
        
        **5.1.2. Criterios de Inversión**
      
            - **Horizonte de Evaluación:** Se consideran los beneficios y costos en un horizonte de 3 años.
            - **Tasa de Descuento (Costo de Oportunidad de Capital - COK):** Se estima un COK del 10% anual, que representa la rentabilidad que se podría obtener invirtiendo el capital en otras alternativas.
    
            **Flujo de Caja Proyectado:**
            
            | Año | 0 | 1 | 2 | 3 |
            | :--- | :--- | :--- | :--- | :--- |
            | **Inversión Inicial** | -S/ 5,943.50 | | | |
            | **Ahorros (Beneficios)** | | S/ 7,200.00 | S/ 7,200.00 | S/ 7,200.00 |
            | **Flujo de Caja Neto** | **-S/ 5,943.50** | **S/ 7,200.00** | **S/ 7,200.00** | **S/ 7,200.00** |


            **5.1.2.1. Relación Beneficio/Costo (B/C)**


                Para calcular el B/C, primero se debe calcular el Valor Presente de los Beneficios (VPB) y el Valor Presente de los Costos (VPC), que en este caso es la inversión inicial.

                VPB = 7200 / (1+0.10)^1 + 7200 / (1+0.10)^2 + 7200 / (1+0.10)^3
                VPB = 7200 / 1.10 + 7200 / 1.21 + 7200 / 1.331
                VPB = 6545.45 + 5950.41 + 5409.46 = S/ 17,905.32
                
                VPC = S/ 5,943.50
                
                B/C = VPB / VPC = 17,905.32 / 5,943.50 = **3.01**
                
                **Conclusión:** El índice B/C es 3.01, el cual es mayor a 1. Por lo tanto, el proyecto es económicamente viable y se acepta.


            **5.1.2.2. Valor Actual Neto (VAN)**

            
                VAN = -Inversión Inicial + VPB
                VAN = -5,943.50 + 17,905.32 = **S/ 11,961.82**
  
                **Conclusión:** El VAN es positivo (S/ 11,961.82), lo que indica que el proyecto generará un valor neto superior a la inversión realizada, por lo que se acepta.

            **5.1.2.3 Tasa Interna de Retorno (TIR)**

            
                La TIR es la tasa de descuento que hace que el VAN sea igual a cero.
                
                -5,943.50 + 7200/(1+TIR)^1 + 7200/(1+TIR)^2 + 7200/(1+TIR)^3 = 0
                
                Calculando la TIR, obtenemos un valor aproximado del **110%**.
                
                **Conclusión:** La TIR (110%) es significativamente mayor que el COK (10%). Esto demuestra que el proyecto es altamente rentable, ya que el retorno de la inversión es muy superior al costo de oportunidad del capital.

<div style="page-break-after: always; visibility: hidden">\pagebreak</div>

6. <span id="_Toc52661357" class="anchor"></span>**Conclusiones**

    Tras realizar el estudio de factibilidad para el Sistema DBMigrator, se presentan las siguientes conclusiones:
    
    1.  **Viabilidad Técnica:** El proyecto es técnicamente viable. Existe un ecosistema de software maduro y de código abierto (Python, SQLAlchemy, pandas) que permite su desarrollo. El plan de trabajo estructurado en 5 hitos y 21 issues garantiza un desarrollo ordenado, secuencial y medible, minimizando los riesgos técnicos.
    
    2.  **Viabilidad Económica y Financiera:** La inversión total estimada es de S/ 5,943.50. Los indicadores financieros calculados (B/C de 3.01, VAN positivo de S/ 11,961.82 y una TIR del 110%) demuestran de manera concluyente que el proyecto es altamente rentable y generará un retorno de la inversión muy superior a cualquier costo de oportunidad, justificando plenamente su ejecución.
    
    3.  **Viabilidad Operativa:** El sistema generará beneficios tangibles (ahorro de tiempo y reducción de errores) e intangibles (disponibilidad de personal, modernización tecnológica). La arquitectura modular y la documentación garantizarán su operatividad y mantenibilidad por parte de los usuarios finales.
    
    4.  **Viabilidad Legal, Social y Ambiental:** El proyecto cumple con las normativas de software libre, promueve buenas prácticas de protección de datos, tiene un impacto social positivo al democratizar el acceso a herramientas de migración y un impacto ambiental bajo al promover la eficiencia y reducir el uso de recursos.
    
    En base a los resultados de los estudios de factibilidad técnica, económica, operativa, legal, social y ambiental, se concluye que **el proyecto "Sistema DBMigrator" es totalmente viable y factible de ser desarrollado**.

