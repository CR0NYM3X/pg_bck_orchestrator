**Sofía (Directora de Calidad y Producto):** ¡Excelente decisión! La teoría es el mapa, pero la práctica es el terreno. Para capacitar al personal de operaciones, administradores de bases de datos y desarrolladores, no utilizaremos diapositivas. Vamos a construir un **Laboratorio de Pruebas de Estrés y Simulación de Fallos**.

**Elena (DBA Expert):** Para que las pruebas sean 100% reales sin alterar la metadata del negocio, lo primero que haremos será crear un entorno aislado con procesos simulados mediante retardos de tiempo (`pg_sleep`) y errores inducidos (divisiones por cero, violaciones de restricciones). Esto permitirá al personal ver el framework operando en tiempo real bajo consolas como DBeaver o pgAdmin.

**Mateo (Especialista en Concurrencia):** Preparé el escenario más extremo: el **Escenario 5 (El Protocolo Shield)**. Le enseñaremos al personal cómo "matar" al proceso Padre a propósito en el sistema operativo y observar cómo los hijos se organizan de forma atómica para revivirlo en milisegundos sin perder el progreso.

**Alejandro (Líder de Desarrollo):** Aquí tienes la guía de laboratorio interactiva paso a paso. Todo el código es ejecutable.

---

# MANUAL DE LABORATORIO PRÁCTICO (ENTRENAMIENTO FOAR)

## Fase: Certificación Práctica y Casos de Uso Real

### 🛠️ Paso 0: Inicialización del Entorno de Pruebas (La Mesa de Trabajo)

Ejecute este bloque para crear una tabla limpia donde nuestros procesos asíncronos escribirán sus resultados y evidencias.

```sql
CREATE SCHEMA IF NOT EXISTS resilient_bg_lab;

-- Tabla donde los procesos del laboratorio dejarán huella de su ejecución
CREATE TABLE IF NOT EXISTS resilient_bg_lab.evidencia_procesos (
    id SERIAL PRIMARY KEY,
    nombre_prueba VARCHAR(100),
    descripcion TEXT,
    creado_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Limpiar evidencias previas para iniciar el laboratorio desde cero
TRUNCATE TABLE resilient_bg_lab.evidencia_procesos;

```

---

### 🧪 Escenario 1: Secuencial Estricto con Falla Humana (SEQUENTIAL_STRICT)

**Objetivo:** Demostrar cómo el motor actúa como un escudo de seguridad bi-fase, deteniendo la cadena completa si un proceso intermedio falla, evitando la corrupción de datos subsecuente.

**Paso 1.1: Lanzar el Job "One-Shot"**
Enviamos 3 tareas. La tarea 1 es exitosa, la tarea 2 provoca un error matemático intencional (`1/0`) y la tarea 3 nunca debería ejecutarse.

```sql
SELECT resilient_bg.launch_job_one_shot(
    'LAB_SECUENCIAL_ESTRICTO', 
    'SEQUENTIAL_STRICT', 
    ARRAY[
        'INSERT INTO resilient_bg_lab.evidencia_procesos (nombre_prueba, descripcion) VALUES (''STRICT'', ''Tarea 1 Ejecutada con Éxito'');',
        'SELECT 1 / 0; -- ERROR INDUCIDO: División por cero',
        'INSERT INTO resilient_bg_lab.evidencia_procesos (nombre_prueba, descripcion) VALUES (''STRICT'', ''Tarea 3 - ¡ERROR CRÍTICO SI VES ESTO!'');'
    ],
    p_timeout_seconds => 30,
    p_max_retries => 0 -- Sin reintentos para ver el fallo inmediato
);
-- Anote el ID de ejecución devuelto (Ej: 1)

```

**Paso 1.2: Verificación del Tablero de Control**
Consulte inmediatamente la vista analítica corporativa:

```sql
SELECT * FROM resilient_bg.vw_status_progreso_corporativo WHERE "Nombre del Job" = 'LAB_SECUENCIAL_ESTRICTO';

```

* **Resultado Esperado en el Tablero:** El "Estatus Real" cambiará a `❌ FALLIDO CON ERRORES`. El avance se detendrá y verá que hubo 1 tarea hecha y 1 error.

**Paso 1.3: Inspección de Evidencias en Tablas**

```sql
SELECT * FROM resilient_bg_lab.evidencia_procesos;

```

* **Hecho Real Demostrado:** Solo existirá el registro de la Tarea 1. La Tarea 3 fue completamente bloqueada por el Orquestador, protegiendo la integridad del flujo.

---

### 🧪 Escenario 2: Secuencial Tolerante a Fallos (SEQUENTIAL_NORMAL)

**Objetivo:** Capacitar al personal sobre cómo ejecutar cadenas de mantenimiento (como reíndices o limpiezas) donde si un paso falla, el sistema debe registrar el error pero continuar con los demás de forma obligatoria.

**Paso 2.1: Lanzamiento del Job**
Usamos las mismas consultas del Escenario 1, pero cambiamos el modo a `SEQUENTIAL_NORMAL`.

```sql
SELECT resilient_bg.launch_job_one_shot(
    'LAB_SECUENCIAL_NORMAL', 
    'SEQUENTIAL_NORMAL', 
    ARRAY[
        'INSERT INTO resilient_bg_lab.evidencia_procesos (nombre_prueba, descripcion) VALUES (''NORMAL'', ''Tarea 1 Exitosa'');',
        'SELECT 1 / 0; -- ERROR INDUCIDO',
        'INSERT INTO resilient_bg_lab.evidencia_procesos (nombre_prueba, descripcion) VALUES (''NORMAL'', ''Tarea 3 Ejecutada a pesar del fallo anterior'');'
    ],
    p_timeout_seconds => 30,
    p_max_retries => 0
);

```

**Paso 2.2: Verificación de Evidencias**
Espere 3 segundos y ejecute:

```sql
SELECT * FROM resilient_bg_lab.evidencia_procesos WHERE nombre_prueba = 'NORMAL';

```

* **Hecho Real Demostrado:** Verá **ambos registros** (Tarea 1 y Tarea 3) en la tabla de evidencias. El sistema no se detuvo ante la falla de la tarea 2.

**Paso 2.3: Auditoría del Error**
Para revisar qué le pasó exactamente a la tarea 2, el operador ejecuta:

```sql
SELECT rt.run_task_id, rt.status, rt.error_log 
FROM resilient_bg.run_tasks rt
JOIN resilient_bg.run_jobs rj ON rt.run_id = rj.run_id
JOIN resilient_bg.def_jobs dj ON rj.job_id = dj.job_id
WHERE dj.job_name = 'LAB_SECUENCIAL_NORMAL';

```

* **Resultado:** La tarea 2 mostrará estado `FAILED` y el campo `error_log` dirá exactamente: `division by zero`.

---

### 🧪 Escenario 3: Ráfaga en Paralelo y Reducción de Tiempos (PARALLEL_INITIAL)

**Objetivo:** Demostrar cómo el framework reduce los tiempos de ejecución de horas a segundos procesando tareas de forma simultánea.

**Paso 3.1: Lanzamiento del Job**
Lanzaremos 3 tareas. Cada tarea simulará una carga pesada tardando 5 segundos exactos en el procesador (`pg_sleep(5)`). Si fuera secuencial, el Job tardaría 15 segundos netos.

```sql
SELECT resilient_bg.launch_job_one_shot(
    'LAB_RAFAGA_PARALELA', 
    'PARALLEL_INITIAL', 
    ARRAY[
        'SELECT pg_sleep(5); INSERT INTO resilient_bg_lab.evidencia_procesos(nombre_prueba, descripcion) VALUES (''PARALLEL'', ''Hilo 1 terminado'');',
        'SELECT pg_sleep(5); INSERT INTO resilient_bg_lab.evidencia_procesos(nombre_prueba, descripcion) VALUES (''PARALLEL'', ''Hilo 2 terminado'');',
        'SELECT pg_sleep(5); INSERT INTO resilient_bg_lab.evidencia_procesos(nombre_prueba, descripcion) VALUES (''PARALLEL'', ''Hilo 3 terminado'');'
    ],
    p_timeout_seconds => 30
);

```

**Paso 3.2: Monitoreo en Tiempo Real (Ejecutar varias veces mientras corre)**

```sql
SELECT "ID Ejecución", "Estatus Real", "Workers Activos (Motor)", "Duración", "Línea de Progreso" 
FROM resilient_bg.vw_status_progreso_corporativo 
WHERE "Nombre del Job" = 'LAB_RAFAGA_PARALELA';

```

* **Observación en Vivo:** Durante los primeros segundos verá que los `Workers Activos (Motor)` marcan `3`. El hardware está trabajando en paralelo.
* **Resultado de Duración Final:** Una vez finalizado, la columna `"Duración"` marcará **00:00:05** o **00:00:06** segundos. ¡Hemos procesado 15 segundos de carga de trabajo en solo 5 segundos reales!

---

### 🧪 Escenario 4: Control de Concurrencia Regulada (RANDOM / SLOTS)

**Objetivo:** Aprender a proteger al servidor de sobrecalentamientos o saturación de memoria. Configuraremos un pool de 4 tareas largas, pero limitaremos el motor a usar **máximo 2 canales concurrentes** (`p_max_parallel_processes => 2`).

**Paso 4.1: Lanzamiento con Límite de Canales**

```sql
SELECT resilient_bg.launch_job_one_shot(
    'LAB_CONCURRENCIA_RANDOM', 
    'RANDOM', 
    ARRAY[
        'SELECT pg_sleep(4);',
        'SELECT pg_sleep(4);',
        'SELECT pg_sleep(4);',
        'SELECT pg_sleep(4);'
    ],
    p_timeout_seconds => 30,
    p_max_parallel_processes => 2 -- Límite de control estricto
);

```

**Paso 4.2: Monitoreo Crítico Inmediato**
Corra la vista de inmediato:

```sql
SELECT "Estatus Real", "Total Tareas", "Hechas", "En Espera", "Workers Activos (Motor)" 
FROM resilient_bg.vw_status_progreso_corporativo 
WHERE "Nombre del Job" = 'LAB_CONCURRENCIA_RANDOM';

```

* **Hecho Real Demostrado:** Aunque hay 4 tareas en total, la columna `Workers Activos (Motor)` **nunca superará el número 2**. Las otras 2 tareas permanecen en estado `En Espera`. En cuanto una de las primeras dos termina, el orquestador toma una de la cola inmediatamente, manteniendo el servidor estabilizado.

---

### 🧪 Escenario 5: El Escudo de Auto-Recuperación (Mecanismo SHIELD)

**Objetivo:** Demostrar la resiliencia industrial del sistema. Mataremos al proceso Padre y veremos cómo los hijos se auto-gestionan para revivirlo.

**Paso 5.1: Iniciar un Job con Retardos Largos**

```sql
SELECT resilient_bg.launch_job_one_shot(
    'LAB_PROVOCA_MUTILACION', 
    'SEQUENTIAL_NORMAL', 
    ARRAY[
        'SELECT pg_sleep(6);',
        'SELECT pg_sleep(6);',
        'SELECT pg_sleep(6);'
    ],
    p_timeout_seconds => 60
);
-- Supongamos que le devuelve el ID de ejecución: 5

```

**Paso 5.2: Identificar el PID del Padre (Supervisor)**
De inmediato, corra esta consulta para saber qué PID del sistema operativo tiene asignado el orquestador Padre:

```sql
SELECT monitor_pid FROM resilient_bg.run_jobs WHERE status = 'RUNNING' ORDER BY run_id DESC LIMIT 1;
-- Supongamos que el resultado es el PID: 14520

```

**Paso 5.3: El Atentado (Simular Caída Forzada del Servidor/Conexión)**
Utilice el comando nativo de PostgreSQL para terminar de forma abrupta el proceso del Padre (reemplace el número por el `monitor_pid` obtenido en el paso anterior):

```sql
SELECT pg_terminate_backend(14520); -- Matamos al Padre de forma fulminante

```

**Paso 5.4: Monitorear la Resurrección Automática**
Consulte de inmediato la vista corporativa de forma repetida:

```sql
SELECT "ID Ejecución", "Estatus Real", "Workers Activos (Motor)" 
FROM resilient_bg.vw_status_progreso_corporativo 
WHERE "Nombre del Job" = 'LAB_PROVOCA_MUTILACION';

```

* **Lo que observará el personal en consola:** 1. Por un instante el estatus cambiará a `🛡️ EN AUTO-RECUPERACIÓN`.
2. El proceso hijo que estaba corriendo detectó que su padre desapareció, ejecutó el bloqueo consultivo `pg_try_advisory_lock` y levantó un **nuevo orquestador** de fondo de forma transparente.
3. El estatus volverá a `🔥 EJECUTANDO (MOTOR ACTIVO)` con un nuevo número de PID asignado en las tablas internas. El flujo no se perdió y terminará con éxito.

---

### 🧪 Escenario 6: El Almacén de Juan y la Ejecución de Roberto (Reutilización Absoluta)

**Objetivo:** Practicar la división del trabajo diario. Registrar un flujo masivo sin gastar recursos y mandarlo a llamar posteriormente mediante su identificador.

**Paso 6.1: Juan registra las tareas a las 9:00 AM**
Juan usa la función definidora. Esto guarda la receta y valida las firmas criptográficas de los queries.

```sql
SELECT resilient_bg.create_job_definition(
    'PLAN_DIARIO_MANTENIMIENTO', 
    'SEQUENTIAL_NORMAL', 
    ARRAY[
        'ANALYZE resilient_bg.cat_queries;',
        'ANALYZE resilient_bg.run_jobs;'
    ]
) AS id_receta_juan;
-- Devuelve el ID del Job. Supongamos que es el Job ID: 12

```

**Paso 6.2: Roberto ejecuta a las 7:00 PM usando el ID**
Roberto llega en la noche, no reescribe ningún query ni array. Simplemente ejecuta:

```sql
SELECT resilient_bg.launch_job_by_id(12) AS corrida_roberto_1;
-- Devuelve ID de Ejecución: 101

```

**Paso 6.3: Roberto vuelve a ejecutar el mismo ID (Doble Corrida Histórica)**
Si Roberto necesita correrlo de nuevo por requerimiento del negocio:

```sql
SELECT resilient_bg.launch_job_by_id(12) AS corrida_roberto_2;
-- Devuelve ID de Ejecución: 102

```

**Paso 6.4: Auditoría de Aislamiento de Reportes**
Para comprobar que los históricos no se mezclan y que se crearon dos bitácoras totalmente limpias e independientes para cada una de las corridas de Roberto:

```sql
SELECT "ID Ejecución", "Nombre del Job", "Estatus Real", "Duración" 
FROM resilient_bg.vw_status_progreso_corporativo 
WHERE "Nombre del Job" = 'PLAN_DIARIO_MANTENIMIENTO'
ORDER BY "ID Ejecución" DESC;

```

---

## 📝 Evaluación Final para el Personal (Criterios de Aceptación)

**Mateo (Concurrencia):** Si el personal logra completar los 6 escenarios en la consola de pruebas y comprende visualmente cómo cambian los estados de la vista `vw_status_progreso_corporativo`, estarán **100% listos y certificados** para operar y monitorear el Framework FOAR en los servidores de producción de misión crítica el próximo mes. ¡El laboratorio está oficialmente homologado!
