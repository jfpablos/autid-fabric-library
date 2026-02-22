# fabric_utils

Utilidades compartidas para notebooks de Microsoft Fabric.

## Descripción

`fabric_utils` proporciona la clase `NotebookAuditLogger`, que registra métricas y estados de ejecución de notebooks en una tabla Delta Lake. Permite rastrear el inicio, éxito o fallo de cada ejecución junto con metadatos como filas procesadas, duración, parámetros y trazas de error.

## Instalación

### 1. Compilar el wheel

Desde el directorio `fabric_utils_project/`, ejecutar:

```bash
pip install build
python -m build --wheel
```

El archivo generado se encontrará en `dist/fabric_utils-0.1.0-py3-none-any.whl`.

### 2. Subir al Environment de Fabric

1. Abrir el **Workspace** de Microsoft Fabric.
2. Ir al **Environment** asociado al workspace o lakehouse.
3. En la sección **Custom libraries**, subir el archivo `.whl`.
4. Publicar el Environment.

El paquete quedará disponible para todos los notebooks que usen ese Environment.

## Uso

```python
from fabric_utils import NotebookAuditLogger

logger = NotebookAuditLogger(
    spark=spark,
    layer="silver",
    operation="transform_sales",
    source_table="bronze_lakehouse.raw_sales",
    target_table="silver_lakehouse.sales",
    environment="prod",
    attempt_number=1,
    parameters={"date": "2024-01-15", "region": "LATAM"},
)

logger.start()

try:
    # --- lógica principal del notebook ---
    df_source = spark.table("bronze_lakehouse.raw_sales")
    logger.set_rows_read(df_source.count())

    df_result = df_source.filter(...)  # transformaciones
    df_result.write.format("delta").saveAsTable("silver_lakehouse.sales")
    logger.set_rows_written(df_result.count())
    # ------------------------------------

    logger.finish_success()

except Exception as e:
    logger.finish_failure(e)
    raise
```

### Parámetros del constructor

| Parámetro | Tipo | Descripción |
|---|---|---|
| `spark` | SparkSession | Sesión Spark activa |
| `layer` | str | Capa de datos (bronze / silver / gold) |
| `operation` | str | Nombre descriptivo de la operación |
| `source_table` | str | Tabla de origen (opcional) |
| `target_table` | str | Tabla de destino (opcional) |
| `correlation_id` | str | ID de correlación externo (opcional) |
| `audit_table` | str | Tabla Delta de auditoría (default: `audit_lakehouse.notebook_audit_log`) |
| `environment` | str | Entorno: dev / uat / prod (default: `dev`) |
| `attempt_number` | int | Número de intento del pipeline (default: `1`) |
| `parameters` | dict | Parámetros del notebook serializados como JSON (opcional) |
| `extra_metadata` | dict | Metadatos adicionales serializados como JSON (opcional) |

### Tabla de auditoría

**Nombre y ubicación por defecto**

`audit_lakehouse.notebook_audit_log` — configurable con el parámetro `audit_table`.

**Comportamiento de creación**

- Si la tabla **no existe**: se crea automáticamente en formato Delta, particionada por `log_date`.
- Si la tabla **ya existe**: se hace un `MERGE` sobre `execution_id`, de forma que cada llamada a `start()`, `finish_success()` o `finish_failure()` actualiza la misma fila sin duplicarla.

**Esquema de columnas**

| Columna | Tipo | Nulable | Descripción |
|---|---|---|---|
| `execution_id` | STRING | NO | UUID único por ejecución del notebook |
| `correlation_id` | STRING | sí | ID para agrupar notebooks de un mismo pipeline |
| `pipeline_run_id` | STRING | sí | Run ID del pipeline de Fabric (extraído de `notebookutils`) |
| `notebook_name` | STRING | NO | Nombre del notebook en ejecución |
| `layer` | STRING | sí | Capa de datos: bronze / silver / gold |
| `operation` | STRING | sí | Nombre descriptivo de la operación |
| `source_table` | STRING | sí | Tabla de origen |
| `target_table` | STRING | sí | Tabla de destino |
| `environment` | STRING | sí | Entorno: dev / uat / prod |
| `workspace_name` | STRING | sí | Nombre del workspace de Fabric |
| `start_time` | TIMESTAMP | sí | Momento de inicio (UTC) |
| `end_time` | TIMESTAMP | sí | Momento de fin (UTC) |
| `duration_seconds` | LONG | sí | Duración total en segundos |
| `rows_read` | LONG | sí | Filas leídas (informado con `set_rows_read()`) |
| `rows_written` | LONG | sí | Filas escritas (informado con `set_rows_written()`) |
| `status` | STRING | NO | Estado: `RUNNING` / `SUCCESS` / `FAILED` |
| `error_message` | STRING | sí | Mensaje de excepción (máx. 2 000 caracteres) |
| `stack_trace` | STRING | sí | Stack trace completo (máx. 5 000 caracteres) |
| `attempt_number` | INTEGER | sí | Número de intento del pipeline |
| `parameters` | STRING | sí | Parámetros del notebook en JSON |
| `extra_metadata` | STRING | sí | Metadatos adicionales en JSON |
| `log_date` | DATE | NO | Fecha UTC del log — **columna de partición** |

## Actualización de versión

Para publicar una nueva versión:

1. Cambiar `version` en `setup.py`.
2. Cambiar `__version__` en `fabric_utils/__init__.py`.
3. Recompilar: `python -m build --wheel`.
4. Subir el nuevo `.whl` al Environment de Fabric y publicar.
