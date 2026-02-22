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

## Actualización de versión

Para publicar una nueva versión:

1. Cambiar `version` en `setup.py`.
2. Cambiar `__version__` en `fabric_utils/__init__.py`.
3. Recompilar: `python -m build --wheel`.
4. Subir el nuevo `.whl` al Environment de Fabric y publicar.
