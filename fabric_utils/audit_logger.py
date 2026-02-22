import json
import traceback
import uuid
from datetime import datetime, date

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

AUDIT_SCHEMA = StructType([
    StructField("execution_id",     StringType(),    nullable=False),
    StructField("correlation_id",   StringType(),    nullable=True),
    StructField("pipeline_run_id",  StringType(),    nullable=True),
    StructField("notebook_name",    StringType(),    nullable=False),
    StructField("layer",            StringType(),    nullable=True),
    StructField("operation",        StringType(),    nullable=True),
    StructField("source_table",     StringType(),    nullable=True),
    StructField("target_table",     StringType(),    nullable=True),
    StructField("environment",      StringType(),    nullable=True),
    StructField("workspace_name",   StringType(),    nullable=True),
    StructField("start_time",       TimestampType(), nullable=True),
    StructField("end_time",         TimestampType(), nullable=True),
    StructField("duration_seconds", LongType(),      nullable=True),
    StructField("rows_read",        LongType(),      nullable=True),
    StructField("rows_written",     LongType(),      nullable=True),
    StructField("status",           StringType(),    nullable=False),
    StructField("error_message",    StringType(),    nullable=True),
    StructField("stack_trace",      StringType(),    nullable=True),
    StructField("attempt_number",   IntegerType(),   nullable=True),
    StructField("parameters",       StringType(),    nullable=True),
    StructField("extra_metadata",   StringType(),    nullable=True),
    StructField("log_date",         DateType(),      nullable=False),
])


class NotebookAuditLogger:
    """Registers execution metrics and states of Fabric notebooks in a Delta Lake table."""

    def __init__(
        self,
        spark: SparkSession,
        layer: str,
        operation: str,
        source_table: str = "",
        target_table: str = "",
        correlation_id: str = None,
        audit_table: str = "audit_lakehouse.notebook_audit_log",
        environment: str = "dev",
        attempt_number: int = 1,
        parameters: dict = None,
        extra_metadata: dict = None,
    ):
        self.spark = spark
        self.layer = layer
        self.operation = operation
        self.source_table = source_table
        self.target_table = target_table
        self.audit_table = audit_table
        self.environment = environment
        self.attempt_number = attempt_number
        self.parameters = json.dumps(parameters) if parameters else None
        self.extra_metadata = json.dumps(extra_metadata) if extra_metadata else None

        (
            self.notebook_name,
            self.pipeline_run_id,
            self.workspace_name,
            self.correlation_id,
        ) = self._extract_context(correlation_id)

        self.execution_id = str(uuid.uuid4())
        self.start_time = datetime.utcnow()
        self.rows_read = 0
        self.rows_written = 0

    def _extract_context(self, correlation_id):
        """Extract runtime context from notebookutils (Fabric) or fall back to local defaults."""
        try:
            import notebookutils  # noqa: F401
            ctx = notebookutils.runtime.context
            notebook_name = ctx.get("currentNotebookName", "unknown")
            pipeline_run_id = ctx.get("currentRunId", None)
            workspace_name = ctx.get("workspaceName", "unknown")
            root_run_id = ctx.get("rootRunId", None)
        except Exception:
            notebook_name = "local"
            pipeline_run_id = None
            workspace_name = "local"
            root_run_id = None

        # Correlation ID priority: explicit parameter → rootRunId → new UUID
        final_correlation_id = correlation_id or root_run_id or str(uuid.uuid4())
        return notebook_name, pipeline_run_id, workspace_name, final_correlation_id

    def set_rows_read(self, count: int):
        """Update the number of rows read."""
        self.rows_read = count

    def set_rows_written(self, count: int):
        """Update the number of rows written."""
        self.rows_written = count

    def start(self):
        """Write an initial RUNNING log entry."""
        self._write_log(status="RUNNING")

    def finish_success(self):
        """Write a SUCCESS log entry."""
        self._write_log(status="SUCCESS")

    def finish_failure(self, exception: Exception):
        """Write a FAILED log entry with error details."""
        error_message = str(exception)[:2000]
        stack_trace = traceback.format_exc()[:5000]
        self._write_log(status="FAILED", error_message=error_message, stack_trace=stack_trace)

    def _write_log(self, status: str, error_message: str = None, stack_trace: str = None):
        """Build a log Row and upsert it into the Delta audit table."""
        end_time = datetime.utcnow()
        duration_seconds = int((end_time - self.start_time).total_seconds())

        row = Row(
            execution_id=self.execution_id,
            correlation_id=self.correlation_id,
            pipeline_run_id=self.pipeline_run_id,
            notebook_name=self.notebook_name,
            layer=self.layer,
            operation=self.operation,
            source_table=self.source_table or None,
            target_table=self.target_table or None,
            environment=self.environment,
            workspace_name=self.workspace_name,
            start_time=self.start_time,
            end_time=end_time,
            duration_seconds=duration_seconds,
            rows_read=self.rows_read,
            rows_written=self.rows_written,
            status=status,
            error_message=error_message,
            stack_trace=stack_trace,
            attempt_number=self.attempt_number,
            parameters=self.parameters,
            extra_metadata=self.extra_metadata,
            log_date=date.today(),
        )

        df = self.spark.createDataFrame([row], schema=AUDIT_SCHEMA)

        table_exists = False
        try:
            self.spark.table(self.audit_table)
            table_exists = True
        except Exception:
            table_exists = False

        if table_exists:
            df.createOrReplaceTempView("_audit_log_upsert")
            self.spark.sql(f"""
                MERGE INTO {self.audit_table} AS target
                USING _audit_log_upsert AS source
                ON target.execution_id = source.execution_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        else:
            (
                df.write
                .format("delta")
                .partitionBy("log_date")
                .option("mergeSchema", "true")
                .saveAsTable(self.audit_table)
            )
