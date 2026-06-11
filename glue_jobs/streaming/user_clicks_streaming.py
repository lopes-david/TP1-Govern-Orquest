"""
Glue Job: streaming de cliques de usuários (Kafka/MSK → S3 raw)
Consome eventos em tempo real e persiste JSON particionado por ano/mes/dia/hora.

Parâmetros:
  --JOB_NAME           (injetado pelo Glue)
  --TARGET_S3_PATH     destino S3 (ex: s3://raw-zone-234828142988/user_clicks/)
  --CHECKPOINT_LOCATION localização do checkpoint Spark Streaming
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'TARGET_S3_PATH', 'CHECKPOINT_LOCATION']
)

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leitura do stream via Glue Data Catalog (tabela registrada via MSK/Kafka)
data_stream = glueContext.create_data_frame.from_catalog(
    database="datalake_clean_zone",
    table_name="user_clicks_stream",
    transformation_ctx="data_stream",
    additional_options={
        "startingPosition": "LATEST",
        "inferSchema": "true",
    },
)


def process_batch(data_frame, batch_id):
    """Persiste cada micro-batch enriquecido com timestamp e partições de data/hora."""
    if data_frame.count() == 0:
        return

    from pyspark.sql.functions import current_timestamp, year, month, dayofmonth, hour

    (
        data_frame
        .withColumn("processed_at", current_timestamp())
        .withColumn("ano",  year("event_timestamp"))
        .withColumn("mes",  month("event_timestamp"))
        .withColumn("dia",  dayofmonth("event_timestamp"))
        .withColumn("hora", hour("event_timestamp"))
        .write
        .mode("append")
        .partitionBy("ano", "mes", "dia", "hora")
        .json(args['TARGET_S3_PATH'])
    )


glueContext.forEachBatch(
    frame=data_stream,
    batch_function=process_batch,
    options={
        "windowSize": "30 seconds",
        "checkpointLocation": args['CHECKPOINT_LOCATION'],
    },
)

job.commit()
