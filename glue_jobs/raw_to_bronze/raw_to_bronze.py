"""
Glue Job: raw → bronze
Lê os CSVs da landing zone (raw-zone) e grava Parquet na bronze-zone.
Tabelas: vendas (particionado ano/mes), clientes, produtos, regioes.

Parâmetros:
  --JOB_NAME     (injetado pelo Glue)
  --RAW_BUCKET    ex: raw-zone-<account-id>
  --BRONZE_BUCKET ex: bronze-zone-<account-id>
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'RAW_BUCKET', 'BRONZE_BUCKET'])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

raw    = f"s3://{args['RAW_BUCKET']}"
bronze = f"s3://{args['BRONZE_BUCKET']}"


def csv_to_parquet(source_path: str, target_path: str, partition_by: list = None):
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .csv(source_path)
        .withColumn("ingestion_ts", current_timestamp())
    )
    writer = df.write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(target_path)


# vendas — fact table, particionado por ano/mes para queries analíticas
csv_to_parquet(
    source_path=f"{raw}/vendas/",
    target_path=f"{bronze}/vendas/",
    partition_by=["ano", "mes"],
)

# dimensões — sem partição (tabelas pequenas)
for table in ["clientes", "produtos", "regioes"]:
    csv_to_parquet(
        source_path=f"{raw}/{table}/",
        target_path=f"{bronze}/{table}/",
    )

job.commit()
