"""
Glue Job: silver → gold  |  mart: relatorio_vendas
Agrega KPIs de vendas por categoria de produto, região e canal para consumo
em dashboards e relatórios executivos.

Parâmetros:
  --JOB_NAME      (injetado pelo Glue)
  --SILVER_BUCKET ex: silver-zone-<account-id>
  --GOLD_BUCKET   ex: gold-zone-<account-id>  (criar se não existir)
  --ANO           ex: 2024
  --MES           ex: 1  (opcional — se omitido agrega o ano inteiro)
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, lit, sum as spark_sum, count, avg, max as spark_max,
    min as spark_min, current_timestamp
)

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SILVER_BUCKET', 'GOLD_BUCKET', 'ANO'])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

silver = f"s3://{args['SILVER_BUCKET']}"
gold   = f"s3://{args['GOLD_BUCKET']}"
ano    = args['ANO']

# Lê do path raiz com basePath para preservar colunas de partição (ano, mes)
df_silver = (
    spark.read
    .option("basePath", f"{silver}/vendas/")
    .parquet(f"{silver}/vendas/ano={ano}/")
    .withColumn("ano", lit(int(ano)))
)

# KPI por categoria + região + canal_venda
df_gold = (
    df_silver
    .filter(col("status_venda") == "Concluída")
    .groupBy("categoria", "nome_regiao", "canal_venda", "ano", "mes")
    .agg(
        count("id_venda").alias("total_pedidos"),
        spark_sum("quantidade").alias("total_unidades"),
        spark_sum("valor_total").alias("receita_bruta"),
        avg("valor_total").alias("ticket_medio"),
        spark_max("valor_total").alias("maior_venda"),
        spark_min("valor_total").alias("menor_venda"),
    )
    .withColumn("_gold_gerado_em", current_timestamp())
    .orderBy("receita_bruta", ascending=False)
    .coalesce(2)
)

(
    df_gold.write
    .mode("overwrite")
    .partitionBy("ano", "mes")
    .parquet(f"{gold}/relatorio_vendas/")
)

job.commit()
print(f"Job silver_to_gold_relatorio_vendas concluído — ano {ano}.")
