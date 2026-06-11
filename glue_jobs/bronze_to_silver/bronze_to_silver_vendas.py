"""
Glue Job: bronze → silver  |  tabela: vendas
Aplica validações de qualidade, enriquecimento com dimensões (clientes, produtos,
regioes) e grava na silver-zone particionado por ano/mes.

Parâmetros:
  --JOB_NAME      (injetado pelo Glue)
  --BRONZE_BUCKET ex: bronze-zone-234828142988
  --SILVER_BUCKET ex: silver-zone-234828142988
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, upper, trim, current_timestamp, when, lit

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_BUCKET', 'SILVER_BUCKET'])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

bronze = f"s3://{args['BRONZE_BUCKET']}"
silver = f"s3://{args['SILVER_BUCKET']}"

VALID_STATUS = ["Concluída", "Cancelada", "Em andamento"]

# --- Leitura das tabelas bronze ---
df_vendas   = spark.read.parquet(f"{bronze}/vendas/")
df_clientes = spark.read.parquet(f"{bronze}/clientes/")
df_produtos = spark.read.parquet(f"{bronze}/produtos/")
df_regioes  = spark.read.parquet(f"{bronze}/regioes/")

# --- Validações de qualidade ---
total = df_vendas.count()

df_valid = (
    df_vendas
    .filter(col("id_venda").isNotNull())
    .filter(col("id_cliente").isNotNull())
    .filter(col("quantidade") > 0)
    .filter(col("valor_total") >= 0)
    .filter(col("status_venda").isin(VALID_STATUS))
)

rejected = total - df_valid.count()
print(f"[QA] Total: {total} | Rejeitados: {rejected} | Aprovados: {df_valid.count()}")

# --- Enriquecimento com dimensões ---
df_silver = (
    df_valid
    .join(df_clientes.select("id_cliente", "nome_cliente", "segmento", "estado"),
          on="id_cliente", how="left")
    .join(df_produtos.select("id_produto", "nome_produto", "categoria"),
          on="id_produto", how="left")
    .join(df_regioes.select("id_regiao", "nome_regiao"),
          on="id_regiao", how="left")
    .withColumn("is_alto_valor",
                when(col("valor_total") > 10000, lit(True)).otherwise(lit(False)))
    .withColumn("_silver_processed_at", current_timestamp())
)

# --- Gravação na silver particionada por ano/mes ---
(
    df_silver.write
    .mode("overwrite")
    .partitionBy("ano", "mes")
    .parquet(f"{silver}/vendas/")
)

job.commit()
print("Job bronze_to_silver_vendas concluído com sucesso.")
