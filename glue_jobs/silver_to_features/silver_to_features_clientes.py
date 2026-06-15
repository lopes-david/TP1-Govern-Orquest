"""
Glue Job: silver → gold/features_clientes  |  Feature Store para ML

Computa variáveis preditivas limpas por cliente a partir do histórico de
vendas na silver zone. O resultado é uma tabela Parquet pronta para consumo
por Cientistas de Dados — sem necessidade de limpeza ou joins adicionais.

Parâmetros:
  --JOB_NAME      (injetado pelo Glue)
  --SILVER_BUCKET ex: silver-zone-<account-id>
  --GOLD_BUCKET   ex: gold-zone-<account-id>

Tabela gerada: cloudmart_features.features_clientes
Caminho S3: s3://<gold-bucket>/features_clientes/

Features por id_cliente:
  ── Recência ──────────────────────────────────────────────────────────────
  dias_ultima_compra      : dias desde a última compra concluída
  ── Frequência ────────────────────────────────────────────────────────────
  total_pedidos           : total de pedidos concluídos no histórico
  media_pedidos_mensais   : média mensal de pedidos (histórico completo)
  media_compras_ultimos_6_meses : média mensal de pedidos (janela 6 meses)
  ── Monetário ─────────────────────────────────────────────────────────────
  receita_total           : receita bruta total gerada pelo cliente
  ticket_medio            : valor médio por pedido
  ticket_max              : maior pedido já realizado
  ticket_min              : menor pedido já realizado
  ── Comportamental ────────────────────────────────────────────────────────
  qtd_categorias          : nº de categorias distintas compradas
  canal_preferido         : canal de venda mais utilizado
  taxa_cancelamento       : (pedidos_cancelados / total_pedidos_historico)
  qtd_pedidos_alto_valor  : pedidos com is_alto_valor = True
  ── Dimensão do cliente ───────────────────────────────────────────────────
  segmento_cliente        : segmento CRM (Premium / Regular / etc.)
  estado_cliente          : UF do cliente
  ── Score composto ────────────────────────────────────────────────────────
  score_rfm               : score RFM 0-100 (R=35% F=35% M=30%)
                            baseado em percentile_rank de cada dimensão
  ── Label de ML ───────────────────────────────────────────────────────────
  label_churn_risco       : 1 se dias_ultima_compra > 90 dias, 0 caso contrário
  ── Metadados ─────────────────────────────────────────────────────────────
  feature_extraction_ts   : timestamp da extração (para reproducibilidade)
"""

import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    avg, max as spark_max, min as spark_min,
    datediff, lit, to_date, current_timestamp,
    when, round as spark_round, coalesce,
    first, percent_rank,
)
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SILVER_BUCKET', 'GOLD_BUCKET'])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

silver = f"s3://{args['SILVER_BUCKET']}"
gold   = f"s3://{args['GOLD_BUCKET']}"

print("[FEATURES] Lendo silver zone...")
df = spark.read.parquet(f"{silver}/vendas/")

# ── 1. Janela de 6 meses ────────────────────────────────────────────────────
now = datetime.now()
cutoff_year  = now.year
cutoff_month = now.month - 6
if cutoff_month <= 0:
    cutoff_year  -= 1
    cutoff_month += 12

df_6m = df.filter(
    (col("ano") > cutoff_year) |
    ((col("ano") == cutoff_year) & (col("mes") >= cutoff_month))
)

# ── 2. Número de meses ativos no histórico ──────────────────────────────────
meses_ativos = (
    df.filter(col("status_venda") == "Concluída")
    .select("id_cliente", "ano", "mes")
    .distinct()
    .groupBy("id_cliente")
    .agg(count("*").alias("meses_com_compra"))
)

# ── 3. Frequência na janela de 6 meses ──────────────────────────────────────
freq_6m = (
    df_6m.filter(col("status_venda") == "Concluída")
    .groupBy("id_cliente", "ano", "mes")
    .agg(count("id_venda").alias("pedidos_mes"))
    .groupBy("id_cliente")
    .agg(avg("pedidos_mes").alias("media_compras_ultimos_6_meses"))
)

# ── 4. Canal preferido (modo) ────────────────────────────────────────────────
canal_count = (
    df.filter(col("status_venda") == "Concluída")
    .groupBy("id_cliente", "canal_venda")
    .agg(count("*").alias("cnt"))
)
w_canal = Window.partitionBy("id_cliente").orderBy(col("cnt").desc())
canal_preferido = (
    canal_count
    .withColumn("rn", percent_rank().over(w_canal))
    .filter(col("rn") == 0.0)
    .select("id_cliente", col("canal_venda").alias("canal_preferido"))
)

# ── 5. Dimensão cliente (última ocorrência) ───────────────────────────────────
dim_cliente = (
    df.select("id_cliente", "segmento", "estado")
    .dropDuplicates(["id_cliente"])
    .withColumnRenamed("segmento", "segmento_cliente")
    .withColumnRenamed("estado", "estado_cliente")
)

# ── 6. Agregações principais por cliente ─────────────────────────────────────
ref_date = lit(to_date(lit(now.strftime("%Y-%m-%d")), "yyyy-MM-dd"))

df_concluida = df.filter(col("status_venda") == "Concluída")
df_todos     = df   # inclui Cancelada e Em andamento para taxa_cancelamento

agg_main = (
    df_concluida.groupBy("id_cliente")
    .agg(
        count("id_venda").alias("total_pedidos"),
        spark_sum("valor_total").alias("receita_total"),
        avg("valor_total").alias("ticket_medio"),
        spark_max("valor_total").alias("ticket_max"),
        spark_min("valor_total").alias("ticket_min"),
        countDistinct("categoria").alias("qtd_categorias"),
        spark_sum(when(col("is_alto_valor"), 1).otherwise(0)).alias("qtd_pedidos_alto_valor"),
        datediff(
            to_date(lit(now.strftime("%Y-%m-%d")), "yyyy-MM-dd"),
            spark_max(to_date(col("data_venda"), "yyyy-MM-dd"))
        ).alias("dias_ultima_compra"),
    )
)

taxa_cancel = (
    df_todos.groupBy("id_cliente")
    .agg(
        count("id_venda").alias("_total_historico"),
        spark_sum(when(col("status_venda") == "Cancelada", 1).otherwise(0))
        .alias("_cancelados"),
    )
    .withColumn(
        "taxa_cancelamento",
        spark_round(col("_cancelados") / col("_total_historico"), 4)
    )
    .select("id_cliente", "taxa_cancelamento")
)

# ── 7. Join de todas as features ─────────────────────────────────────────────
df_features = (
    agg_main
    .join(meses_ativos,  "id_cliente", "left")
    .join(freq_6m,       "id_cliente", "left")
    .join(taxa_cancel,   "id_cliente", "left")
    .join(canal_preferido, "id_cliente", "left")
    .join(dim_cliente,   "id_cliente", "left")
    .withColumn(
        "media_pedidos_mensais",
        spark_round(col("total_pedidos") / coalesce(col("meses_com_compra"), lit(1)), 4)
    )
    .withColumn(
        "media_compras_ultimos_6_meses",
        coalesce(spark_round(col("media_compras_ultimos_6_meses"), 4), lit(0.0))
    )
    .drop("meses_com_compra")
)

# ── 8. Score RFM (percentile rank 0-100 por dimensão) ───────────────────────
w_r = Window.orderBy(col("dias_ultima_compra").desc())  # menor dias → score maior
w_f = Window.orderBy(col("total_pedidos"))
w_m = Window.orderBy(col("receita_total"))

df_features = (
    df_features
    .withColumn("_r_score", percent_rank().over(w_r) * 100)
    .withColumn("_f_score", percent_rank().over(w_f) * 100)
    .withColumn("_m_score", percent_rank().over(w_m) * 100)
    .withColumn(
        "score_rfm",
        spark_round(
            col("_r_score") * 0.35 +
            col("_f_score") * 0.35 +
            col("_m_score") * 0.30,
            2
        )
    )
    .drop("_r_score", "_f_score", "_m_score")
)

# ── 9. Label de churn ────────────────────────────────────────────────────────
df_features = df_features.withColumn(
    "label_churn_risco",
    when(col("dias_ultima_compra") > 90, 1).otherwise(0).cast("int")
)

# ── 10. Metadados de extração ─────────────────────────────────────────────────
df_features = df_features.withColumn(
    "feature_extraction_ts", current_timestamp()
)

# ── 11. Ordenar colunas para leitura amigável ─────────────────────────────────
cols_ordered = [
    "id_cliente",
    "segmento_cliente",
    "estado_cliente",
    "total_pedidos",
    "media_pedidos_mensais",
    "media_compras_ultimos_6_meses",
    "receita_total",
    "ticket_medio",
    "ticket_max",
    "ticket_min",
    "qtd_categorias",
    "canal_preferido",
    "taxa_cancelamento",
    "qtd_pedidos_alto_valor",
    "dias_ultima_compra",
    "score_rfm",
    "label_churn_risco",
    "feature_extraction_ts",
]

df_final = df_features.select(cols_ordered).coalesce(4)

print(f"[FEATURES] Total de clientes na feature table: {df_final.count()}")

# ── 12. Persistir como Parquet na gold zone ────────────────────────────────────
(
    df_final.write
    .mode("overwrite")
    .parquet(f"{gold}/features_clientes/")
)

job.commit()
print("[FEATURES] Job silver_to_features_clientes concluído.")
