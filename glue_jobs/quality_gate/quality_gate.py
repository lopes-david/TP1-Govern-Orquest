"""
Quality Gate — Glue PySpark Job.

Executa verificações de qualidade equivalentes aos testes dbt
(not_null, unique, accepted_values) sobre as zonas bronze e silver,
calcula a taxa de erro por categoria e interrompe o pipeline caso
algum limiar seja ultrapassado.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
POLÍTICA DE LIMIARES (justificativa)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INTEGRITY_THRESHOLD = 0.0  (0 %)
  PKs nulas ou duplicadas e status_venda fora do domínio permitido
  corrompem joins e agregações downstream de forma silenciosa.
  Qualquer violação é sinal de problema na ingestão ou no schema
  da fonte, exigindo intervenção imediata — não existe "nível
  aceitável" para esse tipo de dado.

COMPLETENESS_THRESHOLD = 0.05  (5 %)
  Campos de enriquecimento (nome_cliente, segmento, nome_regiao)
  podem ser nulos legítimos: clientes antigos sem cadastro completo,
  produtos em homologação, regiões não mapeadas. Um nível de até 5 %
  não impede a operação dos dashboards, pois registros incompletos
  são excluídos ou agrupados como "Não informado" na camada silver.
  Se ultrapassar 5 %, o volume de dados faltantes começa a distorcer
  KPIs analíticos.

BUSINESS_THRESHOLD = 0.0  (0 %)
  Regras financeiras (valor_total >= 0, quantidade > 0) não admitem
  exceções: qualquer venda concluída com valor negativo ou quantidade
  zero representa erro contábil grave e deve bloquear a promoção para
  a gold-zone imediatamente.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import json
import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, functions as F

# ── parâmetros do job ────────────────────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    ["BRONZE_BUCKET", "SILVER_BUCKET", "REPORT_BUCKET"],
)
BRONZE_BUCKET = args["BRONZE_BUCKET"]
SILVER_BUCKET = args["SILVER_BUCKET"]
REPORT_BUCKET = args["REPORT_BUCKET"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

# ── limiares ─────────────────────────────────────────────────────────────────
INTEGRITY_THRESHOLD = 0.0   # 0 % — PKs e domínios de negócio
COMPLETENESS_THRESHOLD = 0.05  # 5 % — atributos opcionais de enriquecimento
BUSINESS_THRESHOLD = 0.0    # 0 % — regras financeiras


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def error_rate(failed: int, total: int) -> float:
    return 0.0 if total == 0 else failed / total


def check_not_null(df: DataFrame, col: str) -> dict:
    total = df.count()
    failed = df.filter(F.col(col).isNull()).count()
    return {"check": f"not_null:{col}", "total": total, "failed": failed,
            "rate": error_rate(failed, total)}


def check_unique(df: DataFrame, col: str) -> dict:
    total = df.count()
    failed = total - df.select(col).dropDuplicates().count()
    return {"check": f"unique:{col}", "total": total, "failed": failed,
            "rate": error_rate(failed, total)}


def check_accepted_values(df: DataFrame, col: str, values: list) -> dict:
    total = df.count()
    failed = df.filter(~F.col(col).isin(values)).count()
    return {"check": f"accepted_values:{col}", "total": total, "failed": failed,
            "rate": error_rate(failed, total)}


def check_non_negative(df: DataFrame, col: str) -> dict:
    total = df.count()
    failed = df.filter(F.col(col) < 0).count()
    return {"check": f"non_negative:{col}", "total": total, "failed": failed,
            "rate": error_rate(failed, total)}


def check_positive(df: DataFrame, col: str) -> dict:
    total = df.count()
    failed = df.filter(F.col(col) <= 0).count()
    return {"check": f"positive:{col}", "total": total, "failed": failed,
            "rate": error_rate(failed, total)}


# ─────────────────────────────────────────────────────────────────────────────
# Verificações — Bronze
# ─────────────────────────────────────────────────────────────────────────────

def run_bronze_checks(bronze_path: str) -> tuple[list, list, list]:
    """Retorna (integrity_results, completeness_results, business_results)."""

    vendas = spark.read.parquet(f"s3://{bronze_path}/vendas/")
    clientes = spark.read.parquet(f"s3://{bronze_path}/clientes/")
    produtos = spark.read.parquet(f"s3://{bronze_path}/produtos/")
    regioes = spark.read.parquet(f"s3://{bronze_path}/regioes/")

    status_validos = ["Concluída", "Cancelada", "Em andamento"]

    integrity = [
        # vendas — PKs e domínio
        check_not_null(vendas, "id_venda"),
        check_unique(vendas, "id_venda"),
        check_not_null(vendas, "id_cliente"),
        check_not_null(vendas, "id_produto"),
        check_not_null(vendas, "id_regiao"),
        check_not_null(vendas, "data_venda"),
        check_not_null(vendas, "status_venda"),
        check_accepted_values(vendas, "status_venda", status_validos),
        check_not_null(vendas, "canal_venda"),
        # clientes — PK
        check_not_null(clientes, "id_cliente"),
        check_unique(clientes, "id_cliente"),
        # produtos — PK
        check_not_null(produtos, "id_produto"),
        check_unique(produtos, "id_produto"),
        # regioes — PK
        check_not_null(regioes, "id_regiao"),
        check_unique(regioes, "id_regiao"),
    ]

    completeness = [
        check_not_null(vendas, "quantidade"),
        check_not_null(vendas, "valor_unitario"),
        check_not_null(vendas, "valor_total"),
        check_not_null(clientes, "nome_cliente"),
        check_not_null(clientes, "email"),
        check_not_null(clientes, "segmento"),
        check_not_null(clientes, "estado"),
        check_not_null(produtos, "nome_produto"),
        check_not_null(produtos, "categoria"),
        check_not_null(produtos, "preco_unitario"),
        check_not_null(regioes, "nome_regiao"),
    ]

    business = [
        check_non_negative(vendas, "valor_total"),
        check_positive(vendas, "quantidade"),
    ]

    return integrity, completeness, business


# ─────────────────────────────────────────────────────────────────────────────
# Verificações — Silver
# ─────────────────────────────────────────────────────────────────────────────

def run_silver_checks(silver_path: str) -> tuple[list, list, list]:

    enriched = spark.read.parquet(f"s3://{silver_path}/vendas_enriched/")

    status_validos = ["Concluída", "Cancelada", "Em andamento"]

    integrity = [
        check_not_null(enriched, "id_venda"),
        check_unique(enriched, "id_venda"),
        check_not_null(enriched, "id_cliente"),
        check_not_null(enriched, "id_produto"),
        check_not_null(enriched, "id_regiao"),
        check_not_null(enriched, "status_venda"),
        check_accepted_values(enriched, "status_venda", status_validos),
        check_not_null(enriched, "canal_venda"),
        check_not_null(enriched, "is_alto_valor"),
    ]

    completeness = [
        check_not_null(enriched, "nome_cliente"),
        check_not_null(enriched, "nome_produto"),
        check_not_null(enriched, "nome_regiao"),
    ]

    business = [
        check_non_negative(enriched, "valor_total"),
        check_positive(enriched, "quantidade"),
    ]

    return integrity, completeness, business


# ─────────────────────────────────────────────────────────────────────────────
# Avaliação de limiares e geração de relatório
# ─────────────────────────────────────────────────────────────────────────────

def evaluate(results: list, threshold: float, category: str) -> list[str]:
    """Retorna lista de mensagens de falha para violações acima do limiar."""
    failures = []
    for r in results:
        if r["rate"] > threshold:
            failures.append(
                f"[{category}] {r['check']}: taxa={r['rate']:.2%} "
                f"(limiar={threshold:.0%}, falhas={r['failed']}/{r['total']})"
            )
    return failures


def publish_report(all_results: dict, failures: list[str], report_bucket: str):
    report = {
        "executed_at": datetime.now(timezone.utc).isoformat(),
        "thresholds": {
            "integrity": INTEGRITY_THRESHOLD,
            "completeness": COMPLETENESS_THRESHOLD,
            "business": BUSINESS_THRESHOLD,
        },
        "passed": len(failures) == 0,
        "failures": failures,
        "details": all_results,
    }
    path = (
        f"s3://{report_bucket}/quality-reports/"
        f"{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/"
        "quality_gate_report.json"
    )
    spark.createDataFrame([{"json": json.dumps(report, ensure_ascii=False)}]) \
         .select("json") \
         .coalesce(1) \
         .write.mode("overwrite").text(path)
    logger.info(f"Relatório publicado em {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=== Quality Gate iniciado ===")
    logger.info(f"Limiares — integridade: {INTEGRITY_THRESHOLD:.0%} | "
                f"completude: {COMPLETENESS_THRESHOLD:.0%} | "
                f"regras de negócio: {BUSINESS_THRESHOLD:.0%}")

    b_integrity, b_completeness, b_business = run_bronze_checks(BRONZE_BUCKET)
    s_integrity, s_completeness, s_business = run_silver_checks(SILVER_BUCKET)

    all_results = {
        "bronze": {
            "integrity": b_integrity,
            "completeness": b_completeness,
            "business": b_business,
        },
        "silver": {
            "integrity": s_integrity,
            "completeness": s_completeness,
            "business": s_business,
        },
    }

    failures: list[str] = []
    failures += evaluate(b_integrity + s_integrity, INTEGRITY_THRESHOLD, "INTEGRIDADE")
    failures += evaluate(b_completeness + s_completeness, COMPLETENESS_THRESHOLD, "COMPLETUDE")
    failures += evaluate(b_business + s_business, BUSINESS_THRESHOLD, "NEGÓCIO")

    publish_report(all_results, failures, REPORT_BUCKET)

    if failures:
        msg = (
            "Quality Gate REPROVADO — pipeline interrompido.\n"
            + "\n".join(f"  ✗ {f}" for f in failures)
        )
        logger.error(msg)
        raise RuntimeError(msg)

    logger.info("Quality Gate APROVADO — todos os limiares respeitados.")


if __name__ == "__main__":
    main()
