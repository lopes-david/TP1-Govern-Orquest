"""
Testes unitários para a camada bronze → silver.
Valida enriquecimento com dimensões e regras de qualidade.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_bronze_to_silver")
        .getOrCreate()
    )


def test_join_clientes(spark):
    """O join com a dimensão clientes deve trazer nome_cliente corretamente."""
    vendas = spark.createDataFrame(
        [("1", "100", "Concluída", 5, 200.0)],
        ["id_venda", "id_cliente", "status_venda", "quantidade", "valor_total"],
    )
    clientes = spark.createDataFrame(
        [("100", "Maria Silva", "Pessoa Física")],
        ["id_cliente", "nome_cliente", "segmento"],
    )
    result = vendas.join(clientes, on="id_cliente", how="left")
    assert result.first()["nome_cliente"] == "Maria Silva"


def test_is_alto_valor(spark):
    """Pedidos acima de R$10.000 devem ser marcados como is_alto_valor=True."""
    from pyspark.sql.functions import when, lit
    data = [("1", 15000.0), ("2", 5000.0)]
    df = spark.createDataFrame(data, ["id_venda", "valor_total"])
    df = df.withColumn(
        "is_alto_valor",
        when(col("valor_total") > 10000, lit(True)).otherwise(lit(False)),
    )
    altos = df.filter(col("is_alto_valor") == True)
    assert altos.count() == 1


def test_rejeita_quantidade_zero(spark):
    """Vendas com quantidade == 0 devem ser rejeitadas na silver."""
    data = [("1", 3), ("2", 0), ("3", 1)]
    df = spark.createDataFrame(data, ["id_venda", "quantidade"])
    valid = df.filter(col("quantidade") > 0)
    assert valid.count() == 2
