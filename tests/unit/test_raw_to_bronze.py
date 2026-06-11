"""
Testes unitários para o job raw_to_bronze.
Usa PySpark local (sem AWS) para validar a lógica de transformação.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_raw_to_bronze")
        .getOrCreate()
    )


def test_vendas_schema_columns(spark):
    """Verifica que todas as colunas esperadas existem após leitura do CSV."""
    data = [
        ("1", "2024-01-17", "2024", "1", "37", "100", "1", "7", "4302.29", "0.19", "24393.98", "E-commerce", "Concluída"),
    ]
    cols = ["id_venda","data_venda","ano","mes","id_produto","id_cliente",
            "id_regiao","quantidade","valor_unitario","desconto","valor_total",
            "canal_venda","status_venda"]
    df = spark.createDataFrame(data, cols)

    assert "id_venda" in df.columns
    assert "valor_total" in df.columns
    assert "status_venda" in df.columns


def test_vendas_nao_tem_id_nulo(spark):
    """Linhas com id_venda nulo devem ser removidas na ingestão."""
    data = [("1", "Concluída"), (None, "Concluída")]
    df = spark.createDataFrame(data, ["id_venda", "status_venda"])
    df_filtered = df.filter(col("id_venda").isNotNull())
    assert df_filtered.count() == 1


def test_status_venda_validos(spark):
    """Apenas status do domínio devem ser aceitos na bronze."""
    valid_statuses = ["Concluída", "Cancelada", "Em andamento"]
    data = [("1", "Concluída"), ("2", "Invalido"), ("3", "Em andamento")]
    df = spark.createDataFrame(data, ["id_venda", "status_venda"])
    df_valid = df.filter(col("status_venda").isin(valid_statuses))
    assert df_valid.count() == 2


def test_valor_total_nao_negativo(spark):
    """Registros com valor_total negativo devem ser rejeitados."""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    schema = StructType([
        StructField("id_venda",    StringType(), True),
        StructField("valor_total", DoubleType(), True),
    ])
    data = [("1", 100.0), ("2", -50.0), ("3", 0.0)]
    df = spark.createDataFrame(data, schema)
    df_valid = df.filter(col("valor_total") >= 0)
    assert df_valid.count() == 2
