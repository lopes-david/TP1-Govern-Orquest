"""
CloudMart Feature Store — Guia de Acesso para Cientistas de Dados
==================================================================

Este script documenta como um Cientista de Dados acessa as features
pré-computadas do CloudMart para treinar modelos de ML.

A tabela `cloudmart_features.features_clientes` contém 16 variáveis
preditivas limpas por cliente, eliminando a necessidade de:
  ✗ fazer joins entre bronze/silver
  ✗ reimplementar lógica de limpeza
  ✗ recomputar janelas de tempo (ex: últimos 6 meses)
  ✗ calcular scores compostos (RFM)

Instalação das dependências:
  pip install awswrangler pandas boto3 pyathena scikit-learn

Permissões IAM necessárias para o cientista de dados:
  - athena:StartQueryExecution, GetQueryResults (workgroup cloudmart-analysts)
  - s3:GetObject, s3:PutObject em athena-results-cloudmart-<account>
  - glue:GetTable em cloudmart_features
  - lakeformation:GetDataAccess

Banco   : cloudmart_features
Tabela  : features_clientes
Caminho : s3://gold-zone-<account>/features_clientes/

Colunas disponíveis:
  id_cliente                    string   — chave do cliente
  segmento_cliente              string   — segmento CRM (Premium, Regular…)
  estado_cliente                string   — UF
  total_pedidos                 long     — pedidos concluídos (histórico)
  media_pedidos_mensais         double   — média mensal histórica
  media_compras_ultimos_6_meses double   — média mensal (últimos 6 meses)
  receita_total                 double   — receita bruta total
  ticket_medio                  double   — valor médio por pedido
  ticket_max                    double   — maior pedido
  ticket_min                    double   — menor pedido
  qtd_categorias                long     — categorias distintas compradas
  canal_preferido               string   — canal mais usado
  taxa_cancelamento             double   — taxa de cancelamento (0.0 – 1.0)
  qtd_pedidos_alto_valor        long     — pedidos com valor > R$ 10 000
  dias_ultima_compra            long     — dias desde a última compra
  score_rfm                     double   — score RFM 0-100
  label_churn_risco             int      — 1 = risco de churn (>90 dias inativo)
  feature_extraction_ts         timestamp — data/hora da extração
"""

# =============================================================================
# OPÇÃO 1 — AWS Data Wrangler (recomendado para Data Science)
# Mais Pythônico, retorna diretamente um DataFrame pandas.
# pip install awswrangler
# =============================================================================

import awswrangler as wr
import pandas as pd

WORKGROUP  = "cloudmart-analysts"
DATABASE   = "cloudmart_features"
TABLE      = "features_clientes"
REGION     = "sa-east-1"

def carregar_features_wr() -> pd.DataFrame:
    """Lê todas as features via Athena usando AWS Data Wrangler."""
    df = wr.athena.read_sql_query(
        sql=f"SELECT * FROM {DATABASE}.{TABLE}",
        database=DATABASE,
        workgroup=WORKGROUP,
        boto3_session=None,         # usa credenciais do ambiente (IAM Role / ~/.aws)
        ctas_approach=False,        # DIRECT_QUERY — sem criar tabela temporária
    )
    return df


def carregar_features_para_treino_wr() -> tuple[pd.DataFrame, pd.Series]:
    """
    Retorna (X, y) prontos para sklearn/XGBoost/LightGBM.
    Target: label_churn_risco (classificação binária).
    """
    df = wr.athena.read_sql_query(
        sql="""
            SELECT
                total_pedidos,
                media_pedidos_mensais,
                media_compras_ultimos_6_meses,
                receita_total,
                ticket_medio,
                ticket_max,
                qtd_categorias,
                taxa_cancelamento,
                qtd_pedidos_alto_valor,
                dias_ultima_compra,
                score_rfm,
                label_churn_risco
            FROM cloudmart_features.features_clientes
            WHERE feature_extraction_ts = (
                SELECT MAX(feature_extraction_ts)
                FROM cloudmart_features.features_clientes
            )
        """,
        database=DATABASE,
        workgroup=WORKGROUP,
    )

    feature_cols = [c for c in df.columns if c != "label_churn_risco"]
    X = df[feature_cols]
    y = df["label_churn_risco"]
    return X, y


# =============================================================================
# OPÇÃO 2 — boto3 + Athena (sem dependência adicional além do boto3)
# Útil em ambientes onde awswrangler não está disponível.
# =============================================================================

import boto3
import io
import time

def carregar_features_boto3(
    output_bucket: str = "athena-results-cloudmart-234828142988",
) -> pd.DataFrame:
    """Executa query Athena via boto3 e lê resultado do S3."""
    athena  = boto3.client("athena", region_name=REGION)
    s3      = boto3.client("s3",     region_name=REGION)

    response = athena.start_query_execution(
        QueryString=f"SELECT * FROM {DATABASE}.{TABLE}",
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )
    execution_id = response["QueryExecutionId"]

    # Aguarda conclusão
    while True:
        status = athena.get_query_execution(QueryExecutionId=execution_id)
        state  = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        raise RuntimeError(f"Query Athena falhou com estado: {state}")

    # Lê o CSV de resultado do S3
    result_path = (
        status["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
        .replace("s3://", "")
    )
    bucket, key = result_path.split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    df  = pd.read_csv(io.BytesIO(obj["Body"].read()))
    return df


# =============================================================================
# OPÇÃO 3 — PyAthena (interface padrão DBAPI2 / compatível com SQLAlchemy)
# Útil para integração com ORMs ou ferramentas BI locais.
# pip install pyathena
# =============================================================================

from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

def carregar_features_pyathena(
    s3_staging_dir: str = "s3://athena-results-cloudmart-234828142988/query-results/",
) -> pd.DataFrame:
    """Lê features usando PyAthena com cursor pandas (sem download manual)."""
    conn = connect(
        s3_staging_dir=s3_staging_dir,
        region_name=REGION,
        work_group=WORKGROUP,
        cursor_class=PandasCursor,
    )
    df = conn.cursor().execute(
        f"SELECT * FROM {DATABASE}.{TABLE}"
    ).as_pandas()
    return df


# =============================================================================
# EXEMPLO COMPLETO — Pipeline de treino de modelo de churn
# =============================================================================

def exemplo_treino_modelo_churn():
    """
    Demonstração end-to-end: features → pré-processamento → treino → avaliação.
    Nenhum join ou limpeza de dados é necessário — tudo já está na feature table.
    """
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import LabelEncoder
    from sklearn.metrics import classification_report, roc_auc_score

    print("1. Carregando features do CloudMart Feature Store...")
    X, y = carregar_features_para_treino_wr()

    print(f"   → {len(X)} clientes, {X.shape[1]} features, "
          f"churn rate: {y.mean():.1%}")

    print("2. Divisão treino/teste...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    print("3. Treinando Gradient Boosting Classifier...")
    model = GradientBoostingClassifier(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=4,
        random_state=42,
    )
    model.fit(X_train, y_train)

    print("4. Avaliação:")
    y_pred  = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    print(classification_report(y_test, y_pred, target_names=["Ativo", "Churn"]))
    print(f"   AUC-ROC: {roc_auc_score(y_test, y_proba):.4f}")

    print("5. Importância das features:")
    importances = sorted(
        zip(X.columns, model.feature_importances_),
        key=lambda x: x[1],
        reverse=True,
    )
    for feat, imp in importances:
        print(f"   {feat:<40} {imp:.4f}")

    return model


# =============================================================================
# EXECUÇÃO DIRETA (para testes rápidos)
# =============================================================================

if __name__ == "__main__":
    print("=== CloudMart Feature Store — Acesso Rápido ===\n")

    print("[1] Carregando features via AWS Data Wrangler...")
    df = carregar_features_wr()
    print(df.head())
    print(f"\nShape: {df.shape}")
    print(f"\nEstatísticas básicas:")
    print(df[["score_rfm", "media_compras_ultimos_6_meses",
              "taxa_cancelamento", "dias_ultima_compra"]].describe())

    print("\n[2] Exemplo de pipeline de treino de modelo de churn:")
    modelo = exemplo_treino_modelo_churn()
