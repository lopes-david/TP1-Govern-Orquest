"""
Stack CDK: Feature Store para Machine Learning

Provisiona a camada de serving de dados para Cientistas de Dados:

  1. Glue Database cloudmart_features
       Namespace isolado para todas as tabelas de features ML.

  2. Glue PySparkEtlJob cloudmart-features-clientes
       Executa silver_to_features_clientes.py:
         • Lê vendas da silver zone (histórico completo)
         • Computa 16 variáveis preditivas por cliente
         • Persiste como Parquet em gold-zone/features_clientes/

  3. Glue Crawler cloudmart-features-crawler
       Registra features_clientes no Glue Catalog após execução do job.
       Permite consulta imediata via Athena sem DDL manual.

Fluxo de consumo para ML:
  silver_to_features job → s3://gold-zone/features_clientes/ → Crawler
  → cloudmart_features.features_clientes (Athena) → Data Scientist
"""

import os
from aws_cdk import (
    Duration,
    Stack,
    aws_glue as glue,
    aws_glue_alpha as glue_alpha,
    aws_iam as iam,
)
from constructs import Construct


class FeatureStoreStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        account_id    = self.node.try_get_context("account_id") or self.account
        silver_bucket = f"silver-zone-{account_id}"
        gold_bucket   = f"gold-zone-{account_id}"
        scripts_bucket = self.node.try_get_context("scripts_bucket") or "meu-datalake-glue-scripts"

        # Reutiliza o role Glue já existente na conta
        glue_role = iam.Role.from_role_name(self, "GlueEtlRole", "GlueETLRole")

        # ── 1. Banco de dados cloudmart_features ─────────────────────────────
        db = glue.CfnDatabase(
            self, "FeaturesDatabase",
            catalog_id=account_id,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="cloudmart_features",
                description=(
                    "Feature Store lógica para ML — variáveis preditivas "
                    "pré-computadas e limpas, prontas para treino de modelos."
                ),
            ),
        )

        # ── 2. Glue Job — engenharia de features ─────────────────────────────
        scripts_root = os.path.join(
            os.path.dirname(__file__), "../../../glue_jobs"
        )

        features_job = glue_alpha.PySparkEtlJob(
            self, "FeaturesClientesJob",
            job_name="cloudmart-features-clientes",
            script=glue_alpha.Code.from_asset(
                os.path.join(scripts_root, "silver_to_features/silver_to_features_clientes.py")
            ),
            role=glue_role,
            glue_version=glue_alpha.GlueVersion.V4_0,
            default_arguments={
                "--SILVER_BUCKET": silver_bucket,
                "--GOLD_BUCKET":   gold_bucket,
            },
            worker_type=glue_alpha.WorkerType.G_1X,
            number_of_workers=2,
            timeout=Duration.minutes(45),
            continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
            description=(
                "Computa feature store de clientes: RFM score, churn label, "
                "media_compras_ultimos_6_meses e demais variáveis preditivas."
            ),
        )

        # ── 3. Glue Crawler — registra features_clientes no Catalog ──────────
        crawler = glue.CfnCrawler(
            self, "FeaturesCrawler",
            name="cloudmart-features-crawler",
            description=(
                "Registra cloudmart_features.features_clientes no Glue Catalog "
                "após execução do job de feature engineering."
            ),
            role=glue_role.role_arn,
            database_name="cloudmart_features",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{gold_bucket}/features_clientes/",
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="LOG",
                delete_behavior="LOG",
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY",
            ),
        )
        crawler.add_dependency(db)
