"""
Stack CDK: Governança de Dados — PII & LGPD (Lake Formation + Glue).

Provisiona:

  1. LF-Tags no Lake Formation
       PII                — tipo de dado pessoal (CPF, Email, Telefone…)
       LGPD_Classification — categoria LGPD (dado_pessoal, dado_pessoal_sensivel, nao_pessoal)

  2. IAM Role para o Glue PII Scanner (CloudMartPiiScannerRole)
       - AWSGlueServiceRole (gerenciado AWS)
       - Permissões de leitura no Glue Catalog (GetTable, GetTables, UpdateTable)
       - Permissões de tagging no Lake Formation
       - Permissões S3 (leitura dos dados + escrita do relatório)

  3. Glue PySparkEtlJob cloudmart-piiscanner
       Script: glue_jobs/pii_scanner/pii_scanner.py
"""

import os
from aws_cdk import (
    Stack, Duration,
    aws_glue_alpha as glue_alpha,
    aws_iam as iam,
    aws_lakeformation as lf,
)
from constructs import Construct


# Todos os valores possíveis para a LF-Tag PII
_PII_TAG_VALUES = [
    "CPF", "CNPJ", "Email", "Telefone", "NomePessoa",
    "DataNascimento", "Endereco", "RG", "CEP",
    "Senha", "DadoSaude", "OrigemEtnica", "CrencaReligiosa", "DadoBiometrico",
]

# Classificações LGPD (Art. 5°)
_LGPD_TAG_VALUES = ["dado_pessoal", "dado_pessoal_sensivel", "nao_pessoal"]


class GovernanceStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        account_id     = self.node.try_get_context("account_id") or self.account
        scripts_bucket = self.node.try_get_context("scripts_bucket") or "meu-datalake-glue-scripts"
        bronze_bucket  = f"bronze-zone-{account_id}"
        silver_bucket  = f"silver-zone-{account_id}"

        # ── 1. LF-Tags ──────────────────────────────────────────────────────
        # Criadas no deploy; o PII Scanner as aplica em colunas em runtime.
        lf.CfnTag(
            self, "PiiLfTag",
            tag_key="PII",
            tag_values=_PII_TAG_VALUES,
        )

        lf.CfnTag(
            self, "LgpdLfTag",
            tag_key="LGPD_Classification",
            tag_values=_LGPD_TAG_VALUES,
        )

        # ── 2. IAM Role ──────────────────────────────────────────────────────
        pii_role = self._create_pii_scanner_role(
            bronze_bucket=bronze_bucket,
            silver_bucket=silver_bucket,
            scripts_bucket=scripts_bucket,
        )

        # Registra o role como Data Lake Administrator para que possa aplicar
        # LF-Tags em qualquer banco/tabela sem precisar de permissões individuais.
        # Em produção, substituir por CfnPermissions granulares por banco/tabela.
        lf.CfnDataLakeSettings(
            self, "PiiScannerDlAdmin",
            admins=[
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=pii_role.role_arn
                )
            ],
        )

        # ── 3. Glue Job — PII Scanner ────────────────────────────────────────
        scripts_root = os.path.join(
            os.path.dirname(__file__), "../../../glue_jobs"
        )

        glue_alpha.PySparkEtlJob(
            self, "PiiScanner",
            job_name="cloudmart-piiscanner",
            script=glue_alpha.Code.from_asset(
                os.path.join(scripts_root, "pii_scanner/pii_scanner.py")
            ),
            role=pii_role,
            glue_version=glue_alpha.GlueVersion.V4_0,
            default_arguments={
                "--BRONZE_DB":     "cloudmart_bronze",
                "--SILVER_DB":     "cloudmart_silver",
                "--BRONZE_BUCKET": bronze_bucket,
                "--SILVER_BUCKET": silver_bucket,
                "--REPORT_BUCKET": scripts_bucket,
            },
            worker_type=glue_alpha.WorkerType.G_1X,
            number_of_workers=2,
            timeout=Duration.minutes(60),
            continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
        )

    # ─────────────────────────────────────────────────────────────────────────

    def _create_pii_scanner_role(
        self,
        bronze_bucket: str,
        silver_bucket: str,
        scripts_bucket: str,
    ) -> iam.Role:
        role = iam.Role(
            self, "PiiScannerRole",
            role_name="CloudMartPiiScannerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )

        # Glue Data Catalog — leitura e atualização de metadados de colunas
        role.add_to_policy(iam.PolicyStatement(
            sid="GlueCatalogAccess",
            actions=[
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:UpdateTable",
            ],
            resources=["*"],
        ))

        # Lake Formation — criação e aplicação de LF-Tags
        role.add_to_policy(iam.PolicyStatement(
            sid="LakeFormationTagging",
            actions=[
                "lakeformation:AddLFTagsToResource",
                "lakeformation:RemoveLFTagsFromResource",
                "lakeformation:CreateLFTag",
                "lakeformation:UpdateLFTag",
                "lakeformation:GetLFTag",
                "lakeformation:ListLFTags",
                "lakeformation:GetResourceLFTags",
            ],
            resources=["*"],
        ))

        # S3 — leitura dos Parquets para amostragem
        role.add_to_policy(iam.PolicyStatement(
            sid="S3DataRead",
            actions=["s3:GetObject", "s3:ListBucket"],
            resources=[
                f"arn:aws:s3:::{bronze_bucket}",
                f"arn:aws:s3:::{bronze_bucket}/*",
                f"arn:aws:s3:::{silver_bucket}",
                f"arn:aws:s3:::{silver_bucket}/*",
            ],
        ))

        # S3 — escrita do relatório JSON de PII
        role.add_to_policy(iam.PolicyStatement(
            sid="S3ReportWrite",
            actions=["s3:PutObject"],
            resources=[
                f"arn:aws:s3:::{scripts_bucket}/pii-reports/*",
            ],
        ))

        return role
