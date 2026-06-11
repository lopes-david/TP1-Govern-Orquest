"""
Stack CDK: Glue Jobs para o pipeline CloudMart.
Referencia os buckets existentes (raw/bronze/silver) pelo nome.
"""
from aws_cdk import (
    Stack, Duration,
    aws_glue_alpha as glue_alpha,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct
import os

ACCOUNT_ID    = "234828142988"
RAW_BUCKET    = f"raw-zone-{ACCOUNT_ID}"
BRONZE_BUCKET = f"bronze-zone-{ACCOUNT_ID}"
SILVER_BUCKET = f"silver-zone-{ACCOUNT_ID}"
SCRIPTS_BUCKET = "meu-datalake-glue-scripts-3f566d84"


class GlueStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        role = self._create_glue_role()
        self.jobs = self._create_jobs(role)

    def _create_glue_role(self) -> iam.Role:
        role = iam.Role(
            self, "GlueServiceRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )
        for bucket_name in [RAW_BUCKET, BRONZE_BUCKET, SILVER_BUCKET, SCRIPTS_BUCKET]:
            bucket = s3.Bucket.from_bucket_name(self, f"Ref{bucket_name[:8]}", bucket_name)
            bucket.grant_read_write(role)
        return role

    def _create_jobs(self, role: iam.Role) -> dict:
        scripts_root = os.path.join(
            os.path.dirname(__file__), "../../../glue_jobs"
        )

        configs = [
            {
                "id": "RawToBronze",
                "script": "raw_to_bronze/raw_to_bronze.py",
                "args": {
                    "--RAW_BUCKET":    RAW_BUCKET,
                    "--BRONZE_BUCKET": BRONZE_BUCKET,
                },
            },
            {
                "id": "BronzeToSilverVendas",
                "script": "bronze_to_silver/bronze_to_silver_vendas.py",
                "args": {
                    "--BRONZE_BUCKET": BRONZE_BUCKET,
                    "--SILVER_BUCKET": SILVER_BUCKET,
                },
            },
            {
                "id": "SilverToGoldRelatorio",
                "script": "silver_to_gold/silver_to_gold_relatorio_vendas.py",
                "args": {
                    "--SILVER_BUCKET": SILVER_BUCKET,
                    "--GOLD_BUCKET":   f"gold-zone-{ACCOUNT_ID}",
                    "--ANO":           "2024",
                },
            },
        ]

        jobs = {}
        for cfg in configs:
            job = glue_alpha.Job(
                self, cfg["id"],
                job_name=f"cloudmart-{cfg['id'].lower()}",
                executable=glue_alpha.JobExecutable.python_etl(
                    glue_version=glue_alpha.GlueVersion.V4_0,
                    python_version=glue_alpha.PythonVersion.THREE,
                    script=glue_alpha.Code.from_asset(
                        os.path.join(scripts_root, cfg["script"])
                    ),
                ),
                role=role,
                default_arguments=cfg["args"],
                worker_type=glue_alpha.WorkerType.G_1_X,
                worker_count=2,
                timeout=Duration.minutes(30),
                continuous_logging=glue_alpha.ContinuousLoggingProps(enabled=True),
            )
            jobs[cfg["id"]] = job
        return jobs
