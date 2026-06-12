"""
Stack CDK: CI/CD Pipeline
CodePipeline + CodeBuild conectado ao GitHub (main branch).
Ao detectar push no main:
  1. SyncScripts — sincroniza glue_jobs/ para o S3
  2. CdkDeploy   — executa cdk deploy --all

Todos os parâmetros vêm de contexto CDK (--context) sem nada hardcoded.
O GitHub token é lido do Secrets Manager — nunca em texto plano.
"""
from aws_cdk import (
    Stack,
    aws_codepipeline as cp,
    aws_codepipeline_actions as cpa,
    aws_codebuild as cb,
    aws_iam as iam,
    aws_s3 as s3,
    SecretValue,
)
from constructs import Construct


class CiCdStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        github_secret_name  = self.node.try_get_context("github_secret_name")
        github_owner        = self.node.try_get_context("github_owner")
        github_repo         = self.node.try_get_context("github_repo")
        github_branch       = self.node.try_get_context("github_branch") or "main"
        scripts_bucket_name = self.node.try_get_context("scripts_bucket")

        scripts_bucket = s3.Bucket.from_bucket_name(
            self, "ScriptsBucket", scripts_bucket_name
        )

        pipeline_role = self._create_pipeline_role(scripts_bucket)

        # --- Source: GitHub via webhook ---
        source_output = cp.Artifact("SourceOutput")

        source_action = cpa.GitHubSourceAction(
            action_name="GitHub_Source",
            owner=github_owner,
            repo=github_repo,
            branch=github_branch,
            oauth_token=SecretValue.secrets_manager(github_secret_name),
            output=source_output,
            trigger=cpa.GitHubTrigger.WEBHOOK,
        )

        # --- Estágio 1: sync scripts Glue → S3 ---
        sync_project = cb.PipelineProject(
            self, "SyncScriptsProject",
            project_name="cloudmart-sync-glue-scripts",
            build_spec=cb.BuildSpec.from_object({
                "version": "0.2",
                "phases": {
                    "install": {
                        "runtime-versions": {"python": "3.11"},
                        "commands": ["pip install --upgrade awscli"],
                    },
                    "build": {
                        "commands": [
                            "echo Sincronizando scripts Glue...",
                            "aws s3 sync glue_jobs/ s3://$GLUE_SCRIPTS_BUCKET/scripts/"
                            " --exclude '*.pyc' --exclude '__pycache__/*' --delete",
                            "echo Sync concluido.",
                        ]
                    },
                },
            }),
            environment=cb.BuildEnvironment(
                build_image=cb.LinuxBuildImage.STANDARD_7_0,
            ),
            environment_variables={
                "GLUE_SCRIPTS_BUCKET": cb.BuildEnvironmentVariable(
                    value=scripts_bucket_name
                ),
            },
            role=pipeline_role,
        )

        # --- Estágio 2: CDK deploy ---
        cdk_project = cb.PipelineProject(
            self, "CdkDeployProject",
            project_name="cloudmart-cdk-deploy",
            build_spec=cb.BuildSpec.from_object({
                "version": "0.2",
                "phases": {
                    "install": {
                        "runtime-versions": {"python": "3.11", "nodejs": "20"},
                        "commands": [
                            "npm install -g aws-cdk",
                            "pip install -r infrastructure/cdk/requirements.txt",
                        ],
                    },
                    "pre_build": {
                        "commands": [
                            "aws sts get-caller-identity",
                            "cd infrastructure/cdk && cdk synth --no-staging",
                        ]
                    },
                    "build": {
                        "commands": [
                            "cdk deploy --all --require-approval never",
                        ]
                    },
                },
            }),
            environment=cb.BuildEnvironment(
                build_image=cb.LinuxBuildImage.STANDARD_7_0,
            ),
            role=pipeline_role,
        )

        # --- Pipeline completo ---
        self.pipeline = cp.Pipeline(
            self, "CloudMartCiCdPipeline",
            pipeline_name="cloudmart-ci-cd-pipeline",
            role=pipeline_role,
            stages=[
                cp.StageProps(stage_name="Source",      actions=[source_action]),
                cp.StageProps(stage_name="SyncScripts", actions=[cpa.CodeBuildAction(
                    action_name="Sync_Glue_Scripts",
                    project=sync_project,
                    input=source_output,
                )]),
                cp.StageProps(stage_name="CdkDeploy",   actions=[cpa.CodeBuildAction(
                    action_name="CDK_Deploy",
                    project=cdk_project,
                    input=source_output,
                )]),
            ],
        )

    def _create_pipeline_role(self, scripts_bucket) -> iam.Role:
        role = iam.Role(
            self, "PipelineRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("codepipeline.amazonaws.com"),
                iam.ServicePrincipal("codebuild.amazonaws.com"),
            ),
        )
        scripts_bucket.grant_read_write(role)
        role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")
        )
        return role
