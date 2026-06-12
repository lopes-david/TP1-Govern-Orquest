"""
Stack CDK: Step Functions
Máquina de estados que orquestra o pipeline CloudMart em 3 etapas:
  1. Ingestão        — Glue Job raw → bronze
  2. Transformação   — Glue Job bronze → silver (lógica dbt)
  3. Atualização     — Glue Job silver → gold + Crawler Athena
"""
import json
import os
from aws_cdk import (
    Stack, Duration,
    aws_stepfunctions as sfn,
    aws_iam as iam,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
)
from constructs import Construct
from .glue_stack import GlueStack

ASL_PATH = os.path.join(
    os.path.dirname(__file__),
    "../../../step_functions/cloudmart_pipeline.json",
)


class PipelineStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        glue_stack: GlueStack,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        alert_email = self.node.try_get_context("alert_email")

        # Tópico SNS para notificações de falha
        self.alert_topic = sns.Topic(
            self, "PipelineAlerts",
            display_name="CloudMart Pipeline Alerts",
        )
        if alert_email:
            self.alert_topic.add_subscription(
                sns_subs.EmailSubscription(alert_email)
            )

        sfn_role = self._create_sfn_role(glue_stack)

        # Carrega a definição ASL do arquivo JSON
        with open(ASL_PATH) as f:
            definition = json.load(f)

        self.state_machine = sfn.StateMachine(
            self, "CloudMartPipeline",
            state_machine_name="cloudmart-data-pipeline",
            definition_body=sfn.DefinitionBody.from_string(
                json.dumps(definition)
            ),
            role=sfn_role,
            timeout=Duration.hours(3),
            tracing_enabled=True,
        )

    def _create_sfn_role(self, glue_stack: GlueStack) -> iam.Role:
        role = iam.Role(
            self, "SfnRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )

        # Permissão para disparar e monitorar Glue Jobs
        role.add_to_policy(iam.PolicyStatement(
            actions=[
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetJobRuns",
                "glue:BatchStopJobRun",
            ],
            resources=["*"],
        ))

        # Permissão para iniciar e monitorar Glue Crawlers
        role.add_to_policy(iam.PolicyStatement(
            actions=[
                "glue:StartCrawler",
                "glue:GetCrawler",
                "glue:StopCrawler",
            ],
            resources=["*"],
        ))

        # Permissão para publicar no SNS
        role.add_to_policy(iam.PolicyStatement(
            actions=["sns:Publish"],
            resources=["*"],
        ))

        # Permissão para X-Ray tracing
        role.add_to_policy(iam.PolicyStatement(
            actions=[
                "xray:PutTraceSegments",
                "xray:PutTelemetryRecords",
            ],
            resources=["*"],
        ))

        return role
