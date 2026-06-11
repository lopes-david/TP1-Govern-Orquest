"""
Stack CDK: Step Functions – orquestra o pipeline raw→bronze→silver→gold.
"""
from aws_cdk import (
    Stack, Duration,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam,
)
from constructs import Construct
from .glue_stack import GlueStack


class PipelineStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        glue_stack: GlueStack,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        sfn_role = iam.Role(
            self, "SfnRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSGlueConsoleFullAccess"),
            ],
        )

        step_raw_bronze = tasks.GlueStartJobRun(
            self, "StepRawToBronze",
            glue_job_name=glue_stack.jobs["RawToBronze"].job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            timeout=Duration.minutes(30),
        )

        step_bronze_silver = tasks.GlueStartJobRun(
            self, "StepBronzeToSilver",
            glue_job_name=glue_stack.jobs["BronzeToSilverVendas"].job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            timeout=Duration.minutes(30),
        )

        step_silver_gold = tasks.GlueStartJobRun(
            self, "StepSilverToGold",
            glue_job_name=glue_stack.jobs["SilverToGoldRelatorio"].job_name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            timeout=Duration.minutes(30),
        )

        definition = step_raw_bronze.next(step_bronze_silver).next(step_silver_gold)

        self.state_machine = sfn.StateMachine(
            self, "CloudMartPipeline",
            state_machine_name="cloudmart-data-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            role=sfn_role,
            timeout=Duration.hours(2),
        )
