"""
Stack CDK: CloudWatch Alarms + SNS para alertas do pipeline CloudMart.
Monitora falhas e timeout da Step Functions state machine.
"""
from aws_cdk import (
    Stack, Duration,
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
)
from constructs import Construct
from .pipeline_stack import PipelineStack

class MonitoringStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        pipeline_stack: PipelineStack,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        alert_email = self.node.try_get_context("alert_email")

        topic = sns.Topic(self, "AlertsTopic", display_name="CloudMart Pipeline Alerts")
        if alert_email:
            topic.add_subscription(sns_subs.EmailSubscription(alert_email))

        sfn_arn = pipeline_stack.state_machine.state_machine_arn

        # Alerta 1: qualquer execução com falha
        failed = cw.Alarm(
            self, "PipelineFailed",
            alarm_name="cloudmart-pipeline-falha",
            alarm_description="Pipeline falhou — verificar logs no CloudWatch.",
            metric=cw.Metric(
                namespace="AWS/States",
                metric_name="ExecutionsFailed",
                dimensions_map={"StateMachineArn": sfn_arn},
                period=Duration.minutes(5),
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        failed.add_alarm_action(cw_actions.SnsAction(topic))

        # Alerta 2: execução demorou mais de 2h (SLA)
        timed_out = cw.Alarm(
            self, "PipelineTimedOut",
            alarm_name="cloudmart-pipeline-timeout",
            alarm_description="Pipeline ultrapassou 2h de execução.",
            metric=cw.Metric(
                namespace="AWS/States",
                metric_name="ExecutionsTimedOut",
                dimensions_map={"StateMachineArn": sfn_arn},
                period=Duration.minutes(5),
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        timed_out.add_alarm_action(cw_actions.SnsAction(topic))
