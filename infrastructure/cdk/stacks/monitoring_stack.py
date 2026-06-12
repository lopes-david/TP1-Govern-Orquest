"""
Stack CDK: Monitoramento (atividades 4.1 e 4.2)
- CloudWatch Alarms para Step Functions (falha, timeout, throttle)
- CloudWatch Dashboard com painel visual do pipeline
- SNS Topic com e-mail para notificações
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
        sfn_arn     = pipeline_stack.state_machine.state_machine_arn
        sfn_name    = pipeline_stack.state_machine.state_machine_name

        # --- SNS Topic (4.2) ---
        topic = sns.Topic(
            self, "AlertsTopic",
            display_name="CloudMart Pipeline Alerts",
        )
        if alert_email:
            topic.add_subscription(sns_subs.EmailSubscription(alert_email))

        alarm_action = cw_actions.SnsAction(topic)

        # --- Alarme 1: Execuções com falha (4.2 — requisito principal) ---
        alarm_failed = cw.Alarm(
            self, "AlarmFalha",
            alarm_name="cloudmart-pipeline-falha",
            alarm_description=(
                "O pipeline CloudMart falhou. "
                "Verifique o Step Functions e os logs do Glue no CloudWatch."
            ),
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
        alarm_failed.add_alarm_action(alarm_action)

        # --- Alarme 2: Timeout (SLA > 3h) ---
        alarm_timeout = cw.Alarm(
            self, "AlarmTimeout",
            alarm_name="cloudmart-pipeline-timeout",
            alarm_description="Pipeline ultrapassou o SLA de 3 horas.",
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
        alarm_timeout.add_alarm_action(alarm_action)

        # --- Alarme 3: Throttling ---
        alarm_throttle = cw.Alarm(
            self, "AlarmThrottle",
            alarm_name="cloudmart-pipeline-throttle",
            alarm_description="Execuções do pipeline estão sendo limitadas pela AWS.",
            metric=cw.Metric(
                namespace="AWS/States",
                metric_name="ExecutionThrottled",
                dimensions_map={"StateMachineArn": sfn_arn},
                period=Duration.minutes(5),
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )
        alarm_throttle.add_alarm_action(alarm_action)

        # --- Dashboard CloudWatch (4.1) ---
        dashboard = cw.Dashboard(
            self, "Dashboard",
            dashboard_name="CloudMart-Pipeline-Dashboard",
        )

        dashboard.add_widgets(
            cw.TextWidget(
                markdown="# CloudMart Data Pipeline — Monitoramento\nPipeline: `cloudmart-data-pipeline` | Região: sa-east-1",
                width=24, height=2,
            )
        )

        dashboard.add_widgets(
            cw.AlarmWidget(
                alarm=alarm_failed,
                title="Falhas",
                width=8,
            ),
            cw.AlarmWidget(
                alarm=alarm_timeout,
                title="Timeouts",
                width=8,
            ),
            cw.AlarmWidget(
                alarm=alarm_throttle,
                title="Throttling",
                width=8,
            ),
        )

        dashboard.add_widgets(
            cw.GraphWidget(
                title="Execuções por Status (últimas 24h)",
                width=12,
                left=[
                    cw.Metric(
                        namespace="AWS/States",
                        metric_name="ExecutionsSucceeded",
                        dimensions_map={"StateMachineArn": sfn_arn},
                        period=Duration.hours(1),
                        statistic="Sum",
                        label="Sucesso",
                        color=cw.Color.GREEN,
                    ),
                    cw.Metric(
                        namespace="AWS/States",
                        metric_name="ExecutionsFailed",
                        dimensions_map={"StateMachineArn": sfn_arn},
                        period=Duration.hours(1),
                        statistic="Sum",
                        label="Falha",
                        color=cw.Color.RED,
                    ),
                ],
            ),
            cw.GraphWidget(
                title="Duração Média das Execuções (ms)",
                width=12,
                left=[
                    cw.Metric(
                        namespace="AWS/States",
                        metric_name="ExecutionTime",
                        dimensions_map={"StateMachineArn": sfn_arn},
                        period=Duration.hours(1),
                        statistic="Average",
                        label="Duração média",
                        color=cw.Color.BLUE,
                    ),
                ],
            ),
        )
