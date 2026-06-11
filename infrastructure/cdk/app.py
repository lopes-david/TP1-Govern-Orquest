#!/usr/bin/env python3
"""CDK App – CloudMart Data Platform"""
import aws_cdk as cdk
from stacks.glue_stack import GlueStack
from stacks.pipeline_stack import PipelineStack
from stacks.monitoring_stack import MonitoringStack

app = cdk.App()
env = cdk.Environment(
    account=app.node.try_get_context("account_id") or cdk.Aws.ACCOUNT_ID,
    region=app.node.try_get_context("region") or "us-east-1",
)

glue     = GlueStack(app, "CloudMartGlue", env=env)
pipeline = PipelineStack(app, "CloudMartPipeline", glue_stack=glue, env=env)
MonitoringStack(app, "CloudMartMonitoring", pipeline_stack=pipeline, env=env)

app.synth()
