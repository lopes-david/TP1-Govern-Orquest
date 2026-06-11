#!/usr/bin/env python3
"""CDK App – CloudMart Data Platform (conta 234828142988, us-east-1)"""
import aws_cdk as cdk
from stacks.glue_stack import GlueStack
from stacks.pipeline_stack import PipelineStack
from stacks.monitoring_stack import MonitoringStack

app = cdk.App()
env = cdk.Environment(account="234828142988", region="us-east-1")

glue     = GlueStack(app, "CloudMartGlue", env=env)
pipeline = PipelineStack(app, "CloudMartPipeline", glue_stack=glue, env=env)
MonitoringStack(app, "CloudMartMonitoring", pipeline_stack=pipeline, env=env)

app.synth()
