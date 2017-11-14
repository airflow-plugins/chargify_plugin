from airflow.plugins_manager import AirflowPlugin
from chargify_plugin.hooks.chargify_hook import ChargifyHook
from chargify_plugin.operators.chargify_to_s3_operator import ChargifyToS3Operator


class ChargifyPlugin(AirflowPlugin):
    name = "chargify_plugin"
    operators = [ChargifyToS3Operator]
    hooks = [ChargifyHook]
