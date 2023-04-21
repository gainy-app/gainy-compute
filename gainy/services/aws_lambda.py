import boto3
from gainy.utils import get_logger

logger = get_logger(__name__)


class AWSLambda:

    def __init__(self):
        self.client = boto3.client("lambda")

    def invoke(self, func, payload=None, sync: bool = True):
        return self.client.invoke(
            FunctionName=func,
            InvocationType="RequestResponse" if sync else "Event",
            Payload=payload)
