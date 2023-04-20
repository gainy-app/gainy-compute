import re

import boto3
from gainy.utils import get_logger

logger = get_logger(__name__)


class ECS:

    def __init__(self):
        self.ecs_client = boto3.client("ecs")

    def describe_task_definition(self, id):
        return self.ecs_client.describe_task_definition(taskDefinition=id,
                                                        include=[
                                                            'TAGS',
                                                        ])

    def describe_service(self, service_arns):
        if isinstance(service_arns, str):
            service_arns = [service_arns]

        cluster_arn = None
        for arn in service_arns:
            s = re.search('service/(\S+)/', arn)[1]
            if cluster_arn is None:
                cluster_arn = s
            elif cluster_arn != s:
                raise Exception(
                    'Trying to describe services from different clusters: %s and %s'
                    % (cluster_arn, s))

        return self.ecs_client.describe_services(services=service_arns,
                                                 cluster=cluster_arn)
