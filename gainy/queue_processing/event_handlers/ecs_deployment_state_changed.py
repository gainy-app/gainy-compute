import base64
import datetime

import dateutil.parser

from gainy.utils import get_logger, env
from gainy.queue_processing.event_handlers.abstract_aws_event_handler import AbstractAwsEventHandler
from gainy.services.aws_ecs import ECS
from gainy.services.slack import Slack

logger = get_logger(__name__)
ENV = env()


class ECSDeploymentStateChangeEventHandler(AbstractAwsEventHandler):

    def supports(self, event_type: str):
        return event_type == "ECS Deployment State Change"

    def handle(self, body: dict):
        event_payload = body["detail"]
        logger_extra = {
            "body": body,
        }
        try:
            event_name = event_payload["eventName"]
            if event_name != "SERVICE_DEPLOYMENT_COMPLETED":
                return

            updated_at = event_payload.get("updatedAt")
            updated_at_ago = datetime.datetime.now(
                tz=datetime.timezone.utc) - dateutil.parser.parse(updated_at)
            logger_extra["updated_at_ago"] = updated_at_ago
            if updated_at_ago > datetime.timedelta(minutes=15):
                return

            deployment_id = event_payload["deploymentId"]
            service_arn = body["resources"][0]

            ecs = ECS()
            services = ecs.describe_service(service_arn)["services"]
            logger_extra["services"] = services
            task_def_arns = []
            for service in services:
                for deployment in service["deployments"]:
                    if deployment["id"] == deployment_id:
                        task_def_arns.append(deployment["taskDefinition"])
            logger_extra["task_def_arns"] = task_def_arns

            task_defs = []
            branch = None
            branch_name = None
            for task_def_arn in task_def_arns:
                task_def = ecs.describe_task_definition(task_def_arn)
                task_defs.append(task_def)
                logger_extra["task_defs"] = task_defs

                tags = {t["key"]: t["value"] for t in task_def.get("tags", [])}
                task_def_env = tags.get("environment")
                if task_def_env is None or task_def_env != ENV:
                    continue

                branch = tags.get("source_code_branch")
                branch_name = tags.get("source_code_branch_name")
                try:
                    branch_name = base64.b64decode(branch_name).decode('utf-8')
                except:
                    pass

                if not branch_name and not branch:
                    continue

                logger_extra["tags"] = tags
                logger_extra["env"] = ENV
                logger_extra["branch"] = branch
                logger_extra["branch_name"] = branch_name

            if not branch_name and not branch:
                return
            message = f":large_green_circle: Branch {branch_name or branch} is deployed to *{ENV}*."

            logger_extra["message_text"] = message
            response = Slack().send_message(message)
            logger_extra["response"] = response
        finally:
            logger.info("ECSDeploymentStateChangeEventHandler",
                        extra=logger_extra)
