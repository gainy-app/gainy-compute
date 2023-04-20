import os

from gainy.utils import get_logger

logger = get_logger(__name__)

SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
SLACK_NOTIFICATIONS_CHANNEL = os.getenv('SLACK_NOTIFICATIONS_CHANNEL',
                                        "#build-release")


class Slack:

    def send_message(self, message):
        if not SLACK_BOT_TOKEN:
            return

        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError

        client = WebClient(token=SLACK_BOT_TOKEN)

        try:
            return client.chat_postMessage(channel=SLACK_NOTIFICATIONS_CHANNEL,
                                           text=message)
        except SlackApiError as e:
            logger.exception(e)
