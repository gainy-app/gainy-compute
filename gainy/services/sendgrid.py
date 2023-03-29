from typing import Union

import python_http_client
import sendgrid
import os

from gainy.exceptions import EmailNotSentException
from gainy.utils import get_logger

logger = get_logger(__name__)

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
SENDGRID_FROM_EMAIL = 'no-reply@gainy.app'


class SendGridService:

    def send_email(self,
                   to: Union[str, list[str]],
                   subject: str = None,
                   content_plain=None,
                   dynamic_template_data: dict = None,
                   template_id: str = None):
        from sendgrid.helpers.mail import Mail, Email, To, Content

        if isinstance(to, str):
            to = [to]

        plain_text_content = Content("text/plain",
                                     content_plain) if content_plain else None
        email = Mail(from_email=Email(SENDGRID_FROM_EMAIL),
                     to_emails=[To(i) for i in to],
                     subject=subject,
                     plain_text_content=plain_text_content)

        if dynamic_template_data:
            email.dynamic_template_data = dynamic_template_data
        if template_id:
            email.template_id = template_id

        sg = sendgrid.SendGridAPIClient(api_key=SENDGRID_API_KEY)
        response: python_http_client.client.Response = sg.client.mail.send.post(
            request_body=email.get())

        logger.info('Sent email',
                    extra={
                        "to": to,
                        "subject": subject,
                        "content_plain": content_plain,
                        "dynamic_template_data": dynamic_template_data,
                        "template_id": template_id,
                        "response_status_code": response.status_code,
                        "response_body": response.body,
                        "response_headers": response.headers,
                    })

        if response.status_code != 202:
            raise EmailNotSentException()

        return response
