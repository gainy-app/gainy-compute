from gainy.context_container import ContextContainer
from gainy.utils import get_logger

logger = get_logger(__name__)


def cli():
    try:
        with ContextContainer() as context_container:
            context_container.billing_service.charge_invoices()

    except Exception as e:
        logger.exception(e)
        raise e
