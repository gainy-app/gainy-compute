from gainy.exceptions import NotFoundException
from gainy.trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from gainy.trading.drivewealth.models import DriveWealthStatement
from gainy.trading.models import TradingStatementType
from gainy.utils import get_logger

logger = get_logger(__name__)


class StatementCreatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type in [
            "statements.created", "tradeconfirms.created", "taxforms.created"
        ]

    def handle(self, event_payload: dict):
        entity = DriveWealthStatement()
        if "taxForm" in event_payload:
            entity.set_from_response(event_payload["taxForm"])
            entity.type = TradingStatementType.TAX
        elif "tradeConfirm" in event_payload:
            entity.set_from_response(event_payload["tradeConfirm"])
            entity.type = TradingStatementType.TRADE_CONFIRMATION
        elif "statement" in event_payload:
            entity.set_from_response(event_payload["statement"])
            entity.type = TradingStatementType.MONTHLY_STATEMENT

        entity.account_id = event_payload["accountID"]
        entity.user_id = event_payload["userID"]

        self.repo.refresh(entity)
        self.repo.persist(entity)

        try:
            profile_id = self.provider.get_profile_id_by_user_id(
                entity.user_id)
        except NotFoundException:
            return
        self.provider.create_trading_statements([entity], profile_id)
