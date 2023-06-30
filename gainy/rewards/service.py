import os

from _decimal import Decimal

from gainy.models import Invitation
from gainy.rewards.models import InvitationCashReward
from gainy.trading.repository import TradingRepository
from gainy.trading.service import TradingService

REWARD_INVITATION_CASH_AMOUNT = os.getenv("REWARD_INVITATION_CASH_AMOUNT")
REWARD_INVITATION_CASH_AMOUNT = Decimal(
    REWARD_INVITATION_CASH_AMOUNT
) if REWARD_INVITATION_CASH_AMOUNT is not None else None


class RewardService:

    def __init__(self, trading_service: TradingService,
                 trading_repository: TradingRepository):
        self.trading_service = trading_service
        self.trading_repository = trading_repository

    def reward_invitation_with_cash(self, profile_id, invitation: Invitation):
        amount = REWARD_INVITATION_CASH_AMOUNT
        if not amount or amount < 0:
            raise Exception("REWARD_INVITATION_CASH_AMOUNT is incorrect")

        trading_account = self.trading_repository.get_trading_account(
            profile_id)
        money_flow = self.trading_service.reward_with_cash(
            trading_account, amount)

        cash_reward = InvitationCashReward()
        cash_reward.invitation_id = invitation.id
        cash_reward.profile_id = profile_id
        cash_reward.money_flow_id = money_flow.id
        self.trading_repository.persist(cash_reward)
