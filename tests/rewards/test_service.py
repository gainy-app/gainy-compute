from _decimal import Decimal

from gainy.models import Invitation
from gainy.rewards.models import InvitationCashReward
from gainy.rewards.service import RewardService
from gainy.tests.mocks.repository_mocks import mock_persist
from gainy.trading.models import TradingAccount, TradingMoneyFlow
from gainy.trading.repository import TradingRepository
from gainy.trading.service import TradingService


def test(monkeypatch):
    profile_id = 1
    invitation_id = 2
    amount = Decimal(3)
    money_flow_id = 4

    trading_account = TradingAccount()

    money_flow = TradingMoneyFlow()
    money_flow.id = money_flow_id

    invitation = Invitation()
    invitation.id = invitation_id

    trading_service = TradingService(None, None, None)

    def mock_reward_with_cash(_trading_account, _amount):
        assert _trading_account == trading_account
        assert _amount == amount
        return money_flow

    monkeypatch.setattr(trading_service, "reward_with_cash",
                        mock_reward_with_cash)

    trading_repository = TradingRepository(None)

    def mock_get_trading_account(_profile_id):
        assert _profile_id == profile_id
        return trading_account

    monkeypatch.setattr(trading_repository, "get_trading_account",
                        mock_get_trading_account)

    persisted_objects = {}
    monkeypatch.setattr(trading_repository, "persist",
                        mock_persist(persisted_objects))

    service = RewardService(trading_service, trading_repository)
    service.reward_invitation_with_cash(profile_id, invitation)

    assert InvitationCashReward in persisted_objects
    cash_reward = persisted_objects[InvitationCashReward][0]
    assert cash_reward.invitation_id == invitation_id
    assert cash_reward.profile_id == profile_id
    assert cash_reward.money_flow_id == money_flow_id
