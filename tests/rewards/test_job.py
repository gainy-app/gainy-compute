from gainy.models import Invitation
from gainy.rewards.reward_job import RewardJob
from gainy.rewards.repository import RewardRepository
from gainy.rewards.service import RewardService
from gainy.tests.mocks.repository_mocks import mock_record_calls


def test(monkeypatch):
    profile_id1 = 1
    profile_id2 = 2

    invitation = Invitation()
    invitation.from_profile_id = profile_id1
    invitation.to_profile_id = profile_id2
    invitations = [invitation]

    repo = RewardRepository(None)
    monkeypatch.setattr(repo, "iterate_invitations_to_reward",
                        lambda: invitations)

    service = RewardService(None, None)
    reward_invitation_with_cash_calls = []
    monkeypatch.setattr(service, "reward_invitation_with_cash",
                        mock_record_calls(reward_invitation_with_cash_calls))

    job = RewardJob(repo, service)
    job.run()

    assert {profile_id1, profile_id2
            } == set(args[0]
                     for args, kwargs in reward_invitation_with_cash_calls)
