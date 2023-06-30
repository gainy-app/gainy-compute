import time

from gainy.context_container import ContextContainer
from gainy.rewards.repository import RewardRepository
from gainy.rewards.service import RewardService
from gainy.utils import get_logger

logger = get_logger(__name__)


class RewardJob:

    def __init__(self, repo: RewardRepository, service: RewardService):
        self.repo = repo
        self.service = service

    def run(self):
        for invitation in self.repo.iterate_invitations_to_reward():
            start_time = time.time()

            try:
                self.service.reward_invitation_with_cash(
                    invitation.from_profile_id)
                self.service.reward_invitation_with_cash(
                    invitation.to_profile_id)
                logger.info(
                    "Cash reward to profiles in the invitation %d in %f",
                    invitation.id,
                    time.time() - start_time)
            except Exception as e:
                logger.exception(e)


def cli():
    try:
        with ContextContainer() as context_container:
            job = RewardJob(context_container.promotion_repository,
                            context_container.reward_service)
            job.run()

    except Exception as e:
        logger.exception(e)
        raise e
