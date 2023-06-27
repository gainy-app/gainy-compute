from __future__ import annotations

from _operator import itemgetter

from typing import Iterable

from gainy.data_access.operators import OperatorIn
from gainy.data_access.repository import Repository
from gainy.models import Invitation
from gainy.utils import get_logger

logger = get_logger(__name__)


class RewardRepository(Repository):

    def iterate_invitations_to_reward(self) -> Iterable[Invitation]:
        query = """
            select invitation_id 
            from invitation_history 
            where step1_signed_up
              and step2_brokerate_account_open 
              and step3_deposited_enough 
              and not is_payment_started"""

        with self.db_conn.cursor() as cursor:
            cursor.execute(query)
            invitation_ids = list(map(itemgetter(0), cursor.fetchall()))

        yield from self.iterate_all(Invitation,
                                    {"id": OperatorIn(invitation_ids)})
