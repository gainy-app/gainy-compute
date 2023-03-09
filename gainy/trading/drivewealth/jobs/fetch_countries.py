from gainy.context_container import ContextContainer
from gainy.data_access.operators import OperatorIn, OperatorNot
from gainy.trading.drivewealth import DriveWealthRepository, DriveWealthApi
from gainy.trading.drivewealth.models import DriveWealthCountry
from gainy.utils import get_logger

logger = get_logger(__name__)


def _hydrate_entity(data):
    entity = DriveWealthCountry()
    entity.set_from_response(data)
    return entity


class FetchCountriesJob:

    def __init__(self, repo: DriveWealthRepository, api: DriveWealthApi):
        self.repo = repo
        self.api = api

    def run(self):
        countries = self.api.get_countries(status="active")
        entities = [_hydrate_entity(i) for i in countries]
        self.repo.persist(entities)
        country_ref_ids = [i.ref_id for i in entities]
        self.repo.delete_by(
            DriveWealthCountry,
            {"ref_id": OperatorNot(OperatorIn(country_ref_ids))})


def cli():
    try:
        with ContextContainer() as context_container:
            job = FetchCountriesJob(context_container.drivewealth_repository,
                                    context_container.drivewealth_api)
            job.run()

    except Exception as e:
        logger.exception(e)
        raise e
