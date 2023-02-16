from analytics.interfaces import AnalyticsSinkInterface, AttributionSourceInterface
from gainy.utils import get_logger

logger = get_logger(__name__)


class AnalyticsService:
    def __init__(self, attribution_sources: list[AttributionSourceInterface], sinks: list[AnalyticsSinkInterface]):
        self.attribution_sources = attribution_sources
        self.sinks = sinks

    def sync_profile_attribution(self, profile_id):
        attributes = {}
        for source in self.attribution_sources:
            attributes.update(source.get_attributes(profile_id))

        for sink in self.sinks:
            sink.update_profile_attribution(profile_id, attributes)
