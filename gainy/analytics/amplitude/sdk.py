from typing import Iterable

from amplitude import Amplitude
from amplitude.constants import FLUSH_QUEUE_SIZE
from amplitude.plugin import AmplitudeDestinationPlugin, Plugin
from amplitude.worker import Workers

from gainy.utils import batch_iter


class AmplitudeSequentialWorkers(Workers):

    def flush(self):
        events = self.storage.pull_all()
        if not events:
            return

        for batch in batch_iter(events, FLUSH_QUEUE_SIZE):
            self.send(batch)


class AmplitudeSequentialDestinationPlugin(AmplitudeDestinationPlugin):

    def __init__(self):
        super().__init__()
        self.workers = AmplitudeSequentialWorkers()


class AmplitudeClient(Amplitude):

    @property
    def plugins(self) -> Iterable[Plugin]:
        for plugins in self._Amplitude__timeline.plugins.values():
            yield from plugins
