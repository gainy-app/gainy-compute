import datetime
import json
from typing import Optional

from gainy.data_access.db_lock import ResourceType
from gainy.data_access.models import BaseModel, classproperty, ResourceVersion


class QueueMessage(BaseModel, ResourceVersion):
    id: int = None
    ref_id: str = None
    source_ref_id: str = None
    source_event_ref_id: Optional[str] = None
    body = None
    data = None
    handled: bool = None
    created_at: datetime.datetime = None
    updated_at: datetime.datetime = None
    version: int = 0

    key_fields = ["id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def __init__(self):
        self.handled = False

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "queue_messages"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "body": json.dumps(self.body),
            "data": json.dumps(self.data),
        }

    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.QUEUE_MESSAGE

    @property
    def resource_id(self) -> int:
        return self.id

    @property
    def resource_version(self):
        return self.version

    def update_version(self):
        self.version = self.version + 1 if self.version else 1
