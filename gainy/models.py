from gainy.data_access.db_lock import ResourceType
from gainy.data_access.models import BaseModel, classproperty, ResourceVersion


class Profile(BaseModel):
    id = None
    email = None
    first_name = None
    last_name = None
    gender = None
    user_id = None
    avatar_url = None
    legal_address = None
    subscription_end_date = None
    created_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "profiles"


class AbstractEntityLock(BaseModel, ResourceVersion):
    id = None
    class_name = None
    object_id = None
    version = None

    key_fields = ["class_name", "object_id"]

    db_excluded_fields = []
    non_persistent_fields = ["id", "version"]

    def __init__(self, cls: type = None, object_id=None):
        if cls:
            self.class_name = cls.__qualname__
        if object_id:
            self.object_id = str(object_id)

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "abstract_entity_lock"

    @property
    def resource_type(self) -> ResourceType:
        return ResourceType.ABSTRACT_ENTITY_LOCK

    @property
    def resource_id(self) -> int:
        return self.id

    @property
    def resource_version(self):
        return self.version

    def update_version(self):
        self.version += 1


class Invitation(BaseModel):
    id = None
    from_profile_id = None
    to_profile_id = None
    created_at = None

    key_fields = ["id"]

    db_excluded_fields = ["created_at"]
    non_persistent_fields = ["id", "created_at"]

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "invitations"
