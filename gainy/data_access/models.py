from abc import ABC, abstractmethod
from typing import List, Any, Dict
from gainy.data_access.db_lock import ResourceType


class classproperty(property):

    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()


class BaseModel(ABC):

    def __init__(self, row: dict = None):
        if row:
            self.set_from_dict(row)

    def set_from_dict(self, row: dict = None):
        if not row:
            return

        for field, value in row.items():
            if hasattr(self, field):
                setattr(self, field, value)

    def to_dict(self) -> Dict[str, Any]:
        return self.__dict__

    @property
    @abstractmethod
    def schema_name(self) -> str:
        pass

    @property
    @abstractmethod
    def table_name(self) -> str:
        pass

    @property
    def db_excluded_fields(self) -> List[str]:
        return []

    @property
    def non_persistent_fields(self) -> List[str]:
        """Typically, auto-generated fields like `id` or `created_at`"""
        return []

    @property
    @abstractmethod
    def key_fields(self) -> List[str]:
        pass


class ResourceVersion(ABC):

    @property
    @abstractmethod
    def resource_type(self) -> ResourceType:
        pass

    @property
    @abstractmethod
    def resource_id(self) -> int:
        pass

    @property
    @abstractmethod
    def resource_version(self):
        pass

    @abstractmethod
    def update_version(self):
        pass
