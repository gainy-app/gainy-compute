from abc import ABC, abstractmethod
from typing import List, Any, Dict


class BaseModel(ABC):

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