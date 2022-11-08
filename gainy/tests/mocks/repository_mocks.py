from gainy.data_access.models import BaseModel


def mock_find(options):

    def mock(_cls, _fltr=None):
        for cls, fltr, result in options:
            if cls == _cls and fltr == _fltr:
                return result

        raise Exception(f"unknown find_one call: {_cls}, {_fltr}")

    return mock


def mock_persist(persisted_objects: dict = None):

    def mock(entities):
        if persisted_objects is None:
            return

        if isinstance(entities, BaseModel):
            entities = [entities]

        for entity in entities:
            if entity.__class__ not in persisted_objects:
                persisted_objects[entity.__class__] = []

            persisted_objects[entity.__class__].append(entity)

    return mock


def mock_noop(*args, **kwargs):
    pass


def mock_record_calls(calls: list = None):

    def mock(*args, **kwargs):
        if calls is None:
            return

        calls.append((args, kwargs))

    return mock