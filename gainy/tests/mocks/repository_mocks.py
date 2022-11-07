def mock_find(options):

    def mock(_cls, _fltr=None):
        for cls, fltr, result in options:
            if cls == _cls and fltr == _fltr:
                return result

        raise Exception(f"unknown find_one call: {_cls}, {_fltr}")

    return mock


def mock_persist(persisted_objects: dict = None):

    def mock(_entity):
        if persisted_objects is None:
            return

        if _entity.__class__ not in persisted_objects:
            persisted_objects[_entity.__class__] = []

        persisted_objects[_entity.__class__].append(_entity)

    return mock


def mock_noop(*args, **kwargs):
    pass


def mock_record_calls(calls: list = None):

    def mock(*args, **kwargs):
        if calls is None:
            return

        calls.append((args, kwargs))

    return mock