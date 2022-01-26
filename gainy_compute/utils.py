from math import trunc
from typing import Iterable, List, Any

import numpy as np
import logging


def batch_iter(ary, batch_size: int = 100) -> Iterable[List[Any]]:
    n_chunks = trunc(len(ary) / batch_size) + 1
    return np.array_split(ary, n_chunks)


def current_version() -> str:
    import importlib

    try:
        return importlib.metadata.version("gainy-compute")
    except importlib.metadata.PackageNotFoundError as e:
        logging.info(f"Package not found: {str(e)}")
        return "local"


def env() -> str:
    import os
    return os.environ.get("ENV", "local")
