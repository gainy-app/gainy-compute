from math import trunc
from typing import Iterable, List, Any

import numpy as np


def batch_iter(ary, batch_size: int = 100) -> Iterable[List[Any]]:
    n_chunks = trunc(len(ary) / batch_size) + 1
    return np.array_split(ary, n_chunks)
