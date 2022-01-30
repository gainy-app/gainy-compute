import numpy as np
from numpy.linalg import norm


class DimVector:
    """
    DimVector is a wrapper of `numpy` array, which stores names of dimensions together with values.
    This implementation is useful to deal with "bag-of-words" model - that is how we typically deal
    with categories and industries.

    All mathematical operations (e.g. norm, cosine similarity) are performed against the underlying
     `numpy` vector.

    Alternatives considered:
    - https://xarray.pydata.org/ - can be useful in the future, but now is an overkill (as it requires pandas)
    - https://numpy.org/doc/stable/user/basics.rec.html - numpy structured arrays are differenet and can't be
    a direct replacement to this class
    - https://github.com/wagdav/dimarray (and https://github.com/perrette/dimarray) - custom implementations of
    similar data structure with wider functionality. Unfortunately, booth are outdated.
    """

    def __init__(self, name, coordinates):
        self.name = name
        if coordinates:
            self._coordinates = dict(coordinates)
            self.dims = list(self._coordinates.keys())
            self.values = np.array(list(self._coordinates.values()))
        else:
            self._coordinates = {}
            self.dims = []
            self.values = np.array([])

    @staticmethod
    def norm(vector, order=2):
        return norm(vector.values, ord=order)

    @staticmethod
    def dot_product(first, second):
        result = 0.0
        for dim in set(first.dims).intersection(second.dims):
            result += first._coordinates.get(dim, 0) * second._coordinates.get(
                dim, 0)

        return result

    def cosine_similarity(self, other, norm_order=2):
        self_norm = DimVector.norm(self, order=norm_order)
        other_norm = DimVector.norm(other, order=norm_order)

        if self_norm == 0.0 or other_norm == 0.0:
            return 0.0

        return DimVector.dot_product(self, other) / self_norm / other_norm
