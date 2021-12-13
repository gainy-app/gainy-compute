from abc import ABC


class IndustryAssignmentModel(ABC):

    def fit(self, X, y):
        pass

    def classify(self, descriptions, n: int = 2, include_distances: bool = False):
        pass
