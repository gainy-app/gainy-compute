import re
from abc import ABC
from mlflow.pyfunc import PythonModel


class IndustryAssignmentModel(ABC, PythonModel):

    def fit(self, X, y):
        pass

    def predict(self,
                descriptions,
                n: int = 2,
                include_distances: bool = False):
        pass

    def name(self):
        return re.sub(
            r"(?<!^)(?=[A-Z])", "_",
            self.__class__.__name__).lower()  # class name in snake case

    def description(self) -> str:
        pass
