from abc import abstractmethod
from typing import Dict, Any, Tuple

from psycopg2 import sql


class OperatorInterface:

    @abstractmethod
    def to_sql(self, field_name: str) -> Tuple[sql.Composable, Dict[str, Any]]:
        pass


class Operator(OperatorInterface):

    def __init__(self, op, param):
        self.op = op
        self.param = param

    def to_sql(self, field_name: str) -> Tuple[sql.Composable, Dict[str, Any]]:
        _sql = sql.SQL(f"{{field_name}} {self.op} %({field_name})s").format(
            field_name=sql.Identifier(field_name))
        _params = {field_name: self.param}

        return _sql, _params


class OperatorEq(Operator):

    def __init__(self, param):
        super().__init__("=", param)


class OperatorLt(Operator):

    def __init__(self, param):
        super().__init__("<", param)


class OperatorGt(Operator):

    def __init__(self, param):
        super().__init__("<", param)


class OperatorIn(Operator):

    def __init__(self, param):
        super().__init__(None, param)

    def to_sql(self, field_name: str) -> Tuple[sql.Composable, Dict[str, Any]]:
        _sql = sql.SQL(f"{{field_name}} = ANY (%({field_name})s)").format(
            field_name=sql.Identifier(field_name))
        _params = {field_name: self.param}

        return _sql, _params


class OperatorNot(OperatorInterface):

    def __init__(self, operator: OperatorInterface):
        self.operator = operator

    def to_sql(self, field_name: str) -> Tuple[sql.Composable, Dict[str, Any]]:
        _sql, _params = self.operator.to_sql(field_name)
        _sql = sql.SQL(f"(NOT ({{_sql}}))").format(_sql=_sql)

        return _sql, _params
