from dataclasses import dataclass
from enum import Enum


class Operator(Enum):
    EQUALS = 'EQUALS'

    def __str__(self):
        return self.value

    def __repr__(self):
        return f'Operator.{self.value}'

    @staticmethod
    def from_string(op: str) -> 'Operator':
        return _OPERATOR_LOOKUP[op.lower()]


_OPERATOR_LOOKUP = {
    'equals': Operator.EQUALS
}


class Constraint:
    def to_list(self) -> list:
        raise NotImplementedError("stub")


@dataclass(frozen=True)
class OneToOneConstraint(Constraint):
    operator: Operator
    attribute: str
    value: str

    def __hash__(self):
        return hash((self.operator, self.attribute, self.value))

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, self.__class__):
            return False
        return (self.operator == other.operator and
                self.attribute == other.attribute and
                self.value == other.value)

    def to_list(self) -> list:
        return [
            self.attribute,
            str(self.operator),
            self.value
        ]

    @classmethod
    def from_list(cls, ls: list) -> 'OneToOneConstraint':
        cls(*ls)


def build_equals_constraint(attr: str, value: str) -> Constraint:
    return OneToOneConstraint(Operator.EQUALS, attr, value)


def parse_from(constraint: list) -> Constraint:
    op, attr, val = constraint
    op = Operator.from_string(op)
    if op == Operator.EQUALS:
        return OneToOneConstraint(op, attr, val)
    else:
        raise NotImplementedError(f"Operator {op} is not supported.")
