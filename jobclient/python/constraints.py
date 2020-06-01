# Copyright (c) Two Sigma Open Source, LLC
#
# Licensed under the Apache license, Version 2.0 (the "License");
# you may not use this file ecept in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from enum import Enum


class Operator(Enum):
    """Operator identifier for a constraint."""
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
    """Interface for constraints."""
    def to_list(self) -> list:
        raise NotImplementedError("stub")


class OneToOneConstraint(Constraint):
    """A constraint specifying that some attribute must equal some value."""
    operator: Operator
    attribute: str
    value: str

    def __init__(self, operator: Operator, attribute: str, value: str):
        """Initializes a one-to-one constraint."""
        self.operator = operator
        self.attribute = attribute
        self.value = value

    def to_list(self) -> list:
        """Generate this constraint's `list` representation."""
        return [
            self.attribute,
            str(self.operator),
            self.value
        ]

    @classmethod
    def from_list(cls, ls: list) -> 'OneToOneConstraint':
        """Create a `OneToOneConstraint` from its `list` represnetation."""
        cls(*ls)


def build_equals_constraint(attr: str, value: str) -> Constraint:
    """Create a one-to-one constraint."""
    return OneToOneConstraint(Operator.EQUALS, attr, value)


def parse_from(constraint: list) -> Constraint:
    """Parse a sequence of constraints from a list."""
    op, attr, val = constraint
    op = Operator.from_string(op)
    if op == Operator.EQUALS:
        return OneToOneConstraint(op, attr, val)
    else:
        raise NotImplementedError(f"Operator {op} is not supported.")
