from __future__ import annotations

from dataclasses import dataclass
from typing import Union

import jmespath


@dataclass
class Operand:
    value1: Union[int, str, float, bool, list[str]]
    value2: Union[int, str, float, bool, list[str]]


# operation type e.g eq,gt,lt,gte,lte,and,or,in
@dataclass
class Operator:
    _operator: str
    operand: Operand

    def operate(self) -> bool:
        match self._operator:
            case "eq":
                return self.operand.value1 == self.operand.value2
            case "lt":
                return self.operand.value1 < self.operand.value2
            case "lte":
                return self.operand.value1 <= self.operand.value2
            case "gt":
                return self.operand.value1 > self.operand.value2
            case "gte":
                return self.operand.value1 >= self.operand.value2
            case "and":
                return self.operand.value1 and self.operand.value2
            case "or":
                return self.operand.value1 or self.operand.value2
            case "in":
                return self.operand.value1 in self.operand.value2

        return False


@dataclass
class Node:
    _type: str
    left: Node | None
    right: Node | None
    value: Operator | None

    def solve(self) -> bool:
        if self.left is None and self.right is None:
            return self.value.operate()

        left_value = None
        if self.left is not None:
            left_value = self.left.solve()

        right_value = None
        if self.right is not None:
            right_value = self.right.solve()

        self.set_operator(value1=left_value, value2=right_value)

        return self.value.operate()

    def set_operator(
        self,
        value1: Union[int, str, float, bool, list[str]],
        value2: Union[int, str, float, bool, list[str]],
    ):
        self.value = Operator(_operator=self._type, operand=Operand(value1=value1, value2=value2))


@dataclass
class TreeOperator:
    root: Node

    def solve(self) -> bool:
        return self.root.solve()


class TreeBuilder:
    def build(self, config: dict, data: dict | None) -> TreeOperator:
        operator_config = config.get("operator", dict())
        root_node = self._build_node(operator_config, data)
        return TreeOperator(root=root_node)

    def _build_node(self, config: dict, data: dict | None) -> Node:
        op = config.get("op")
        value1 = config.get("value1", dict())
        value2 = config.get("value2", dict())

        if "operator" not in value1 and "operator" not in value2:
            node = Node(op, None, None, None)
            a = None
            b = None
            if "pattern" in value1 and value1.get("pattern") is not None:
                a = jmespath.search(value1["pattern"], data)
            elif "value" in value1 and value1.get("value") is not None:
                a = value1["value"]
            else:
                raise Exception("config for value1 error, provide pattern or value")

            if "pattern" in value2 and value2.get("pattern") is not None:
                b = jmespath.search(value2["pattern"], data)
            elif "value" in value2 and value2.get("value") is not None:
                b = value2["value"]
            else:
                raise Exception("config for value2 error, provide pattern or value")
            node.set_operator(a, b)

            return node

        left_node = None
        right_node = None

        if "operator" in value1:
            left_node = self._build_node(value1["operator"], data)
        else:
            raise Exception("config error, left node is not an operator or concrete value")

        if "operator" in value2:
            right_node = self._build_node(value2["operator"], data)
        else:
            raise Exception("config error, right node is not an operator or concrete value")

        return Node(op, left_node, right_node, None)
