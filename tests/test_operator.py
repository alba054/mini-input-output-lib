import unittest
from unittest.mock import patch

from spengine.core.operator import Operand, Operator, Node, TreeOperator, TreeBuilder


class TestOperatorAndTree(unittest.TestCase):
    def test_operator_basic_comparisons_and_logic(self):
        # eq
        self.assertTrue(Operator("eq", Operand(1, 1)).operate())
        self.assertFalse(Operator("eq", Operand(1, 2)).operate())

        # lt/lte
        self.assertTrue(Operator("lt", Operand(1, 2)).operate())
        self.assertFalse(Operator("lt", Operand(2, 1)).operate())
        self.assertTrue(Operator("lte", Operand(2, 2)).operate())
        self.assertTrue(Operator("lte", Operand(1, 2)).operate())
        self.assertFalse(Operator("lte", Operand(3, 2)).operate())

        # gt/gte
        self.assertTrue(Operator("gt", Operand(2, 1)).operate())
        self.assertFalse(Operator("gt", Operand(1, 2)).operate())
        self.assertTrue(Operator("gte", Operand(2, 2)).operate())
        self.assertTrue(Operator("gte", Operand(3, 2)).operate())
        self.assertFalse(Operator("gte", Operand(1, 2)).operate())

        # and/or
        self.assertTrue(Operator("and", Operand(True, True)).operate())
        self.assertFalse(Operator("and", Operand(True, False)).operate())
        self.assertTrue(Operator("or", Operand(True, False)).operate())
        self.assertFalse(Operator("or", Operand(False, False)).operate())

        # in
        self.assertTrue(Operator("in", Operand("a", ["a", "b"])).operate())
        self.assertFalse(Operator("in", Operand("c", ["a", "b"])).operate())

    def test_node_leaf_solve_uses_operator(self):
        op = Operator("eq", Operand(1, 1))
        node = Node("eq", None, None, op)
        self.assertTrue(node.solve())

    def test_treebuilder_leaf_with_direct_values(self):
        config = {
            "operator": {
                "op": "eq",
                "value1": {"value": 3},
                "value2": {"value": 3},
            }
        }
        tree = TreeBuilder().build(config, data=None)
        self.assertTrue(tree.solve())

    def test_treebuilder_nested_and_or(self):
        # (1 == 1) and ((0 < 1) or (2 > 1)) -> True and (True or True) -> True
        config = {
            "operator": {
                "op": "and",
                "value1": {
                    "operator": {
                        "op": "eq",
                        "value1": {"value": 1},
                        "value2": {"value": 1},
                    }
                },
                "value2": {
                    "operator": {
                        "op": "or",
                        "value1": {
                            "operator": {
                                "op": "lt",
                                "value1": {"value": 0},
                                "value2": {"value": 1},
                            }
                        },
                        "value2": {
                            "operator": {
                                "op": "gt",
                                "value1": {"value": 2},
                                "value2": {"value": 1},
                            }
                        },
                    }
                },
            }
        }
        tree = TreeBuilder().build(config, data=None)
        self.assertTrue(tree.solve())

    @patch("spengine.core.operator.jmespath.search")
    def test_treebuilder_uses_jmespath_patterns(self, mock_search):
        # Simulate extracting values from data using jmespath
        mock_search.side_effect = ["x", "x"]
        data = {"a": "x", "b": "x"}
        config = {
            "operator": {
                "op": "eq",
                "value1": {"pattern": "a"},
                "value2": {"pattern": "b"},
            }
        }
        tree = TreeBuilder().build(config, data=data)
        self.assertTrue(tree.solve())
        self.assertEqual(mock_search.call_count, 2)
        mock_search.assert_any_call("a", data)
        mock_search.assert_any_call("b", data)

    def test_treebuilder_error_missing_value1_config(self):
        # Leaf node without pattern or value for value1 should error
        config = {
            "operator": {
                "op": "eq",
                "value1": {},
                "value2": {"value": 1},
            }
        }
        with self.assertRaises(Exception) as ctx:
            TreeBuilder().build(config, data=None)
        self.assertIn("config for value1 error", str(ctx.exception))

    def test_treebuilder_error_when_left_is_not_operator_on_nested(self):
        # Nested node requires both left and right to be operator blocks
        config = {
            "operator": {
                "op": "and",
                "value1": {"value": 1},  # not an operator block
                "value2": {
                    "operator": {
                        "op": "eq",
                        "value1": {"value": 1},
                        "value2": {"value": 1},
                    }
                },
            }
        }
        with self.assertRaises(Exception) as ctx:
            TreeBuilder().build(config, data=None)
        self.assertIn("left node is not an operator", str(ctx.exception))

    def test_treeoperator_delegates_to_root(self):
        op = Operator("eq", Operand(True, True))
        node = Node("eq", None, None, op)
        tree = TreeOperator(root=node)
        self.assertTrue(tree.solve())


if __name__ == "__main__":
    unittest.main()
