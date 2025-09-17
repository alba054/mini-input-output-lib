from loguru import logger
from spengine.core.operator import TreeBuilder
from spengine.processor.base_processor import BaseProcessor


class FilterProcessor(BaseProcessor):
    def __init__(self, operator_config: dict):
        super().__init__()
        self.tree_config = operator_config

    def process(self, *args, **kwargs) -> dict | list[dict] | None:
        if kwargs.get("data") is None:
            raise

        if not isinstance(kwargs.get("data"), dict) and not self._validate_list_of_dict(kwargs.get("data")):
            raise

        if isinstance(kwargs.get("data"), dict):
            tree = TreeBuilder().build(self.tree_config, kwargs.get("data"))

            if not tree.solve():
                return None

            logger.info("found")
            return kwargs.get("data")
        else:
            filtered = []
            for d in kwargs.get("data"):
                tree = TreeBuilder().build(self.tree_config, d)

                if tree.solve():
                    logger.info("found")
                    filtered.append(d)

            return filtered
