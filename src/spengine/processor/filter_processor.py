from loguru import logger
from spengine.core.operator import TreeBuilder
from spengine.processor.base_processor import BaseProcessor


class FilterProcessor(BaseProcessor):
    def __init__(self, operator_config: dict):
        super().__init__()
        self.tree_config = operator_config

    def process(self, *args, **kwargs):
        if kwargs.get("data") is None:
            raise

        if not isinstance(kwargs.get("data"), dict):
            raise

        tree = TreeBuilder().build(self.tree_config, kwargs.get("data"))

        if not tree.solve():
            return None

        logger.info("found")
        # time.sleep(5)
        return kwargs.get("data")
