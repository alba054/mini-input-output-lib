from spengine.components.mapper_factory import MapperConfigFactory
from spengine.processor.base_processor import BaseProcessor
from spengine.processor.filter_processor import FilterProcessor
from spengine.processor.mapping_processor import MappingProcessor


class FilterProcessorFactory:
    @staticmethod
    def build(config: dict) -> FilterProcessor:
        return FilterProcessor(config["operators"])


class MappingProcessorFactory:
    @staticmethod
    def build(config: dict) -> MappingProcessor:
        mappers = MapperConfigFactory.build(config["mappers"])
        return MappingProcessor(mappers)


class ProcessorFactory:
    @staticmethod
    def build(config: dict) -> BaseProcessor:
        if config["type"] == "filter":
            return FilterProcessorFactory.build(config["components"])
        elif config["type"] == "mapper":
            return MappingProcessorFactory.build(config["components"])
