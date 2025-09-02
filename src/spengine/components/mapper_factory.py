from spengine.base.mapper import BaseMapper
from spengine.util.call_function_from_string import call_function_from_string


class MapperConfigFactory:
    @staticmethod
    def build(config: list[dict]) -> list[BaseMapper]:
        mappers: BaseMapper = []

        for cfg in config:
            create_mapper = call_function_from_string(cfg["object"])
            include_in_field = cfg["includeInField"]

            mapper = create_mapper(include_in_field)
            mappers.append(mapper)

        return mappers
