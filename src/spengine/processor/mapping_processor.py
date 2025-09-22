from spengine.base.mapper import BaseMapper
from spengine.core.context import Context
from spengine.processor.base_processor import BaseProcessor


class MappingProcessor(BaseProcessor):
    mappers: list[BaseMapper]

    # the order of mappers matters
    # context of each mapper will be passed to next mapper in the list
    def __init__(self, mappers: list[BaseMapper]):
        super().__init__()
        self.mappers = mappers

    def process(self, *args, **kwargs):
        if kwargs.get("data") is None:
            raise

        if not isinstance(kwargs.get("data"), dict) and not self._validate_list_of_dict(kwargs.get("data")):
            raise

        mapped = dict()

        ctx = Context.new()
        for mapper in self.mappers:
            temp = kwargs.get("data")

            r = mapper.map(temp, ctx, kwargs.get("additional_info"))

            if mapper.include_in_field != "*":
                mapped[mapper.include_in_field] = r
            else:
                if isinstance(r, list):
                    return r  # return as a list not dictionary
                if r is not None:
                    mapped.update(r)

        return mapped
