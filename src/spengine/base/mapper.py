from abc import ABC, abstractmethod

from spengine.core.context import Context


class BaseMapper(ABC):
    include_in_field: str

    def __init__(self, include_in_field: str):
        super().__init__()
        self.context = []
        self.include_in_field = include_in_field

    @abstractmethod
    def map(self, data: dict | list[dict], context: Context, additional_info: dict = None) -> list[dict] | dict:
        pass
