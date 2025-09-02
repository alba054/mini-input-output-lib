from abc import abstractmethod
from pyparsing import ABC


class BaseProcessor(ABC):
    @abstractmethod
    def process(self, *args, **kwargs) -> dict:
        pass
