from abc import ABC, abstractmethod


class BaseProcessor(ABC):
    @abstractmethod
    def process(self, *args, **kwargs) -> dict:
        pass

    def _validate_list_of_dict(self, data: list) -> bool:
        if not isinstance(data, list):
            return False

        for d in data:
            if not isinstance(d, dict):
                return False

        return True
