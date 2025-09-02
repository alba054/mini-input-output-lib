from contextlib import suppress
from spengine.base.observer import BaseObserver
from spengine.base.subject import BaseSubject
from spengine.processor.base_processor import BaseProcessor


class DataSourceSubject(BaseSubject):
    data: dict
    processors: list[BaseProcessor]
    additional_info: dict = None

    def __init__(self, processors: list[BaseProcessor]) -> None:
        self._service_observers: list[BaseObserver] = []
        self.processors = processors

    def attach(self, observer: BaseObserver) -> None:
        if observer not in self._service_observers:
            self._service_observers.append(observer)

    def detach(self, observer: BaseObserver) -> None:
        with suppress(ValueError):
            self._service_observers.remove(observer)

    def notify(self):
        for observer in self._service_observers:
            observer.update(self)
