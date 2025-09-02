from abc import ABC, abstractmethod

from spengine.base.subject import BaseSubject


class BaseObserver(ABC):
    @abstractmethod
    def update(self, subject: BaseSubject):
        pass
