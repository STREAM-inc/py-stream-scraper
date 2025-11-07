from abc import ABC, abstractmethod


class URLManager(ABC):
    @abstractmethod
    def add_url(self, url: str):
        pass

    @abstractmethod
    def to_iter(self, start_at: str):
        pass
