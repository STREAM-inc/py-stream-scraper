from abc import ABC, abstractmethod
import hashlib
from urllib.parse import urlparse
import rocksdbpy


class URLManager(ABC):
    @abstractmethod
    def add_url(self, url: str):
        pass

    @abstractmethod
    def to_iter(self, start_at: str):
        pass


class DiskURLManager(URLManager):
    def __init__(self, host):
        super().__init__()

        self.db = rocksdbpy.open_default("./.rocksdb")
        self.host = host

        self.upper = f"{host}\x01".encode("utf-8")
        self.lower = f"{host}\x00".encode("utf-8")
        self.cursor = f"{self.host}:cursor".encode("utf-8")
        self.db.set(self.upper, b"")
        self.db.set(self.lower, b"")

        self._num_url = None

    def add_url(self, url: str):
        path, query = DiskURLManager.normalize_url(url)

        k = self.key_for(path, query)
        v = url.encode("utf-8")
        self.db.set(k, v)

    def delete_url(self, url: str):
        path, query = DiskURLManager.normalize_url(url)
        
        k = self.key_for(path, query)
        self.db.delete(k)

    def to_iter(self, start_key: bytes | None = None):
        if start_key is None:
            start_key = self.lower
        iter = self.db.iterator(mode="from", key=start_key)
        for key, value in iter:
            if key == self.lower:
                continue
            elif key == self.upper:
                break
            host = key.decode("utf-8").split("\x00")[0]
            if host != self.host:
                break
            yield key, value

    def set_cursor(self, key: bytes | None = None):
        if key is None:
            self.db.set(self.cursor, self.lower)
        else:
            self.db.set(self.cursor, key)

    def get_cursor(self) -> bytes:
        return self.db.get(self.cursor)

    @classmethod
    def normalize_url(cls, u: str):
        p = urlparse(u.strip())
        path = p.path or "/"
        query = p.query
        return path, query

    def key_for(self, path: str, query: str, hash_tail: bool = False):
        prefix = f"{self.host}\x00".encode("utf-8")
        if hash_tail:
            h = hashlib.sha256(
                (path + ("?" + query if query else "")).encode("utf-8")
            ).digest()
            return prefix + h
        else:
            tail = (path + (("?" + query) if query else "")).encode("utf-8")
            return prefix + tail

    @property
    def urls_total(self):
        num = 0
        for key, _ in self.to_iter():
            if key == self.lower:
                continue
            elif key == self.upper:
                break
            num += 1

        return num

    @property
    def url_current_index(self):
        num = 0
        for key, _ in self.to_iter():
            if key == self.lower:
                continue
            elif key == self.upper:
                break
            num += 1
            if key == self.get_cursor():
                break

        return num
