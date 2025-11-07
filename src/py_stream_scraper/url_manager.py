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
        self.db.set(self.upper, b"")
        self.db.set(self.lower, b"")

    def add_url(self, url: str, meta: bytes = b""):
        _, _, path, query = DiskURLManager.normalize_url(url)

        k = self.key_for(path, query)
        v = url.encode("utf-8")
        self.db.set(k, v)

    def to_iter(self, start_url: str | None = None):
        if start_url is None:
            iter = self.db.iterator(mode="from", key=self.lower)
        else:
            _, _, path, query = DiskURLManager.normalize_url(start_url)
            iter = self.db.iterator(mode="from", key=self.key_for(path, query))
        for key, value in iter:
            if key == self.lower:
                continue
            elif key == self.upper:
                break
            host = key.decode("utf-8").split("\x00")[0]
            if host != self.host:
                break
            yield value

    @classmethod
    def normalize_url(cls, u: str):
        p = urlparse(u.strip())
        scheme = (p.scheme or "http").lower()
        host = (p.hostname or "").rstrip(".").lower()
        host = host.encode("idna").decode("ascii")
        path = p.path or "/"
        query = p.query
        return scheme, host, path, query

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
