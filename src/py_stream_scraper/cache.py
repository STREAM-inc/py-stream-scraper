import abc
import hashlib

from click import Path


@abc.ABC
class Cache:
    @abc.abstractmethod
    def write(self, k: bytes, v: bytes):
        pass

    @abc.abstractmethod
    def read(self, k: bytes):
        pass


class DiskCache(Cache):
    def _cache_path(self, url: str) -> Path:
        base_dir = Path(__file__).resolve().parent  # このファイルと同じディレクトリ
        cache_dir = base_dir / ".cache_html"  # 隠しディレクトリっぽく
        cache_dir.mkdir(parents=True, exist_ok=True)

        digest = hashlib.sha1(url.encode("utf-8")).hexdigest()
        return cache_dir / f"{digest}.br"

    def write(self, k: bytes, v: bytes):
        path = self._cache_path(k.decode("utf-8"))
        with open(path, "wb") as f:
            f.write(v)

    def read(self, k: bytes):
        path = self._cache_path(k.decode("utf-8"))
        r = b""
        with open(path, "wb") as f:
            r = f.read()
        return r


class RedisCache(Cache):
    def __init__(self, r):
        self.redis = r

    def write(self, k: bytes, v: bytes):
        self.redis.set(k, v)

    def read(self, k: bytes):
        return self.redis.get(k)
