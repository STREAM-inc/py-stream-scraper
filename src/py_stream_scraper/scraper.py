import datetime
import re
import random
from typing import Callable, Iterable, List, Optional
from urllib.parse import urlparse
import redis
from usp.tree import sitemap_tree_for_homepage

import re
from typing import Callable, Iterable, List, Optional, Pattern, Union

from .sink import Sink
from .url_manager import DiskURLManager


def _random_user_agent():
    def lerp(a1, b1, a2, b2, n):
        return (n - a1) / (b1 - a1) * (b2 - a2) + a2

    version = int(
        lerp(
            datetime.date(2023, 3, 7).toordinal(),
            datetime.date(2030, 9, 24).toordinal(),
            111,
            200,
            datetime.date.today().toordinal(),
        )
    )
    version += random.randint(-5, 1)
    version = max(version, 101)
    return f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{version}.0.0.0 Safari/537.36"


_DEFAULT_USER_AGENT = _random_user_agent()


class Scraper:
    def __init__(self, host, qps, url_filter, redis_client=None):
        self.host = host
        self.qps = qps
        self.url_filter = url_filter
        self.redis = redis_client or redis.Redis(
            host="localhost", port=6379, decode_responses=True
        )
        self.stream_name = f"stream-scraper:scrape:{self.host}"
        self.url_manager = DiskURLManager(host)

    def discover_urls(self):
        index_url = f"https://{self.host}/"
        tree = sitemap_tree_for_homepage(index_url)
        for page in tree.all_pages():
            if self._path_allowed(page.url):
                self.url_manager.add_url(page.url)

    def _path_allowed(self, url):
        path = urlparse(url).path or "/"
        return any(rx.search(path) for rx in self.url_filter)

    def scrape(self):
        if self.url_manager.get_cursor() == self.url_manager.upper:
            self.url_manager.set_cursor()
        for key, url in self.url_manager.to_iter(self.url_manager.get_cursor()):
            self.url_manager.set_cursor(key)
            print(url)
        self.url_manager.set_cursor()


FilterInput = Union[str, Pattern, Iterable[Union[str, Pattern]]]


class ScraperBuilder:
    def __init__(self):
        self._host: Optional[str] = None
        self._qps: Optional[float] = None
        self._filters: List[Pattern] = []
        self._parser: Optional[Callable] = None
        self._redis_client: Optional[redis.Redis] = None
        self._sink: Optional[Sink] = None

    def set_host(self, host: str) -> "ScraperBuilder":
        if not host or not isinstance(host, str):
            raise ValueError("host must be a non-empty string")
        self._host = host.strip()
        return self

    def set_qps(self, qps: Union[int, float]) -> "ScraperBuilder":
        try:
            qps_val = float(qps)
        except Exception as e:
            raise ValueError("qps must be a number") from e
        if qps_val <= 0:
            raise ValueError("qps must be > 0")
        self._qps = qps_val
        return self

    def set_filter(self, filt: FilterInput, flags: int = 0) -> "ScraperBuilder":
        """
        filt に渡せるもの:
          - r'^/(blog|news)/' のような文字列
          - re.compile(...) 済みの Pattern
          - 上記の反復可能（list/tuple など）
        すべて build() 時点で Pattern に統一します（ここで渡した分は即時追加）。
        """
        compiled = self._coerce_filters(filt, flags)
        self._filters.extend(compiled)
        return self

    def set_parser(self, parser: Callable) -> "ScraperBuilder":
        """
        解析関数（例: def parse(html)->dict）。build 後に scraper.parser として生やします。
        """
        if not callable(parser):
            raise ValueError("parser must be callable")
        self._parser = parser
        return self

    def set_redis_client(self, client: redis.Redis) -> "ScraperBuilder":
        self._redis_client = client
        return self

    def set_sink(self, sink: Sink) -> "ScraperBuilder":
        """
        データの保存先を設定する

        Args:
            sink: Sinkインスタンス（例: FileSink, ConsoleSinkなど）

        Returns:
            self（メソッドチェーン用）
        """
        if not isinstance(sink, Sink):
            raise ValueError("sink must be an instance of Sink")
        self._sink = sink
        return self

    def build(self) -> Scraper:
        if not self._host:
            raise ValueError("host is required. Call set_host().")
        if self._qps is None:
            raise ValueError("qps is required. Call set_qps().")
        if not self._filters:
            # フィルタ未設定なら全許可の安全なデフォルトは避け、明示エラーにします
            raise ValueError("at least one filter is required. Call set_filter().")

        # すでに Pattern 化済みだが、念のため Pattern のみを渡す
        url_filters: List[Pattern] = list(self._filters)

        scraper = Scraper(
            host=self._host,
            qps=self._qps,
            url_filter=url_filters,
            redis_client=self._redis_client,
        )
        if self._parser:
            # 既存クラスに影響を与えないよう、属性として追加
            setattr(scraper, "parser", self._parser)
        if self._sink:
            # Sinkも属性として追加
            setattr(scraper, "sink", self._sink)
        return scraper

    # --- helpers ---
    def _coerce_filters(self, filt: FilterInput, flags: int = 0) -> List[Pattern]:
        def compile_one(x) -> Pattern:
            if isinstance(x, str):
                return re.compile(x, flags)
            if hasattr(x, "pattern") and hasattr(x, "search"):
                # すでに Pattern（re.Pattern 互換）
                return x  # type: ignore
            raise ValueError(f"Unsupported filter type: {type(x)}")

        if isinstance(filt, (str, re.Pattern)):
            return [compile_one(filt)]
        try:
            return [compile_one(x) for x in filt]  # type: ignore
        except TypeError as e:
            # filt が反復可能でない場合にここに来る
            raise ValueError(
                "filt must be a string, Pattern, or an iterable of them"
            ) from e
