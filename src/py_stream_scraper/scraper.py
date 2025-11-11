import datetime
import re
import random
import time
from typing import Callable, Iterable, List, Optional
from urllib.parse import urlparse
import redis
from usp.tree import sitemap_tree_for_homepage

import re
from typing import Callable, Iterable, List, Optional, Pattern, Union

from .sink import Sink
from .url_manager import DiskURLManager
from .rate_limiter import Limiter, MemoryStorage


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
    def __init__(self, host, qps, redis_client=None):
        self.host = host
        self.qps = qps
        self.redis = redis_client or redis.Redis(
            host="localhost", port=6379, decode_responses=True
        )
        self.stream_name = f"stream-scraper:scrape:{self.host}"
        self.url_manager = DiskURLManager(host)
        self.limiter = Limiter(self.qps, MemoryStorage())

    def discover_urls(self):
        pass
        # index_url = f"https://{self.host}/"
        # tree = sitemap_tree_for_homepage(index_url)
        # for page in tree.all_pages():
        #     if self._path_allowed(page.url):
        #         self.url_manager.add_url(page.url)

    def _path_allowed(self, url):
        path = urlparse(url).path or "/"
        return any(rx.search(path) for rx in self.url_filter)

    def scrape(self):
        if self.url_manager.get_cursor() == self.url_manager.upper:
            self.url_manager.set_cursor()
        for key, url in self.url_manager.to_iter(self.url_manager.get_cursor()):
            while not self.limiter.consume(self.host):
                time.sleep(0.01)
            self.url_manager.set_cursor(key)
        self.url_manager.set_cursor()
