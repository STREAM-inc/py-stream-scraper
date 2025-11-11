import datetime
import re
import random
import time
from typing import Callable, Iterable, List, Optional
from urllib.parse import urlparse
import requests
import redis
from usp.tree import sitemap_tree_for_homepage
from tqdm import tqdm

import re
from typing import Callable, Iterable, List, Optional, Pattern, Union

from .sink import Sink, FileSink
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
        self.limiter = Limiter(self.qps, 100, MemoryStorage())
        outfilename = self.host.replace(".", "-") + ".csv"
        self.sink = FileSink(outfilename)

        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "pragma": "no-cache",
            "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "cross-site",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
        }

        # referrer と cookies（credentials=include 相当）を指定
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def discover_urls(self):
        pass
        # index_url = f"https://{self.host}/"
        # tree = sitemap_tree_for_homepage(index_url)
        # for page in tree.all_pages():
        #     if self._path_allowed(page.url):
        #         self.url_manager.add_url(page.url)

    def parse(self, url, html) -> List[str]:
        pass

    def _path_allowed(self, url):
        path = urlparse(url).path or "/"
        return any(rx.search(path) for rx in self.url_filter)

    def scrape(self, progress=False):
        prog_f = lambda iter: (
            tqdm(iter, total=self.url_manager.urls_total) if progress else iter
        )
        if self.url_manager.get_cursor() == self.url_manager.upper:
            self.url_manager.set_cursor()
        for key, url in prog_f(self.url_manager.to_iter(self.url_manager.get_cursor())):
            while not self.limiter.consume(self.host):
                time.sleep(0.01)
            try:
                res = self.session.get(
                    url, headers=self.headers, allow_redirects=True, timeout=15
                )
                res.raise_for_status()

                if res.status_code == 200:
                    parsed = self.parse(url, res.text)
                    self.sink.write(parsed)
            except:
                pass
            self.url_manager.set_cursor(key)
        self.url_manager.set_cursor()
