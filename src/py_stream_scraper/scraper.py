import datetime
import random
from urllib.parse import urlparse
import redis
from usp.tree import sitemap_tree_for_homepage


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

    def discover_urls(self):
        index_url = f"https://{self.host}/"
        tree = sitemap_tree_for_homepage(index_url)
        for page in tree.all_pages():
            if self._path_allowed(page.url):
                self.redis.xadd(self.stream_name, {"url": page.url})

    def _path_allowed(self, url):
        path = urlparse(url).path or "/"
        return any(rx.search(path) for rx in self.url_filter)

    def scrape(self):
        pass
