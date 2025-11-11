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
import aiohttp
import asyncio

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

        self.max_concurrency = 10
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

        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def discover_urls(self):
        pass

    def parse(self, url, html) -> List[str]:
        pass

    def _path_allowed(self, url):
        path = urlparse(url).path or "/"
        return any(rx.search(path) for rx in self.url_filter)

    async def _wait_for_token(self):
        while not self.limiter.consume(self.host):
            await asyncio.sleep(0.01)

    async def _fetch_one(self, session: aiohttp.ClientSession, key: bytes, url: str):
        await self._wait_for_token()
        try:
            async with session.get(
                url, headers=self.headers, allow_redirects=True, timeout=15
            ) as resp:
                resp.raise_for_status()
                if resp.status == 200:
                    html = await resp.text()
                    parsed = self.parse(url, html)
                    self.sink.write(parsed)
        except aiohttp.ClientConnectorError:
            pass
        except aiohttp.ClientResponseError:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(e)
        finally:
            self.url_manager.set_cursor(key)

    async def scrape_async(self, progress: bool = False, ssl: bool = True):
        if self.url_manager.get_cursor() == self.url_manager.upper:
            self.url_manager.set_cursor()

        pbar = None
        if progress:
            pbar = tqdm(
                total=self.url_manager.urls_total,
                initial=self.url_manager.url_current_index,
                desc=f"Scraping {self.host}",
            )

        connector = aiohttp.TCPConnector(limit_per_host=self.max_concurrency, ssl=ssl)
        async with aiohttp.ClientSession(
            connector=connector, headers=self.headers
        ) as session:
            sem = asyncio.Semaphore(self.max_concurrency)

            async def worker(key: bytes, url: str):
                async with sem:
                    await self._fetch_one(session, key, url)
                if pbar:
                    pbar.update(1)

            tasks = []
            try:
                for key, url in self.url_manager.to_iter(self.url_manager.get_cursor()):
                    tasks.append(asyncio.create_task(worker(key, url)))

                    if len(tasks) >= self.max_concurrency * 4:
                        for t in asyncio.as_completed(tasks):
                            await t
                        tasks.clear()

                if tasks:
                    for t in asyncio.as_completed(tasks):
                        await t

            finally:
                if pbar:
                    pbar.close()

        self.url_manager.set_cursor()

    def scrape(self, progress: bool = False):
        return asyncio.run(self.scrape_async(progress=progress))
