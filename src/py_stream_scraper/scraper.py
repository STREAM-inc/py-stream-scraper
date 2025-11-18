import datetime
import hashlib
import re
import random
import time
from typing import Callable, Iterable, List, Optional
from urllib.parse import urlparse
import uuid
from click import Path
import requests
import redis
from usp.tree import sitemap_tree_for_homepage
from tqdm import tqdm
import aiohttp
import asyncio
import brotli
import enum
import socket
import os
import re
from typing import Callable, Iterable, List, Optional, Pattern, Union

from .sink import Sink, FileSink
from .url_manager import DiskURLManager
from .rate_limiter import Limiter, MemoryStorage
from .log import setup_logger
from .cache import Cache


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


class FetchStrategy(enum.Enum):
    STOP_ON_FAIL = 1
    NEVER_STOP = 2


class Scraper:
    def __init__(
        self,
        host,
        qps,
        redis_client=None,
        max_concurrency=10,
        fetch_strategy=FetchStrategy.STOP_ON_FAIL,
    ):
        self.log = setup_logger()
        self.host = host
        self.qps = qps
        self.redis = redis_client or redis.Redis(
            host="localhost", port=6379, decode_responses=True
        )
        self.max_concurrency = max_concurrency
        self.fetch_strategy = fetch_strategy
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

        self.running = False

    def discover_urls(self):
        pass

    def discover_urls_from_sitemap(self, r_filter=None):
        tree = sitemap_tree_for_homepage(f"https://{self.host}")
        for page in tree.all_pages():
            if r_filter and r_filter.search(page.url):
                self.url_manager.add_url(page.url)

    def parse(self, url, html) -> List[str]:
        pass

    def _path_allowed(self, url):
        path = urlparse(url).path or "/"
        return any(rx.search(path) for rx in self.url_filter)

    async def _wait_for_token(self):
        while not self.limiter.consume(self.host):
            await asyncio.sleep(0.01)

    async def _fetch_one(self, session: aiohttp.ClientSession, key: str, url: str):
        await self._wait_for_token()
        try:
            self.log.info(f"fetching: {url}")
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
            self.log.error(e)
        finally:
            self.url_manager.set_cursor(key.encode("utf-8"))

    def _fetch_one_sync(self, session, key, url, cache: Cache | None = None):
        time.sleep(1.0 / self.qps)
        try:
            self.log.info(f"fetching: {url}")
            with session.get(
                url, headers=self.headers, allow_redirects=True, timeout=15
            ) as resp:
                resp.raise_for_status()
                if resp.status_code == 200:
                    html = resp.text
                    if cache:
                        compressed = brotli.compress(html.encode("utf-8"))
                        cache.write(url, compressed)
                    else:
                        parsed = self.parse(url, html)
                        self.sink.write(parsed)
                    self.url_manager.set_cursor(key.encode("utf-8"))
        except Exception as e:
            self.log.error(e)
            if self.fetch_strategy == FetchStrategy.STOP_ON_FAIL:
                self.running = False

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
                    tasks.append(
                        asyncio.create_task(
                            worker(key.decode("utf-8"), url.decode("utf-8"))
                        )
                    )

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

    def scrape_sync(
        self,
        progress: bool = False,
        ssl: bool = True,
        cache: Cache | None = None,
        url_filter: str | None = None,
    ):
        self.running = True

        if self.url_manager.get_cursor() == self.url_manager.upper:
            self.url_manager.set_cursor()

        pbar = None
        if progress:
            pbar = tqdm(
                total=self.url_manager.urls_total,
                initial=self.url_manager.url_current_index,
                desc=f"Scraping {self.host}",
            )

        session = requests.Session()
        if not ssl:
            session.verify = False

        try:
            for key, url in self.url_manager.to_iter(self.url_manager.get_cursor()):
                key_str = key.decode("utf-8")
                url_str = url.decode("utf-8")

                if url_filter:
                    ptn = re.compile(url_filter)
                    if not ptn.search(url_str):
                        continue
                if url_str.startswith("/") or not url_str.startswith("http"):
                    url_str = f"https://{self.host}{url_str}"

                self._fetch_one_sync(session, key_str, url_str, cache=cache)

                if not self.running:
                    return

                if pbar:
                    pbar.update(1)

        finally:
            if pbar:
                pbar.close()
            session.close()

        # 終了位置を保存
        self.url_manager.set_cursor()

    def scrape(self, progress: bool = False):
        return asyncio.run(self.scrape_async(progress=progress))


class DistributedScraper(Scraper):
    def __init__(
        self,
        host,
        qps,
        redis_client=None,
        max_concurrency=10,
        fetch_strategy=FetchStrategy.STOP_ON_FAIL,
        consumer_name: str | None = None
    ):
        super().__init__(
            host,
            qps,
            redis_client=redis_client,
            max_concurrency=max_concurrency,
            fetch_strategy=fetch_strategy,
        )

        self.consumer_name = consumer_name or f"{socket.gethostname()}:{os.getpid()}"

        try:
            self.redis.xgroup_create(
                self.stream_name, "scrapers", id=">", mkstream=True
            )
        except:
            pass

    def recover_stuck_messages(self, session, cache: Cache | None = None, url_filter: str | None = None, min_idle_ms: int = 60_000, batch: int = 100):
        cursor = "0-0"
        while True:
            cursor, messages, _ = self.redis.xautoclaim(
                self.stream_name,
                "scrapers",
                self.consumer_name,
                min_idle_ms,
                cursor,
                count=batch,
            )

            if not messages:
                break

            for msg_id, data in messages:
                url_str = data[b"url"].decode("utf-8")

                if url_filter:
                    ptn = re.compile(url_filter)
                    if not ptn.search(url_str):
                        continue
                if url_str.startswith("/") or not url_str.startswith("http"):
                    url_str = f"https://{self.host}{url_str}"

                self._fetch_one_sync(session, url_str, msg_id, cache=cache)

                if not self.running:
                    return

    def scrape_sync(
        self,
        ssl: bool = True,
        cache: Cache | None = None,
        url_filter: str | None = None,
    ):
        self.running = True

        session = requests.Session()
        if not ssl:
            session.verify = False

        self.recover_stuck_messages(session, cache=cache, url_filter=url_filter)

        while True:
            read_res = self.redis.xreadgroup(
                groupname="scrapers",
                consumername=str(uuid.uuid4()),
                streams={self.stream_name: ">"},
                count=10,
                block=5000,
            )
            try:
                for stream, messages in read_res:
                    for msg_id, data in messages:
                        url_str = data[b"url"].decode("utf-8")

                        if url_filter:
                            ptn = re.compile(url_filter)
                            if not ptn.search(url_str):
                                continue
                        if url_str.startswith("/") or not url_str.startswith("http"):
                            url_str = f"https://{self.host}{url_str}"

                        self._fetch_one_sync(session, url_str, msg_id, cache=cache)

                        if not self.running:
                            return
            finally:
                session.close()

    def _fetch_one_sync(self, session, url, msg_id, cache: Cache | None = None):
        time.sleep(1.0 / self.qps)
        try:
            self.log.info(f"fetching: {url}")
            with session.get(
                url, headers=self.headers, allow_redirects=True, timeout=15
            ) as resp:
                resp.raise_for_status()
                if resp.status_code == 200:
                    html = resp.text
                    self.redis.xack(self.stream_name, "scrapers", msg_id)
                    if cache:
                        compressed = brotli.compress(html.encode("utf-8"))
                        cache.write(url, compressed)
                    else:
                        parsed = self.parse(url, html)
                        self.sink.write(parsed)
        except Exception as e:
            self.log.error(e)
            if self.fetch_strategy == FetchStrategy.STOP_ON_FAIL:
                self.running = False

    def scrape(self, progress: bool = False):
        return asyncio.run(self.scrape_async(progress=progress))

    def start_stream(self):
        for key, value in self.url_manager.to_iter(self.url_manager.lower):
            url_str = value.decode("utf-8")
            self.log.info(f"streaming: {url_str}")
            self.redis.xadd(self.stream_name, {"url": url_str})
