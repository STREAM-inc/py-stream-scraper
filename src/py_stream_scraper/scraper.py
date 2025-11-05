import datetime
import logging
import re
import time
import random
import asyncio
from typing import Callable, Iterable, List, Optional
import aiohttp
from urllib.parse import urlparse
import redis
from usp.tree import sitemap_tree_for_homepage
from multiprocessing import Process

from py_stream_scraper.log import setup_logger


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

ParseFn = Callable[[str, str], None]  # (html, url) -> None


class Scraper:
    def __init__(
        self,
        host: str,
        qps: float,
        filters: List[re.Pattern],
        parser: ParseFn,
        user_agent: Optional[str] = None,
        timeout_sec: float = 20.0,
        max_concurrency: Optional[int] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.host = host
        self.qps = max(qps, 0.001)
        self.filters = filters
        self.parser = parser
        self.user_agent = user_agent or "py-stream-scraper/0.1 (+asyncio;aiohttp)"
        self.timeout_sec = timeout_sec
        # 同時実行はデフォで QPS と同じ程度（バースト抑制）
        self.max_concurrency = int(max_concurrency or max(1, int(self.qps)))

        # レート制御
        self._min_interval = 1.0 / self.qps
        self._last_req_at = 0.0
        self._rate_lock = asyncio.Lock()

        self.log = logger or setup_logger()

        # 統計
        self._stat_total_urls = 0
        self._stat_fetched = 0
        self._stat_success = 0
        self._stat_failed = 0

        self.log.info(
            "Scraper init host=%s qps=%.3f min_interval=%.3fs max_concurrency=%d ua='%s' timeout=%.1fs",
            self.host,
            self.qps,
            self._min_interval,
            self.max_concurrency,
            self.user_agent,
            self.timeout_sec,
        )

    def _path_allowed(self, url: str) -> bool:
        path = urlparse(url).path or "/"
        if not self.filters:
            return True
        return any(rx.search(path) for rx in self.filters)

    async def _rate_pause(self):
        """
        単純な「グローバル最小間隔」型のレート制御。
        複数タスクがいても 1リクエスト毎に最低 self._min_interval を担保。
        """
        async with self._rate_lock:
            now = time.time()
            delta = now - self._last_req_at
            if delta < self._min_interval:
                await asyncio.sleep(self._min_interval - delta)
            self._last_req_at = time.time()

    async def _fetch_one(self, session: aiohttp.ClientSession, url: str):
        await self._rate_pause()
        try:
            async with session.get(url, timeout=self.timeout_sec) as resp:
                text = await resp.text(errors="ignore")
        except Exception as e:
            # 失敗もパーサに投げたい場合はここで分岐可
            # ひとまず無視（ログに出すならここ）
            return
        try:
            self.parser(text, url)
        except Exception:
            # パーサで落ちても他は回す
            pass

    async def _run_async(self):
        index_url = f"https://{self.host}/"
        try:
            self.log.info("Loading sitemaps for %s", index_url)
            tree = sitemap_tree_for_homepage(index_url)
            pages_iter = tree.all_pages()
            self.log.info("Sitemap loaded for %s", index_url)
        except Exception as e:
            self.log.warning(
                "Sitemap unavailable (%s) → fallback to index only", repr(e)
            )
            pages_iter = [{"url": index_url}]  # 疑似イテレータ

        connector = aiohttp.TCPConnector(limit_per_host=self.max_concurrency, ssl=False)
        headers = {"User-Agent": self.user_agent}

        sem = asyncio.Semaphore(self.max_concurrency)
        async with aiohttp.ClientSession(
            headers=headers, connector=connector
        ) as session:

            async def worker(u: str):
                async with sem:
                    await self._fetch_one(session, u)

            batch_size = 100
            tasks = []

            for page in pages_iter:
                url = getattr(page, "url", None) or page.get("url")
                if not url:
                    continue
                if not self._path_allowed(url):
                    continue

                tasks.append(asyncio.create_task(worker(url)))
                if len(tasks) >= batch_size:
                    # バッチ実行
                    self.log.info("Dispatch batch of %d urls", len(tasks))
                    await asyncio.gather(*tasks, return_exceptions=True)
                    tasks.clear()

            # 残りがあれば最後に処理
            if tasks:
                self.log.info("Dispatch last batch of %d urls", len(tasks))
                await asyncio.gather(*tasks, return_exceptions=True)

        self.log.info("Scraping completed for host=%s", self.host)

    def scrape(self):
        asyncio.run(self._run_async())


def _glob_to_regex(glob: str) -> re.Pattern:
    """
    '/str*' みたいなglobをURLの path に対して使う。
    例:
      '/blog/*' → /blog/ 以下
      '/news*'  → /news で始まる
      '*'       → なんでもOK
    """
    # 必ず先頭はパスとして扱う
    if not glob.startswith("/"):
        glob = "/" + glob
    # fnmatch.translate を使わず簡潔に変換（[], ?, * だけ考慮。必要なら強化）
    # '.' など正規表現メタはエスケープ
    special = ".^$+{}[]|()"
    out = []
    for ch in glob:
        if ch == "*":
            out.append(".*")
        elif ch == "?":
            out.append(".")
        elif ch in special:
            out.append("\\" + ch)
        else:
            out.append(ch)
    # パス全体の先頭マッチを想定（/ からスタート）
    pattern = "^" + "".join(out)
    return re.compile(pattern)


class ScraperBuilder:
    def __init__(self):
        self._host: Optional[str] = None
        self._qps: float = 1.0
        self._filters: List[re.Pattern] = []
        self._parser: Optional[ParseFn] = None
        self._ua: Optional[str] = None
        self._timeout: float = 20.0
        self._max_conc: Optional[int] = None

    def set_host(self, host: str) -> "ScraperBuilder":
        self._host = host
        return self

    def set_qps(self, qps: float) -> "ScraperBuilder":
        self._qps = float(qps)
        return self

    def set_filter(self, pattern: str | Iterable[str]) -> "ScraperBuilder":
        """
        文字列 or 文字列のiterableを受け付ける。
        例: "/str*", ["/blog/*", "/news*"]
        """
        self._filters.clear()
        if isinstance(pattern, str):
            self._filters.append(_glob_to_regex(pattern))
        else:
            for p in pattern:
                self._filters.append(_glob_to_regex(str(p)))
        return self

    def add_filter(self, pattern: str) -> "ScraperBuilder":
        self._filters.append(_glob_to_regex(pattern))
        return self

    def set_parser(self, fn: ParseFn) -> "ScraperBuilder":
        self._parser = fn
        return self

    def set_user_agent(self, ua: str) -> "ScraperBuilder":
        self._ua = ua
        return self

    def set_timeout(self, seconds: float) -> "ScraperBuilder":
        self._timeout = float(seconds)
        return self

    def set_max_concurrency(self, n: int) -> "ScraperBuilder":
        self._max_conc = int(n)
        return self

    def build(self) -> Scraper:
        if not self._host:
            raise ValueError("host is required (call set_host).")
        if not self._parser:
            raise ValueError("parser is required (call set_parser).")
        return Scraper(
            host=self._host,
            qps=self._qps,
            filters=self._filters,
            parser=self._parser,
            user_agent=self._ua,
            timeout_sec=self._timeout,
            max_concurrency=self._max_conc,
        )
