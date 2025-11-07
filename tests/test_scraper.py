from types import SimpleNamespace
from py_stream_scraper.scraper import Scraper, ScraperBuilder
from py_stream_scraper.sink import FileSink
from py_stream_scraper import scraper as scraper_mod
import tempfile
from pathlib import Path


class DummyTree:
    def __init__(self, urls):
        self._urls = urls

    def all_pages(self):
        for u in self._urls:
            yield SimpleNamespace(url=u)


def _dummy_sitemap(_):
    return DummyTree(
        [
            "https://example.com/",
            "https://example.com/blog/a.html",
            "https://example.com/wp-admin/panel",
            "https://example.com/news/today.html",
        ]
    )


def test_discover_urls_pushes_to_stream(redis_client, url_filter, monkeypatch):
    from py_stream_scraper import scraper as scraper_mod

    monkeypatch.setattr(scraper_mod, "sitemap_tree_for_homepage", _dummy_sitemap)

    s = Scraper(
        host="example.com", qps=2, url_filter=url_filter, redis_client=redis_client
    )
    s.discover_urls()

    entries = redis_client.xrange(s.stream_name, min="-", max="+")
    pushed = [fields["url"] for _id, fields in entries]

    assert "https://example.com/" not in pushed
    assert "https://example.com/blog/a.html" in pushed
    assert "https://example.com/news/today.html" in pushed
    assert "https://example.com/wp-admin/panel" not in pushed


def test_builder_builds_and_discovers_urls(monkeypatch, redis_client):
    """
    set_host / set_qps / set_filter で build した Scraper が
    discover_urls 時に filter を満たすURLだけを Redis Stream に積むことを検証。
    """
    monkeypatch.setattr(scraper_mod, "sitemap_tree_for_homepage", _dummy_sitemap)

    scraper = (
        ScraperBuilder()
        .set_host("example.com")
        .set_qps(2)
        .set_filter(r"^/(blog|news)/")
        .set_redis_client(redis_client)
        .build()
    )

    scraper.discover_urls()

    entries = redis_client.xrange(scraper.stream_name, min="-", max="+")
    pushed = [fields["url"] for _id, fields in entries]

    assert "https://example.com/" not in pushed
    assert "https://example.com/blog/a.html" in pushed
    assert "https://example.com/news/today.html" in pushed
    assert "https://example.com/wp-admin/panel" not in pushed


def test_builder_with_sink(redis_client):
    """
    set_sink で Sink を設定できることを検証
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = Path(tmpdir) / "output.csv"
        sink = FileSink(str(filepath))

        scraper = (
            ScraperBuilder()
            .set_host("example.com")
            .set_qps(2)
            .set_filter(r"^/(blog|news)/")
            .set_sink(sink)
            .set_redis_client(redis_client)
            .build()
        )

        # Scraperにsink属性が追加されていることを確認
        assert hasattr(scraper, "sink")
        assert scraper.sink == sink
