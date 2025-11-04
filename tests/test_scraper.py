from types import SimpleNamespace
from py_stream_scraper import Scraper


class DummyTree:
    def __init__(self, urls):
        self._urls = urls

    def all_pages(self):
        for u in self._urls:
            yield SimpleNamespace(url=u)


def test_discover_urls_pushes_to_stream(redis_client, url_filter, monkeypatch):
    from py_stream_scraper import scraper as scraper_mod

    def dummy_sitemap(_):
        return DummyTree(
            [
                "https://example.com/",
                "https://example.com/blog/a.html",
                "https://example.com/wp-admin/panel",
                "https://example.com/news/today.html",
            ]
        )

    monkeypatch.setattr(scraper_mod, "sitemap_tree_for_homepage", dummy_sitemap)

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
