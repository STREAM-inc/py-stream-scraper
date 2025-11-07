from py_stream_scraper.url_manager import DiskURLManager


def test_url_parititioned_by_host():
    urls = ["https://a.com/dab", "https://a.com/ab", "https://a.com/cd"]
    url_manager = DiskURLManager(host="a.com")
    for url in urls:
        url_manager.add_url(url)

    iterated = [url for url in url_manager.to_iter()]
    assert len(iterated) == 3
