from py_stream_scraper import ScraperBuilder


def parse(html: str, url: str):
    import re

    m = re.search(r"<title>(.*?)</title>", html, flags=re.I | re.S)
    title = m.group(1).strip() if m else "(no title)"
    print(url, "→", title)


scraper = (
    ScraperBuilder()
    .set_host("www.ekiten.jp")
    .set_qps(10)
    .set_filter(r"^/slnH*")  # 例: /str で始まるパスを対象
    .set_parser(parse)  # parse(html, url)
    .build()
)

scraper.scrape()
