from py_stream_scraper.scraper import ScraperBuilder


def parse(html: str):
    print(html)


scraper = (
    ScraperBuilder()
    .set_host("minimodel.jp")
    .set_qps(10)
    .set_filter(r"^/r/")
    .set_parser(parse)
    .build()
)

# scraper.discover_urls()

scraper.scrape()
