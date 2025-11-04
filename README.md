# Introduction
スクレイピング状況の可視化と障害耐性、分散処理をするためのpackageです。

```python
from py_stream_scraper import ScraperBuilder

def parse(html: str):

scraper = ScraperBuilder()
	.set_host("www.hotpepper.jp")
	.set_qps(10)
	.set_filter("/str*")
	.set_parser(parse)
	.build()

scraper.scrape()
```