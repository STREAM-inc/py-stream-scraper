# Introduction
スクレイピング状況の可視化と障害耐性、分散処理をするためのpackageです。

基本的な使い方
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

# Contributions
```sh
git clone https://github.com/STREAM-inc/py-stream-scraper
cd py-stream-scraper

uv sync
uv run pytest
```


# architecture

## Distributed Scraping
k3sをもちいて社内PC間でclusterを構築している。

master nodeの設定
```sh
sudo cat /var/lib/rancher/k3s/server/node-token
```

worker nodeの設定
```sh
curl -sfL https://get.k3s.io | K3S_URL="https://<master_node_ip>:6443" k3S_TOKEN=<node-token> sh -
```

## URL management
URLを管理する方法について考える。

満たすべき要件をまずまとめる

1. order preservation

スクレイピングが途中で中断した場合、offsetを指定して途中から始めるには保存されているURLに順序が保たれている必要がある。

2. compactness

URLの量が数百、数千万になることが見込まれているためできるだけコンパクトに保存したい、

3. iterable 

すべてのURLをメモリにロードする必要がないようにしたい。

4. partitionable

hostによって分割可能である必要がある。

## fault-recovery
スクレイピングが途中で止まってしまった時の復旧方法について考える。

設定
urlを読み取るまいに
rocksDBに

{host}\x00cursor: {key}

を設定する

このkeyが存在しない場合は{host}\x00にセット。（最初から読まれる）

## Debug
スクレイピングの不具合やテストをしやすいようにDebug用の機構を整えたい。
