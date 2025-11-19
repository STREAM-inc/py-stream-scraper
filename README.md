# Introduction
スクレイピング状況の可視化と障害耐性、分散処理をするためのライブラリ、コマンドラインツールです。スクレイピングは3段階のフェーズ、

1. URL収集
2. リクエスト
3. 解析

に分けることで効率的なスクレイピングをすることを可能にしています。

## Getting Started
```
mkdir scraping-example
cd scraping-example
uv init
uv add git+https://github.com/STREAM-inc/py-stream-scraper.git
```

## Note
簡単な技術解説
- 収集されたURLは[RocksDB](https://rocksdb.org/)によってディスクに保存される。カレントディレクトリに.rockdbというフォルダが作られているはず。
- 分散処理では [Redis stream](https://medium.com/redis-with-raphael-de-lio/understanding-redis-streams-33aa96ca7206) をつかって対象URLの配信を行っている。
- HTMLの保存には [brotli](https://github.com/google/brotli) を使っている。

## Examples

### URL収集 (sitemap)
パッケージをインストールするとsxというコマンドが使えるようになります。discoverではsitemapから直接URLを収集し、すべて記録しておくことができます。
```sh
sx discover --from sitemap --host <host>
```

### URL収集 (独自ロジック)
DiskURLManagerはURLをディスクに記録し、どこまでスクレイピングしたかも管理してくれます。
```python
import requests
from bs4 import BeautifulSoup

from py_stream_scraper import Scraper
from py_stream_scraper.url_manager import DiskURLManager

res = requests.get("https://retty.me/")
soup = BeautifulSoup(res.text, "html.parser")
hrefs = soup.find_all("a")
links = [a["href"] for a in hrefs]
filtered_links = [link for link in links if "https://retty.me/selection/area" in link]

manager = DiskURLManager("retty.me")

for link in filtered_links:
    manager.add_url(link)
```


### リクエスト
Scraperは収集されたURLに実際にリクエストを送り、その結果を保存してくれます。保存先はディスクとRedisに２種類があり、基本的にRedisを使ってください。
```python
from py_stream_scraper.cache import RedisCache
from py_stream_scraper.scraper import FetchStrategy, Scraper

import redis

r = redis.Redis(host="192.168.100.8", port=6379)
scraper = Scraper(
    "retty.me",
    0.5,
    max_concurrency=1,
    fetch_strategy=FetchStrategy.STOP_ON_FAIL,
    redis_client=r,
)

scraper.scrape_sync(
    cache=RedisCache(scraper.redis), url_filter="/area/pre(\d+)/are(\d+)/sub(\d+)/"
)
```

### 分散リクエスト
IP制限がある場合やレート制限がきつい場合、複数のPCを使ってスクレイピングをすると効率がいいです。

まずコマンドを使って複数のPCがアクセスできるように配信を開始します。
```sh
sx stream --host <host>
```
つぎに普通のリクエストのコードをScraperからDistributedScraperに変えて複数PCで起動すると分散でリクエストを送ることができます。

```python
scraper = DistributedScraper(
    "retty.me",
    0.5,
    max_concurrency=1,
    fetch_strategy=FetchStrategy.STOP_ON_FAIL,
    redis_client=r,
)
```

コンピューターによって若干環境が違ったりするのでDocker container 化するのが望ましいです。また、社内PCにk3sを使いクラスタを構築してあるので、そこから起動することができます。（TASK. 詳細記載）

# Contributions
```sh
git clone https://github.com/STREAM-inc/py-stream-scraper
cd py-stream-scraper

uv sync
uv run pytest
```


# Thoughts

## Distributed Scraping
k3sをもちいて社内PC間でclusterを構築している。

master nodeの設定
```sh
sudo cat /var/lib/rancher/k3s/server/node-token
```

worker nodeの設定
```sh
curl -sfL https://get.k3s.io | K3S_URL="https://<master_node_ip>:6443" k3S_TOKEN=<node-token> sh -

sudo mkdir -p /etc/rancher/k3s

sudo tee /etc/rancher/k3s/registries.yaml >/dev/null << 'EOF'
mirrors:
  "<master_node>:5000":
    endpoint:
      - "http://<master_node>:5000"
EOF
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
