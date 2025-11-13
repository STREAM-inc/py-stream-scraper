from urllib.parse import urlparse
import sys, os, csv, json, importlib, xml.etree.ElementTree as ET
from typing import Iterator, Optional
import click
from py_stream_scraper.scraper import DistributedScraper, Scraper
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
import redis

from py_stream_scraper.url_manager import DiskURLManager

log = Console(stderr=True)

BANNER = r"""
   _____ _______ _____  ______          __  __    _____  _____ _____            _____  ______ _____  
  / ____|__   __|  __ \|  ____|   /\   |  \/  |  / ____|/ ____|  __ \     /\   |  __ \|  ____|  __ \ 
 | (___    | |  | |__) | |__     /  \  | \  / | | (___ | |    | |__) |   /  \  | |__) | |__  | |__) |
  \___ \   | |  |  _  /|  __|   / /\ \ | |\/| |  \___ \| |    |  _  /   / /\ \ |  ___/|  __| |  _  / 
  ____) |  | |  | | \ \| |____ / ____ \| |  | |  ____) | |____| | \ \  / ____ \| |    | |____| | \ \ 
 |_____/   |_|  |_|  \_\______/_/    \_\_|  |_| |_____/ \_____|_|  \_\/_/    \_\_|    |______|_|  \_\
                                                                                                     
                                                                                                     
"""


def load_class(path: str):
    mod, cls = path.rsplit(".", 1)
    return getattr(importlib.import_module(mod), cls)


# ---- builtin discoverers (stdoutにURLを吐く) ----
def discover_builtin(kind: str, source: str) -> Iterator[str]:
    if kind == "txt":
        with open(source, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u:
                    yield u
    elif kind == "csv":
        with open(source, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            if r.fieldnames and "URL" in r.fieldnames:
                for row in r:
                    u = (row.get("URL") or "").strip()
                    if u:
                        yield u
            else:
                f.seek(0)
                reader = csv.reader(f)
                _ = next(reader, [])
                for row in reader:
                    if row and row[0].strip():
                        yield row[0].strip()
    elif kind == "sitemap":
        import requests

        r = requests.get(source, timeout=15)
        r.raise_for_status()
        root = ET.fromstring(r.content)
        for loc in root.findall(".//{*}loc"):
            t = (loc.text or "").strip()
            if t:
                yield t
    else:
        raise click.UsageError("--from は sitemap/txt/csv のみ")


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def _cli():
    log.print(Panel(BANNER, style="green"))


# ---------------- discover ----------------


@_cli.command()
@click.option(
    "--from",
    "from_",
    type=click.Choice(["sitemap", "txt", "csv"]),
    help="discover source",
)
@click.option(
    "--host", required=False, help="URLManager 用のホスト名（例: example.com）"
)
@click.argument("arg", required=False)  # ← 引数は任意に
def discover(from_: Optional[str], host: Optional[str], arg: Optional[str]):
    """
    使い方:
      sx discover module.ClassName
      sx discover --from sitemap --host example.com
      sx discover --from txt     --host example.com urls.txt
      sx discover --from csv     --host example.com urls.csv
    """
    log.rule("[bold green]discover")

    # --- クラスモード（--from なし）: クラス名必須 ---
    if from_ is None:
        if not arg:
            raise click.UsageError(
                "クラス名を指定してください（例: main.ArubaitonaituScraper）"
            )
        Cls = load_class(arg)
        inst = Cls()
        with Progress(
            SpinnerColumn(), TextColumn("{task.description}"), console=log
        ) as p:
            t = p.add_task("discover_urls()", start=True)
            inst.discover_urls()  # クラス側で self.url_manager.add_url(...)
            p.update(t, description="done")
        return

    # --- ここから builtin ルートは host 必須 ---
    if not host:
        raise click.UsageError("--host を指定してください（例: --host example.com）")

    # --- sitemap: 引数なしでOK（USPで自動探索）---
    if from_ == "sitemap":
        from py_stream_scraper import Scraper

        inst = Scraper(host=host, qps=10)
        with Progress(
            SpinnerColumn(), TextColumn("{task.description}"), console=log
        ) as p:
            t = p.add_task(f"discover_urls_from_sitemap({host})", start=True)
            inst.discover_urls_from_sitemap()  # USPで自動的にサイトマップ探索→URLManagerへenqueue
            p.update(t, description="done")
        return

    # --- txt/csv: パス必須 → DiskURLManager(host) にenqueue ---
    if not arg:
        raise click.UsageError(
            f"{from_} のソースパスを指定してください（例: urls.{from_}）"
        )

    from py_stream_scraper.url_manager import DiskURLManager

    urlman = DiskURLManager(host)

    def iter_urls_txt(path: str):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u:
                    yield u

    def iter_urls_csv(path: str):
        import csv

        with open(path, "r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f)
            if r.fieldnames and "URL" in r.fieldnames:
                for row in r:
                    u = (row.get("URL") or "").strip()
                    if u:
                        yield u
            else:
                f.seek(0)
                reader = csv.reader(f)
                _ = next(reader, [])
                for row in reader:
                    if row and row[0].strip():
                        yield row[0].strip()

    with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=log) as p:
        t = p.add_task(f"enqueue from {from_} → {host}", start=True)
        cnt = 0
        it = iter_urls_txt if from_ == "txt" else iter_urls_csv
        for u in it(arg):
            urlman.add_url(u)
            cnt += 1
        p.update(t, description=f"done ({cnt} urls)")


@_cli.command()
@click.option("--host", help="show details")
def list(host):
    manager = DiskURLManager(host)
    cnt = 0
    for key, value in manager.to_iter(manager.lower):
        url_str = value.decode("utf-8")
        cnt += 1
        print(url_str)
    print(f"Total: {cnt} urls")


@_cli.command()
@click.option("--host", help="show details")
def stream(host):
    scraper = DistributedScraper(host, 10)
    scraper.start_stream()
    log.print("Stream started. name: " + scraper.stream_name)


# ---------------- scrape ----------------
@_cli.command()
@click.argument("klass", required=True)
def scrape(klass: str):
    """
    使い方:
      # 1) 事前に class discover を行ってURLManagerにURLがある場合
      sx scrape module.ClassName

      # 2) discover --from の結果をパイプで投入してから実行したい場合
      sx discover --from sitemap https://.../sitemap.xml | sx scrape module.ClassName
    """
    Cls = load_class(klass)
    inst = Cls()

    # stdin からURLが来ているなら URLManager に積む
    if not sys.stdin.isatty():
        with Progress(
            SpinnerColumn(), TextColumn("{task.description}"), console=log
        ) as p:
            t = p.add_task("enqueue urls → URLManager", start=True)
            enq = 0
            for line in sys.stdin:
                u = line.strip()
                if u:
                    # Scraper 実装に合わせる（url_managerはpy_stream_scraper.Scraperにある想定）
                    inst.url_manager.add_url(u)
                    enq += 1
            p.update(t, description=f"enqueued {enq} urls")

    # 実行（ユーザー実装の scrape(progress=True) をそのまま呼ぶ）
    log.rule("[bold green]scrape(progress=True)")
    inst.scrape(progress=True)


def main():
    _cli()


if __name__ == "__main__":
    main()
