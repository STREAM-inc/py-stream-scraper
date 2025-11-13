from datetime import datetime
from py_stream_scraper import Scraper

import requests
from bs4 import BeautifulSoup
import time


class ArubaitonaituScraper(Scraper):
    def __init__(self):
        super().__init__(
            host="baito.nights.fun",
            qps=10,
        )

    def discover_urls(self):
        roots = [
            f"https://baito.nights.fun/A{str(i).zfill(2)}/job-list/"
            for i in range(1, 31)
        ]

        for root in roots:
            self._get_shop_links(root)

    def parse(self, url: str, html: str):
        COLUMNS = [
            "URL",
            "都道府県",
            "店名",
            "業種",
            "所在地",
            "アクセス",
            "最寄駅",
            "電話番号",
            "職種",
            "給与",
            "勤務日／時",
            "応募資格",
            "平均年齢",
            "採用担当",
            "取得時刻",
        ]

        soup = BeautifulSoup(html, "html.parser")

        area_name = soup.find(
            "span", class_="pc_only area_display pc_disp", id="header_area_name"
        )
        if area_name:
            prefecture = area_name.get_text(strip=True)
        else:
            prefecture = ""

        tables = soup.find_all("table", class_="basic_information_table")

        shop_info = {column: "" for column in COLUMNS}
        shop_info["URL"] = url
        shop_info["都道府県"] = prefecture

        for table in tables:
            rows = table.find_all("tr")

            for row in rows:
                th = row.find("th")
                td = row.find("td")

                if th and td:
                    key = th.get_text(strip=True)
                    if key == "所在地":
                        address_div = td.find("div", class_="address")
                        if address_div:
                            value = address_div.get_text(strip=True)
                        else:
                            value = td.get_text(strip=True)
                    else:
                        value = td.get_text(strip=True)

                    if key in COLUMNS:
                        shop_info[key] = value

        shop_info["取得時刻"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        return shop_info

    def _get_shop_links(self, url):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        current_url = url
        page_count = 1

        while current_url:
            try:
                print(f"\nページ {current_url} を処理中...")
                response = requests.get(current_url, headers=headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                area_elements = soup.find_all(class_="areasmall_area")
                for area in area_elements:
                    shop_link = area.find_next("a", class_="anothertab")
                    if shop_link:
                        href = shop_link.get("href")
                        if href:
                            link = f"https://baito.nights.fun{href}"
                            self.url_manager.add_url(link)

                pagination = soup.find("ul", class_="pagination")
                if pagination:
                    all_links = pagination.find_all("a")
                    next_page = None
                    for link in all_links:
                        if link.text.isdigit() and int(link.text) == page_count + 1:
                            next_page = link
                            break

                    print(f"次ページのリンクが見つかりました:")

                    if next_page:

                        current_url = f"https://baito.nights.fun{next_page['href']}"
                        print(f"次ページのリンク: {current_url}")
                        page_count += 1
                        time.sleep(1)
                    else:
                        current_url = None
                        print(f"\n全{page_count}ページの処理が完了しました。")
                else:
                    current_url = None
                    print(f"\n全{page_count}ページの処理が完了しました。")

            except requests.RequestException as e:
                print(f"エラーが発生しました: {e}")
                break


def main():
    scraper = ArubaitonaituScraper()
    # scraper.discover_urls()
    scraper.scrape(progress=True)


if __name__ == "__main__":
    main()
