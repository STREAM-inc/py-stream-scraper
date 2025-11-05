"""
データシンク（保存先）を定義するモジュール
"""
import csv
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional


class Sink(ABC):
    """
    スクレイピングデータの保存先を表す抽象基底クラス
    """

    @abstractmethod
    def write(self, data: Any) -> None:
        """
        データを保存する

        Args:
            data: 保存するデータ
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        リソースをクローズする
        """
        pass


class FileSink(Sink):
    """
    CSVファイルにデータを保存するSink
    """

    def __init__(self, filepath: str, mode: str = "w", encoding: str = "utf-8-sig"):
        """
        Args:
            filepath: 保存先のファイルパス
            mode: ファイルオープンモード（デフォルト: "w"）
            encoding: ファイルエンコーディング（デフォルト: "utf-8-sig"）
        """
        self.filepath = Path(filepath)
        self.mode = mode
        self.encoding = encoding
        self._file = None
        self._writer = None
        self._headers_written = False

        # ディレクトリが存在しない場合は作成
        self.filepath.parent.mkdir(parents=True, exist_ok=True)

    def write(self, data: Any) -> None:
        """
        データをCSVファイルに書き込む

        Args:
            data: 辞書、辞書のリスト、またはその他のデータ
        """
        if self._file is None:
            self._file = open(self.filepath, self.mode, encoding=self.encoding, newline='')

        # データが辞書の場合、CSVとして書き込む
        if isinstance(data, dict):
            if self._writer is None:
                self._writer = csv.DictWriter(self._file, fieldnames=data.keys())
                self._writer.writeheader()
                self._headers_written = True
            self._writer.writerow(data)
            self._file.flush()

        # データが辞書のリストの場合
        elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
            if self._writer is None:
                self._writer = csv.DictWriter(self._file, fieldnames=data[0].keys())
                self._writer.writeheader()
                self._headers_written = True
            self._writer.writerows(data)
            self._file.flush()

        # その他の場合はJSON Lines形式で保存
        else:
            if self._writer is None:
                # JSON Lines形式
                self._file.write(json.dumps(data, ensure_ascii=False) + "\n")
            else:
                # 既にCSVライターが初期化されている場合はエラー
                raise ValueError("Cannot mix CSV and JSON data in the same sink")
            self._file.flush()

    def close(self) -> None:
        """
        ファイルをクローズする
        """
        if self._file:
            self._file.close()
            self._file = None
            self._writer = None
            self._headers_written = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class ConsoleSink(Sink):
    """
    コンソールにデータを出力するSink
    """

    def __init__(self, pretty: bool = True):
        """
        Args:
            pretty: Pretty print するかどうか
        """
        self.pretty = pretty

    def write(self, data: Any) -> None:
        """
        データをコンソールに出力する

        Args:
            data: 出力するデータ
        """
        if self.pretty:
            print(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            print(data)

    def close(self) -> None:
        """
        何もしない（コンソール出力なのでクローズ不要）
        """
        pass
