#!/usr/bin/env python3
"""
tdcs_downloader.py

TDCS (M03A / M06A) 歷史 tar.gz 下載器。
只負責「下載到本機 + 驗證」，不做解壓、不做 HDFS put（那是後面兩支的工作）。

設計重點（依專案 ADR-001）：
- 下載與落地分開：下載失敗（網路/rate-limit）跟落地失敗（HDFS 寫一半斷）原因不同，
  分兩支可以各自獨立重試，半途斷線不會在 data lake 留壞檔。
- 冪等可續跑：目標檔已存在就跳過，不計時、不等待。
- 原子寫入：先寫 .part，下載+驗證完整才 rename 成正式檔名，不留半成品。
- 完整性驗證：用 tarfile.open 讀 members，壞檔直接刪除（不留假的成功檔）。
- 智慧節流：從「收到 response 的那一刻」開始算間隔，下次請求前補等到滿間隔，
  下載/驗證耗時算進等待裡，不是「處理完再傻等 45 秒」。
- 限流預警：連續 403/429 達門檻就印警告，不用等整批跑完才發現被擋。
- 設定集中在最上方，換機器只改設定，程式邏輯不動。

⚠️ 此檔為依照過去對話規格重建，尚未跟你手上實際跑過的版本逐行核對，
   等你傳原始檔過來我再比對修正。
"""

import argparse
import os
import sys
import time
import tarfile
import logging
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta

# ============================================================
# 設定區（換機器 / 換範圍只改這裡）
# ============================================================

DOWNLOAD_ROOT = os.path.expanduser("~/wulin/raw")   # 本機 tar.gz 存放根目錄
DATA_TYPES = ["M03A", "M06A"]

# 預設日期區間（沒給 --start/--end 時才用這個；命令列優先）
START_DATE = date(2026, 1, 1)
END_DATE = date(2026, 4, 30)

SOURCE_URL_TEMPLATE = (
    "https://tisvcloud.freeway.gov.tw/history/TDCS/{dtype}/{dtype}_{ymd}.tar.gz"
)

THROTTLE_SECONDS = 45          # 高公局規定 >40 秒，用 45 秒當緩衝
RATE_LIMIT_WARN_THRESHOLD = 3  # 連續幾次 403/429 就警告

LOG_PATH = os.path.expanduser("~/wulin/logs/tdcs_downloader.log")   # 與其他程式一致，統一放 ~/wulin/logs

# ============================================================
# Logging
# ============================================================

# FileHandler 不會自動建資料夾，先確保 log 目錄存在（否則啟動就 FileNotFoundError）
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("tdcs_downloader")


# ============================================================
# 核心函式
# ============================================================

def daterange(start: date, end: date):
    cur = start
    while cur <= end:
        yield cur
        cur += timedelta(days=1)


def target_path(dtype: str, d: date) -> str:
    ymd = d.strftime("%Y%m%d")
    dir_path = os.path.join(DOWNLOAD_ROOT, dtype)
    os.makedirs(dir_path, exist_ok=True)
    return os.path.join(dir_path, f"{dtype}_{ymd}.tar.gz")


def is_downloaded(dtype: str, d: date) -> bool:
    """冪等檢查：目標檔已存在就視為完成，跳過。"""
    return os.path.exists(target_path(dtype, d))


def verify_tarball(path: str) -> bool:
    """完整性驗證：能正常讀出 members 才算好檔。"""
    try:
        with tarfile.open(path, "r:gz") as tf:
            members = tf.getmembers()
            return len(members) > 0
    except Exception as e:
        log.warning(f"驗證失敗，判定為壞檔：{path}（{e}）")
        return False


def download_one(dtype: str, d: date) -> str:
    """
    下載單一天的 tar.gz。
    回傳值："ok" / "skip" / "not_found" / "rate_limited" / "fail"
    """
    final_path = target_path(dtype, d)

    if is_downloaded(dtype, d):
        log.info(f"SKIP（已存在） {dtype} {d}")
        return "skip"

    url = SOURCE_URL_TEMPLATE.format(dtype=dtype, ymd=d.strftime("%Y%m%d"))
    part_path = final_path + ".part"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "tdcs-downloader/1.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            response_ts = time.time()  # 節流計時基準：收到 response 就開始算
            with open(part_path, "wb") as f:
                f.write(resp.read())

        if not verify_tarball(part_path):
            os.remove(part_path)
            log.error(f"FAIL（驗證未通過，已刪除半成品） {dtype} {d}")
            return "fail"

        os.rename(part_path, final_path)
        size_mb = os.path.getsize(final_path) / (1024 * 1024)
        log.info(f"OK {dtype} {d} ({size_mb:.1f} MB)")
        return ("ok", response_ts)

    except urllib.error.HTTPError as e:
        if os.path.exists(part_path):
            os.remove(part_path)
        if e.code == 404:
            log.warning(f"NOT_FOUND {dtype} {d}（來源尚未產生此日資料）")
            return "not_found"
        if e.code in (403, 429):
            log.warning(f"RATE_LIMITED {dtype} {d}（HTTP {e.code}）")
            return "rate_limited"
        log.error(f"FAIL {dtype} {d}（HTTP {e.code}）")
        return "fail"

    except Exception as e:
        if os.path.exists(part_path):
            os.remove(part_path)
        log.error(f"FAIL {dtype} {d}（{e}）")
        return "fail"


def wait_until_next(last_response_ts: float, throttle: float):
    """
    智慧節流：距離上次「收到 response」的時間，補等到滿 throttle 秒。
    下載/驗證耗費的時間已經算在間隔裡，不會多等。
    """
    if last_response_ts is None:
        return
    elapsed = time.time() - last_response_ts
    remaining = throttle - elapsed
    if remaining > 0:
        time.sleep(remaining)


# ============================================================
# 主流程
# ============================================================

def parse_args():
    p = argparse.ArgumentParser(
        description="TDCS 歷史 tar.gz 下載器（只下載+驗證，不解壓、不上 HDFS）")
    p.add_argument("--start", type=str, default=None,
                   help="起日 YYYY-MM-DD（需與 --end 成對；不給則用檔案 CONFIG 的 START_DATE）")
    p.add_argument("--end", type=str, default=None,
                   help="迄日 YYYY-MM-DD（需與 --start 成對）")
    p.add_argument("--dtype", choices=["M03A", "M06A"], default=None,
                   help="只下載指定 type；不給則兩個都下載")
    return p.parse_args()


def resolve_range(a):
    """日期：命令列優先，沒給才回退用 CONFIG 的 START_DATE / END_DATE。"""
    if a.start and a.end:
        return (datetime.strptime(a.start, "%Y-%m-%d").date(),
                datetime.strptime(a.end, "%Y-%m-%d").date())
    if a.start or a.end:
        log.error("--start 與 --end 必須成對給；只給一個無效")
        sys.exit(2)
    return START_DATE, END_DATE


def main():
    a = parse_args()
    start, end = resolve_range(a)
    dtypes = [a.dtype] if a.dtype else DATA_TYPES
    if start > end:
        log.error(f"起日 {start} 晚於迄日 {end}")
        sys.exit(2)

    total_days = (end - start).days + 1
    log.info(f"開始下載：{start} ~ {end}，共 {total_days} 天 × {len(dtypes)} 種類型 {dtypes}")

    last_response_ts = None
    consecutive_rate_limited = 0

    for d in daterange(start, end):
        for dtype in dtypes:
            # 只有「真的發送請求」才需要節流；skip 不用等
            if not is_downloaded(dtype, d):
                wait_until_next(last_response_ts, THROTTLE_SECONDS)

            result = download_one(dtype, d)

            if isinstance(result, tuple):
                status, last_response_ts = result
            else:
                status = result

            if status == "rate_limited":
                consecutive_rate_limited += 1
                if consecutive_rate_limited >= RATE_LIMIT_WARN_THRESHOLD:
                    log.warning(
                        f"⚠️ 連續 {consecutive_rate_limited} 次被限流，"
                        f"可能已被高公局擋下，建議手動確認"
                    )
            else:
                consecutive_rate_limited = 0

    log.info("下載作業結束。")


if __name__ == "__main__":
    main()
