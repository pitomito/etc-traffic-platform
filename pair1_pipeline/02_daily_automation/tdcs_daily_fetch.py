#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tdcs_daily_fetch.py
===================
職責範圍：只做「抓取高公局 TISVCloud 未壓縮每日 CSV → 存到本機 staging」。
          不碰 HDFS。put 上 HDFS 交由 03_shared/put_to_hdfs.sh 負責。
          （由 tdcs_daily_fetch_to_hdfs.py 拆出，移除其中的 HDFS put 段落。）

對應壓縮延遲區（約 35 天內）的資料：此區資料尚未打包成 tar.gz，
而是以「日 / 時 / csv」三層未壓縮目錄形式存在。本程式抓的就是這種。

輸入來源：Apache autoindex（動態解析,不硬組路徑）
  {BASE_URL}/<DTYPE>/YYYYMMDD/HH/TDCS_<DTYPE>_YYYYMMDD_HHMMSS.csv
    M03A: 每時目錄 12 檔（每 5 分），288 檔/日
    M06A: 每時目錄  1 檔（每小時），24 檔/日

輸出位置：本機 staging（跟 01 的 extract_to_hdfs.sh 統一同一路徑,讓 put_to_hdfs.sh 通吃兩邊）
  staging/extract/<DTYPE>/year=YYYY/month=MM/TDCS_<DTYPE>_YYYYMMDD_HHMMSS.csv
  - partition 用「資料日期」（非執行日期）。
  - 抓下來的 csv 留在本機 staging,不刪（交給 put_to_hdfs.sh 上傳,不由本支清理）。

銜接對象：
  - 下游：03_shared/put_to_hdfs.sh <type> <year> <month>
          （讀本機 staging 同一路徑,整批 put 上 HDFS /raw/…）
  - 排程：02_daily_automation/tdcs_daily_fetch_dag.py（Airflow 先叫本支,再叫 put_to_hdfs.sh）

用法：
  # 兩個 type 都抓（不給 --dtype，先 M06A 再 M03A），日期用 CONFIG 預設
  python3 -u tdcs_daily_fetch.py

  # 兩個都抓，抓 執行日-5 天（單日）
  python3 -u tdcs_daily_fetch.py --offset-days 5

  # 只抓單一 type（debug 或補某一個 type 時用）
  python3 -u tdcs_daily_fetch.py --dtype M06A --offset-days 5

  # 手動補抓一段日期（兩個 type 都抓）
  python3 -u tdcs_daily_fetch.py --start 2026-05-20 --end 2026-06-11

  # 回掃補齊（已淘汰：staging 上傳後即清空,本機快篩失效;保留除錯用。
  #   自動補漏改由 tdcs_backfill_missing.py --auto 以 HDFS 為準檔級補漏,見該檔）
  python3 -u tdcs_daily_fetch.py --backfill-scan

  # 回掃但自訂窗口（掃 d-5 往回 15 天）
  python3 -u tdcs_daily_fetch.py --backfill-scan --scan-window 15

  # 先看會做什麼、不真的下載
  python3 -u tdcs_daily_fetch.py --offset-days 5 --dry-run

日期設定（並存）：
  - 命令列有給 --offset-days 或 --start/--end → 以命令列為準。
  - 命令列都沒給 → 用檔案開頭 CONFIG 的 DEFAULT_MODE / DEFAULT_OFFSET_DAYS /
    DEFAULT_START / DEFAULT_END。詳見 CONFIG 區塊註解。

相依套件：
  - 只用 Python 標準庫，無需 pip install 任何東西。
  - （dtadm pod 重啟會清掉非 ~/wulin 的東西；本程式不裝套件，故不受影響。）

設計重點：
  - 解析 Apache autoindex HTML 動態取得實際時目錄與檔名 → 自動容錯缺漏、不硬組路徑。
  - 全程 sequential、每次 HTTP 請求間隔 >40s（高公局規則：擷取週期 >40 秒）。
  - 冪等：本機 staging 已有同名非空檔就跳過,不重抓（省節流時間）。
  - 下載到本機 staging → 驗證非空 → 留檔（不刪、不 put）。
"""

import argparse
import datetime as dt
import os
import re
import sys
import time
import urllib.request
import urllib.error

# ============================================================
# CONFIG（會變的東西集中在此；對齊知識庫「不寫死」鐵則）
# ============================================================
BASE_URL      = "https://tisvcloud.freeway.gov.tw/history/TDCS"  # 來源根
# 本機 staging 落地根（跟 01 的 extract_to_hdfs.sh、03 的 put_to_hdfs.sh 一致）。
# 分區路徑：STAGING_ROOT/<dtype>/year=YYYY/month=MM/
STAGING_ROOT  = "/home/bigred/wulin/staging/extract"     # 本機下載落地根
LOG_ROOT      = "/home/bigred/wulin/logs"                 # 缺檔對帳 log 目錄（永存）
THROTTLE_SEC  = 45         # 每次 HTTP 請求間隔（>40s，遵守高公局規則）
HTTP_TIMEOUT  = 60         # 單次請求逾時（秒）
HTTP_RETRY    = 3          # 單檔下載失敗重試次數
USER_AGENT    = "pair1-tdcs-fetcher/1.0"

# ------------------------------------------------------------
# 回掃補齊（self-heal）設定
# ------------------------------------------------------------
# 用途：電腦關機數天後，Airflow 只跑 d-5 會漏抓中間幾天。
#       回掃模式會掃描一段窗口，用本機 staging 既有檔數「免費快篩」，
#       只對看起來缺檔的天才去問來源、只補真正缺的檔。
#
# 窗口定義：從「今天 - SCAN_FRESH_OFFSET」往回算 SCAN_WINDOW 天。
#   例：今天 2026-06-25、FRESH=5、WINDOW=10
#       → 掃 d-5(06-20) 一路回到 d-14(06-11)，共 10 天。
#   FRESH=5 的理由：高公局壓縮延遲，太新的日（<d-5）來源可能尚未齊全。
SCAN_FRESH_OFFSET = 5      # 窗口最新端 = 今天 - N 天
SCAN_WINDOW       = 10     # 往回掃幾天（含最新端）

# 「完整天」預期檔數（固定值快篩用）。
#   來源若某天本身就缺檔（< 預期），該天每次回掃都會再問一次來源（可接受的代價）。
EXPECTED_FILES = {
    "M06A": 24,    # 每小時 1 檔
    "M03A": 288,   # 每 5 分 1 檔
}

# ------------------------------------------------------------
# 日期設定（並存方案：這裡是「預設值」，命令列參數可覆蓋它）
# ------------------------------------------------------------
# 規則：
#   1. 若執行時有給命令列參數（--offset-days 或 --start/--end），
#      則「以命令列為準」，完全忽略下面這三個預設值。
#   2. 若執行時「完全沒給」日期參數，才會用下面這三個預設值。
#
# 怎麼改成「抓單天」：
#   - 把 DEFAULT_MODE 設成 "offset"
#   - 改 DEFAULT_OFFSET_DAYS（例如 5 = 抓「今天-5天」那一天）
#   - DEFAULT_START / DEFAULT_END 留著不管它
#   例：今天是 2026-06-16、DEFAULT_OFFSET_DAYS=5 → 抓 2026-06-11 單天
#
# 怎麼改成「抓多天（一段範圍）」：
#   - 把 DEFAULT_MODE 設成 "range"
#   - 改 DEFAULT_START 和 DEFAULT_END（含頭含尾，兩天都會抓）
#   - DEFAULT_OFFSET_DAYS 留著不管它
#   例：DEFAULT_START="2026-05-20"、DEFAULT_END="2026-06-11" → 抓這 23 天
#
DEFAULT_MODE        = "offset"        # "offset"=抓單天 / "range"=抓多天
DEFAULT_OFFSET_DAYS = 5               # offset 模式用：抓「今天 - N 天」那一天
DEFAULT_START       = "2026-05-20"    # range 模式用：起日（含）
DEFAULT_END         = "2026-06-11"    # range 模式用：迄日（含）
# ============================================================

# autoindex 連結解析：抓出 href（時目錄結尾 "/"、或 .csv 檔）
HREF_RE = re.compile(r'href="([^"?][^"]*)"', re.IGNORECASE)


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


class NotFound(RuntimeError):
    """HTTP 404：來源沒有這個路徑。獨立成類別讓補漏能區分「來源本身沒有」
    與「下載失敗」；繼承 RuntimeError 使既有 except RuntimeError 行為不變。"""


def http_get(url: str) -> bytes:
    """單次 HTTP GET，含重試。每次實際發出請求前都會節流。
    404 不重試、立刻拋 NotFound（重試 3 次也不會生出檔案，只浪費 3 次節流）。"""
    last_err = None
    for attempt in range(1, HTTP_RETRY + 1):
        throttle()  # 任何對伺服器的請求都先等 THROTTLE_SEC
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise NotFound(f"404：{url}")
            last_err = e
            log(f"  ! 請求失敗（第 {attempt}/{HTTP_RETRY} 次）：{url} -> {e}")
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            log(f"  ! 請求失敗（第 {attempt}/{HTTP_RETRY} 次）：{url} -> {e}")
    raise RuntimeError(f"下載失敗（已重試 {HTTP_RETRY} 次）：{url} -> {last_err}")


_last_request_ts = 0.0
def throttle() -> None:
    """確保兩次請求間隔 >= THROTTLE_SEC。"""
    global _last_request_ts
    now = time.time()
    wait = THROTTLE_SEC - (now - _last_request_ts)
    if wait > 0:
        log(f"  …節流等待 {wait:.0f}s")
        time.sleep(wait)
    _last_request_ts = time.time()


def list_links(url: str) -> list:
    """讀 autoindex 頁面，回傳該層所有連結（相對名稱）。"""
    html = http_get(url).decode("utf-8", errors="ignore")
    links = []
    for m in HREF_RE.finditer(html):
        href = m.group(1)
        # 過濾上層連結與排序連結
        if href.startswith("/") or href.startswith("..") or href.startswith("?"):
            continue
        links.append(href)
    return links


def list_hour_dirs(dtype: str, yyyymmdd: str) -> list:
    """列出某一天底下實際存在的時目錄（00..23）。缺哪幾時就自動少哪幾時。"""
    day_url = f"{BASE_URL}/{dtype}/{yyyymmdd}/"
    links = list_links(day_url)
    hours = []
    for href in links:
        name = href.strip("/")
        if re.fullmatch(r"\d{1,2}", name):
            hours.append(name)
    return sorted(set(hours), key=int)


def list_csv_files(dtype: str, yyyymmdd: str, hour: str) -> list:
    """
    列出某時目錄下實際存在的 csv 檔名。

    去重原因：高公局 autoindex 的 HTML 裡，同一個檔案的連結常出現多次
    （例如檔名連結 + 前面的 icon 圖示連結，兩者 href 都指向同一檔）。
    若不去重，同一個 csv 會被下載兩次，白白多花一倍流量與節流時間。
    這裡用 dict.fromkeys 去重且保留原始出現順序。
    """
    hour_url = f"{BASE_URL}/{dtype}/{yyyymmdd}/{hour}/"
    links = list_links(hour_url)
    csvs = [h for h in links if h.lower().endswith(".csv")]
    return list(dict.fromkeys(csvs))  # 去重、保序


def local_partition_path(dtype: str, d: dt.date) -> str:
    """本機 staging 分區路徑（跟 extract_to_hdfs.sh / put_to_hdfs.sh 對齊）。"""
    return f"{STAGING_ROOT}/{dtype}/year={d.year:04d}/month={d.month:02d}"


def local_existing_day_files(dtype: str, d: dt.date) -> set:
    """
    回傳本機 staging 中該『單一資料日』已有的「非空」檔名 set（依檔名日期前綴過濾）。

    這是免費操作（不對外部來源發請求、不吃節流），用於冪等/快篩：
    已存在且非空的檔就不重抓。空檔（0 byte）不算,會再抓一次。
    """
    part = local_partition_path(dtype, d)
    prefix = f"TDCS_{dtype}_{d.strftime('%Y%m%d')}_"
    files = set()
    try:
        if os.path.isdir(part):
            for name in os.listdir(part):
                if not name.startswith(prefix) or not name.lower().endswith(".csv"):
                    continue
                fp = os.path.join(part, name)
                if os.path.isfile(fp) and os.path.getsize(fp) > 0:
                    files.add(name)
    except OSError as e:
        log(f"  ! 列本機分區失敗（視為空）：{part} -> {e}")
    return files


def process_one_day(dtype: str, d: dt.date, dry: bool, existing: set = None) -> dict:
    """
    抓取並存到本機 staging 某一天。回傳統計。

    existing：本機 staging 已有的檔名 set。若提供，已存在的檔會直接跳過（不重抓），
              用於冪等 / 回掃補齊只補真正缺的檔。None 表示自動以本機現況計算。
    """
    if existing is None:
        existing = local_existing_day_files(dtype, d)
    yyyymmdd = d.strftime("%Y%m%d")
    part = local_partition_path(dtype, d)
    stat = {"date": yyyymmdd, "found": 0, "saved": 0, "fail": 0, "skipped_hours": 0,
            "skipped_exist": 0, "ok_files": [], "fail_files": []}

    log(f"=== {dtype} {yyyymmdd} 開始 ===")
    log(f"  落地分區（本機 staging）：{part}")

    # 1) 列出實際存在的時目錄（容錯：缺時段自動略過）
    try:
        hours = list_hour_dirs(dtype, yyyymmdd)
    except RuntimeError as e:
        log(f"  ! 無法列出日目錄（可能該日尚未產出）：{e}")
        return stat
    if not hours:
        log("  ! 該日無任何時目錄，略過")
        return stat
    log(f"  發現時目錄：{hours}")

    if not dry:
        os.makedirs(part, exist_ok=True)

    # 2) 逐時、逐檔抓取 → 存本機 staging
    for hour in hours:
        try:
            files = list_csv_files(dtype, yyyymmdd, hour)
        except RuntimeError as e:
            log(f"  ! 時目錄 {hour} 列檔失敗，略過：{e}")
            stat["skipped_hours"] += 1
            continue
        if not files:
            log(f"  - {hour}: 無 csv，略過")
            continue
        log(f"  - {hour}: {len(files)} 檔")
        for fname in files:
            stat["found"] += 1
            # 冪等/回掃補齊：本機 staging 已有就跳過，不重抓（省節流時間）
            if fname in existing:
                stat["skipped_exist"] += 1
                continue
            file_url = f"{BASE_URL}/{dtype}/{yyyymmdd}/{hour}/{fname}"
            local_file = os.path.join(part, fname)
            try:
                data = http_get(file_url)
                if not data:
                    log(f"    ! 空檔，略過：{fname}")
                    stat["fail"] += 1
                    stat["fail_files"].append(f"{hour}/{fname} (空檔)")
                    continue
                if dry:
                    log(f"    (dry) 將下載並存本機：{fname} ({len(data)} bytes)")
                else:
                    with open(local_file, "wb") as f:
                        f.write(data)
                stat["saved"] += 1
                stat["ok_files"].append(f"{hour}/{fname}")
            except RuntimeError as e:
                log(f"    ! 下載失敗：{fname} -> {e}")
                stat["fail"] += 1
                stat["fail_files"].append(f"{hour}/{fname} (下載失敗)")
                continue

    # 3) 寫缺檔對帳 log（當天若有缺檔，留紀錄供事後查）
    write_reconcile_log(dtype, d, stat, dry)

    log(f"=== {dtype} {yyyymmdd} 完成：found={stat['found']} "
        f"saved={stat['saved']} fail={stat['fail']} skipped_exist={stat['skipped_exist']} "
        f"skipped_hours={stat['skipped_hours']} ===")
    return stat


def fetch_specific_files(dtype: str, d: dt.date, filenames: list, dry: bool = False,
                         hour_listing_cache: dict = None) -> dict:
    """
    只下載「指定檔名清單」到本機 staging（檔級補漏用，由 tdcs_backfill_missing.py 呼叫）。

    跟 process_one_day 的差別：不列日目錄、不列時目錄——檔名規律固定
    （M06A=_HH0000、M03A=_HHMM00，時目錄=補零兩位數），直接組 URL 下載，
    缺 k 個檔就只花 k 次節流請求，不用整天重抓。

    404 fallback：直抓拿到 404 時，列一次該時目錄確認（結果快取避免重列）——
      目錄裡真的沒有 → 視為「來源本身無此檔」（source_missing，不算失敗）；
      其他情況 → 算 fail（讓上游決定要不要讓 task 失敗）。

    dry=True 完全不發任何請求，只列印會抓什麼。
    回傳統計 dict：saved / fail / source_missing / skipped_exist / fail_files / missing_files
    """
    yyyymmdd = d.strftime("%Y%m%d")
    part = local_partition_path(dtype, d)
    stat = {"saved": 0, "fail": 0, "source_missing": 0, "skipped_exist": 0,
            "fail_files": [], "missing_files": []}
    cache = hour_listing_cache if hour_listing_cache is not None else {}
    existing = local_existing_day_files(dtype, d)
    fname_re = re.compile(rf"TDCS_{dtype}_{yyyymmdd}_(\d{{2}})\d{{4}}\.csv")

    if not dry:
        os.makedirs(part, exist_ok=True)

    for fname in filenames:
        if fname in existing:
            stat["skipped_exist"] += 1
            continue
        m = fname_re.fullmatch(fname)
        if not m:
            log(f"  ! 檔名不符規律，略過：{fname}")
            stat["fail"] += 1
            stat["fail_files"].append(fname)
            continue
        hour = m.group(1)
        url = f"{BASE_URL}/{dtype}/{yyyymmdd}/{hour}/{fname}"
        if dry:
            log(f"  (dry) 將直抓：{url}")
            stat["saved"] += 1
            continue
        try:
            data = http_get(url)
        except NotFound:
            # 直抓 404 → 列一次該時目錄確認來源是否真的沒有（同時段結果快取）
            key = (dtype, yyyymmdd, hour)
            if key not in cache:
                try:
                    cache[key] = set(list_csv_files(dtype, yyyymmdd, hour))
                except NotFound:
                    cache[key] = set()   # 整個時目錄都不存在 → 來源就是沒有
                except RuntimeError:
                    cache[key] = None    # 列目錄失敗（網路等）→ 無法確認
            listing = cache[key]
            if listing is not None and fname not in listing:
                log(f"  ∅ 來源本身無此檔（已列目錄確認）：{fname}")
                stat["source_missing"] += 1
                stat["missing_files"].append(fname)
            else:
                log(f"  ! 直抓 404 但無法確認來源狀態：{fname}")
                stat["fail"] += 1
                stat["fail_files"].append(fname)
            continue
        except RuntimeError as e:
            log(f"  ! 下載失敗：{fname} -> {e}")
            stat["fail"] += 1
            stat["fail_files"].append(fname)
            continue
        if not data:
            log(f"  ! 空檔，略過：{fname}")
            stat["fail"] += 1
            stat["fail_files"].append(fname)
            continue
        with open(os.path.join(part, fname), "wb") as f:
            f.write(data)
        stat["saved"] += 1
    return stat


def write_reconcile_log(dtype: str, d: dt.date, stat: dict, dry: bool) -> None:
    """
    寫當日對帳 log 到 ~/wulin/logs/。

    內容：來源發現幾檔(found) vs 成功存本機幾檔(saved) vs 失敗幾檔(fail)，
    並列出失敗的具體檔名，方便事後人工補抓。
    檔名：reconcile_<dtype>_<YYYYMMDD>.log（同日重跑會覆蓋，反映最新狀態）。

    注意：這只對帳「當天有跑時的缺檔」。整天沒跑（server 關機）由 Airflow
    catchup / 回掃補。上 HDFS 的對帳不在本支（本支不 put）。
    """
    yyyymmdd = d.strftime("%Y%m%d")
    status = "OK" if stat["fail"] == 0 and stat["found"] > 0 else "INCOMPLETE"
    if stat["found"] == 0:
        status = "NO_SOURCE"  # 來源該日無資料（可能尚未產出 / 日期太新）

    lines = []
    lines.append(f"# 對帳報告 {dtype} {yyyymmdd}")
    lines.append(f"# 產生時間: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"狀態: {status}")
    lines.append(f"來源發現檔數 (found): {stat['found']}")
    lines.append(f"成功存本機 (saved):   {stat['saved']}")
    lines.append(f"失敗 (fail):          {stat['fail']}")
    lines.append(f"列檔失敗時段數:        {stat['skipped_hours']}")
    if stat["fail_files"]:
        lines.append("")
        lines.append("# 失敗檔案清單（需補抓）：")
        for ff in stat["fail_files"]:
            lines.append(f"  - {ff}")
    content = "\n".join(lines) + "\n"

    if dry:
        log(f"  (dry) 將寫對帳 log：狀態={status} found={stat['found']} "
            f"saved={stat['saved']} fail={stat['fail']}")
        return

    os.makedirs(LOG_ROOT, exist_ok=True)
    log_path = os.path.join(LOG_ROOT, f"reconcile_{dtype}_{yyyymmdd}.log")
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(content)
    if status != "OK":
        log(f"  ⚠️ 對帳：{status}（found={stat['found']} saved={stat['saved']} "
            f"fail={stat['fail']}）→ 詳見 {log_path}")
    else:
        log(f"  ✓ 對帳 OK（{stat['saved']} 檔全數存本機）→ {log_path}")


def daterange(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def parse_args():
    p = argparse.ArgumentParser(description="TDCS 每日 CSV 抓取到本機 staging（不含 put）")
    p.add_argument("--dtype", choices=["M03A", "M06A"], default=None,
                   help="只抓指定 type；不給則兩個都抓（先 M06A 再 M03A）。")
    # 兩種模式擇一；若都不給，則回退用檔案開頭 CONFIG 的 DEFAULT_* 預設值
    p.add_argument("--offset-days", type=int, default=None,
                   help="抓「今天-N 天」單日（覆蓋 CONFIG）。Airflow 排程用，建議 5。")
    p.add_argument("--start", type=str, default=None,
                   help="範圍起 YYYY-MM-DD（需與 --end 成對，覆蓋 CONFIG）")
    p.add_argument("--end", type=str, default=None,
                   help="範圍迄 YYYY-MM-DD（需與 --start 成對，覆蓋 CONFIG）")
    p.add_argument("--dry-run", action="store_true", help="只印不下載")
    # 回掃補齊模式（self-heal）
    p.add_argument("--backfill-scan", action="store_true",
                   help="回掃補齊模式：掃描窗口、本機 staging 快篩、只補缺檔（建議 Airflow 用這個）")
    p.add_argument("--scan-window", type=int, default=None,
                   help=f"回掃幾天（預設 CONFIG SCAN_WINDOW={SCAN_WINDOW}）")
    p.add_argument("--scan-fresh-offset", type=int, default=None,
                   help=f"窗口最新端=今天-N（預設 CONFIG SCAN_FRESH_OFFSET={SCAN_FRESH_OFFSET}）")
    return p.parse_args()


def resolve_date_range(a) -> tuple:
    """
    決定要抓的日期區間（並存邏輯）。

    優先序：
      1. 命令列有給 --offset-days       → 抓「今天 - N 天」單日（忽略 CONFIG）
      2. 命令列有給 --start 且 --end    → 抓該範圍（忽略 CONFIG）
      3. 命令列什麼日期都沒給           → 用 CONFIG 區塊的 DEFAULT_* 預設值

    回傳 (start_date, end_date)，含頭含尾。
    """
    # --- 情況 1/2：命令列有給，命令列優先 ---
    if a.offset_days is not None:
        target = dt.date.today() - dt.timedelta(days=a.offset_days)
        return target, target
    if a.start and a.end:
        s = dt.datetime.strptime(a.start, "%Y-%m-%d").date()
        e = dt.datetime.strptime(a.end, "%Y-%m-%d").date()
        return s, e
    if (a.start and not a.end) or (a.end and not a.start):
        log("! --start 與 --end 必須成對給；只給一個無效")
        sys.exit(2)

    # --- 情況 3：命令列沒給，回退用 CONFIG 預設 ---
    if DEFAULT_MODE == "offset":
        target = dt.date.today() - dt.timedelta(days=DEFAULT_OFFSET_DAYS)
        return target, target
    elif DEFAULT_MODE == "range":
        s = dt.datetime.strptime(DEFAULT_START, "%Y-%m-%d").date()
        e = dt.datetime.strptime(DEFAULT_END, "%Y-%m-%d").date()
        return s, e
    else:
        log(f"! CONFIG 的 DEFAULT_MODE 不合法：{DEFAULT_MODE}（須為 offset 或 range）")
        sys.exit(2)


def scan_backfill(dtypes: list, fresh_offset: int, window: int, dry: bool) -> dict:
    """
    回掃補齊（self-heal）。

    窗口：從 (今天 - fresh_offset) 往回 window 天。
    流程（每天、每 type）：
      1. 本機 staging 免費快篩：數該天已有非空檔數。
         - >= 預期(EXPECTED_FILES) → 視為完整，直接跳過（不問來源）。
         - <  預期 → 問來源，只補本機 staging 還沒有的檔。
    """
    today = dt.date.today()
    newest = today - dt.timedelta(days=fresh_offset)
    days = [newest - dt.timedelta(days=i) for i in range(window)]  # 由新到舊
    oldest = days[-1]

    log(f"========== 回掃補齊：{oldest} ~ {newest}（{window} 天）"
        f" dtypes={dtypes} dry_run={dry} ==========")

    grand = {"found": 0, "saved": 0, "fail": 0, "skipped_exist": 0,
             "complete_days": 0, "fetched_days": 0}

    for dtype in dtypes:
        expected = EXPECTED_FILES.get(dtype, 0)
        log(f"########## 回掃 {dtype}（每完整天預期 {expected} 檔）##########")
        for d in days:
            existing = local_existing_day_files(dtype, d)
            have = len(existing)
            if expected > 0 and have >= expected:
                log(f"  ✓ {dtype} {d.strftime('%Y%m%d')}：本機已有 {have}/{expected}，完整，跳過")
                grand["complete_days"] += 1
                continue
            log(f"  → {dtype} {d.strftime('%Y%m%d')}：本機僅 {have}/{expected}，"
                f"問來源補缺")
            grand["fetched_days"] += 1
            s = process_one_day(dtype, d, dry, existing=existing)
            for k in ("found", "saved", "fail", "skipped_exist"):
                grand[k] += s[k]

    log(f"========== 回掃完成：完整跳過 {grand['complete_days']} 天、"
        f"需補 {grand['fetched_days']} 天｜"
        f"found={grand['found']} saved={grand['saved']} "
        f"skipped_exist={grand['skipped_exist']} fail={grand['fail']} ==========")
    return grand


def main():
    a = parse_args()

    # 決定要抓哪些 type：給了 --dtype 就只抓那個；沒給就兩個都抓（先 M06A 再 M03A）
    dtypes = [a.dtype] if a.dtype else ["M06A", "M03A"]

    # === 回掃補齊模式：優先於單日/範圍模式 ===
    if a.backfill_scan:
        window = a.scan_window if a.scan_window is not None else SCAN_WINDOW
        fresh = a.scan_fresh_offset if a.scan_fresh_offset is not None else SCAN_FRESH_OFFSET
        g = scan_backfill(dtypes, fresh, window, a.dry_run)
        sys.exit(1 if g["fail"] > 0 else 0)

    start, end = resolve_date_range(a)
    if start > end:
        log(f"! 起日 {start} 晚於迄日 {end}，結束")
        sys.exit(2)

    log(f"啟動：dtypes={dtypes} 範圍={start}~{end} dry_run={a.dry_run} "
        f"throttle={THROTTLE_SEC}s staging={STAGING_ROOT}")

    grand = {"found": 0, "saved": 0, "fail": 0}
    for dtype in dtypes:
        log(f"########## 開始處理 {dtype} ##########")
        sub = {"found": 0, "saved": 0, "fail": 0}
        for d in daterange(start, end):
            s = process_one_day(dtype, d, a.dry_run)
            for k in sub:
                sub[k] += s[k]
        log(f"########## {dtype} 小計：found={sub['found']} "
            f"saved={sub['saved']} fail={sub['fail']} ##########")
        for k in grand:
            grand[k] += sub[k]

    log(f"全部完成：found={grand['found']} saved={grand['saved']} fail={grand['fail']}")
    log("下一步：對每個 year/month 分區執行 03_shared/put_to_hdfs.sh <type> <year> <month>")
    # 有任何失敗回非零，讓 Airflow 能標記 task 失敗
    sys.exit(1 if grand["fail"] > 0 else 0)


if __name__ == "__main__":
    main()
