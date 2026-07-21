#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tdcs_backfill_missing.py
========================
職責範圍：以 HDFS 為準的「檔級」補漏。快照 HDFS 現有檔名 → 跟預期檔名差集 →
          精確算出缺哪幾個檔 → 只直抓那幾個檔 → 增量 put 上 HDFS。

兩種用法（同一套邏輯，只差預設值）：
  1) Airflow 每日自動（DAG 第三個 task verify_and_heal）：
       python3 -u tdcs_backfill_missing.py --auto
     窗口 D-5 往回 20 天、單次補檔上限 480（防止巨大缺口讓 task 跑不完；
     窗口每天滾動，缺口會在幾天內自動收斂補完；20 天窗口 = 連續關機容忍約 19 天）。
  2) 手動深掃（窗口外的深層歷史、災難後總檢查）：
       python3 -u tdcs_backfill_missing.py                # 掃近 30 天，全部補
       python3 -u tdcs_backfill_missing.py --check-only   # 只列缺什麼，不動
       python3 -u tdcs_backfill_missing.py --days 60      # 自訂窗口

為什麼快（對比舊版）：
  * 舊版逐日 hdfs dfs -ls（30 天×2 type＝60 次 JVM 啟動≈3-5 分鐘）；
    新版每個 type 一次 -ls 帶入窗口涵蓋的所有月分區 → 2 次呼叫、十幾秒。
  * 舊版缺 1 檔就整天重抓（列目錄 25 請求＋整天檔案×45s 節流≈數小時）；
    新版檔名規律固定（M06A=_HH0000、M03A=_HHMM00）→ 本地生成預期集合，
    差集得到精確缺檔清單，直接組 URL 只抓缺的檔：缺 k 檔 ≈ k×45s。

失敗語意（--auto 與手動相同）：
  * 來源本身無此檔（直抓 404 且列目錄確認沒有）→ 記錄、不算失敗（exit 0），
    避免高公局永久缺檔讓 DAG 天天紅。窗口滾動，過了窗口就不再重問。
  * 下載失敗 / put 失敗 → exit 1（Airflow 標紅、觸發 retry）。
  * HDFS 快照失敗（hdfs 指令壞掉、連不上 NameNode）→ exit 2，且「不會」
    把整個窗口誤判為全缺（安全閥：分不清「分區不存在」與「環境壞掉」時直接放棄）。

節流與並發：
  * 下載沿用 tdcs_daily_fetch.py 的節流（>40s 規則）。
  * --auto 跑在主 DAG 的 fetch/put 下游（同 DAG 串行、max_active_runs=1），
    不會跟主 DAG 的抓取並發、不搶來源流量 —— 這是能自動化的關鍵前提。
  * 手動深掃仍請挑主 DAG 沒在跑的時段執行。

銜接對象：
  * import 同目錄 tdcs_daily_fetch.py 的 fetch_specific_files()（直抓指定檔名）。
  * put 用 03_shared/put_to_hdfs.sh（已改為增量 put -f、不先刪當天整批，
    所以「staging 只有補回來的那幾個檔」也安全，不會蓋掉 HDFS 上原有的檔）。
"""

import argparse
import datetime as dt
import os
import subprocess
import sys

# ============================================================
# CONFIG
# ============================================================
HDFS_RAW_ROOT = "/raw"

# 下游的位置（dtadm 佈局：automation 放 fetch、shared 放 put）。
# 用相對本檔的路徑推算，換機器不用改；換佈局才動這兩行。
_THIS_DIR  = os.path.dirname(os.path.abspath(__file__))
PUT_SCRIPT = os.path.normpath(os.path.join(_THIS_DIR, "..", "shared", "put_to_hdfs.sh"))

OFFSET_DAYS = 5      # 掃描最新邊界 = 今天 - N（對齊來源壓縮延遲）
SCAN_DAYS_MANUAL = 30    # 手動模式預設窗口
SCAN_DAYS_AUTO   = 20    # --auto（DAG）預設窗口：D-5 ~ D-24（連續關機容忍 ~19 天）
                         # 刻意 ≤~20：再寬會伸進「已壓成 tar.gz」的區域（約 35 天前），
                         # 那些日子逐檔永遠抓不到、每天空重試；更舊的走 tar.gz 歷史流程。
MAX_FILES_AUTO   = 480   # --auto 單次補檔上限（480×45s ≈ 6 小時，窗口滾動會收斂）

# 檔名規律：TDCS_<DTYPE>_YYYYMMDD_HHMM00.csv，分鐘間隔依 type
FILE_MINUTE_STEP = {
    "M06A": 60,   # 每小時 1 檔（_HH0000）→ 24 檔/日
    "M03A": 5,    # 每 5 分 1 檔（_HHMM00）→ 288 檔/日
}
# ============================================================

# 同目錄 import 抓取器（dtadm 上兩支都在 ~/wulin/automation/）
sys.path.insert(0, _THIS_DIR)
import tdcs_daily_fetch as fetcher  # noqa: E402


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def expected_filenames(dtype: str, d: dt.date) -> set:
    """本地生成某天「應有」的完整檔名集合（不需問來源）。"""
    ymd = d.strftime("%Y%m%d")
    step = FILE_MINUTE_STEP[dtype]
    return {
        f"TDCS_{dtype}_{ymd}_{hh:02d}{mm:02d}00.csv"
        for hh in range(24)
        for mm in range(0, 60, step)
    }


def expected_count(dtype: str) -> int:
    """某 type 一天應有幾檔（M06A=24、M03A=288）。"""
    return 24 * (60 // FILE_MINUTE_STEP[dtype])


def hdfs_snapshot(dtype: str, days: list) -> set:
    """
    一次 hdfs dfs -ls 拿到窗口涵蓋月分區的所有現有檔名（單次 JVM）。

    安全閥：分區不存在（No such file...）是正常的（該月還沒資料）；
    但若出現其他錯誤（連不上 NameNode、指令壞掉），拋 RuntimeError 讓上游
    exit 2 —— 絕不能把「查不到」當成「全缺」，否則會誤觸整窗口重抓。
    """
    parts = sorted({
        f"{HDFS_RAW_ROOT}/{dtype}/year={d.year:04d}/month={d.month:02d}"
        for d in days
    })
    try:
        out = subprocess.run(["hdfs", "dfs", "-ls"] + parts,
                             capture_output=True, text=True)
    except FileNotFoundError as e:
        raise RuntimeError(f"hdfs 指令不存在：{e}")

    if out.returncode != 0:
        bad = [ln for ln in out.stderr.splitlines()
               if ln.strip() and "No such file or directory" not in ln]
        if bad:
            raise RuntimeError("hdfs -ls 非預期錯誤：" + " | ".join(bad[:3]))
        # 只有「分區不存在」→ 正常，繼續解析已存在分區的輸出

    names = set()
    prefix = f"TDCS_{dtype}_"
    for line in out.stdout.splitlines():
        cols = line.split()
        if cols and cols[-1].startswith("/"):
            name = os.path.basename(cols[-1])
            if name.startswith(prefix) and name.lower().endswith(".csv"):
                names.add(name)
    return names


def scan_missing_files(dtypes: list, days: list) -> dict:
    """
    回傳 {(dtype, date): [缺檔名…（排序）]}，只含真的有缺的天。
    每個 dtype 只打一次 HDFS；差集為純本地運算。
    """
    missing = {}
    for dtype in dtypes:
        per_day = len(expected_filenames(dtype, days[0]))
        snapshot = hdfs_snapshot(dtype, days)
        log(f"########## 檢查 {dtype}：HDFS 快照 {len(snapshot)} 檔"
            f"（每完整天應有 {per_day} 檔）##########")
        for d in days:
            lack = sorted(expected_filenames(dtype, d) - snapshot)
            if not lack:
                log(f"  ✓ {dtype} {d.strftime('%Y%m%d')}：完整")
                continue
            head = "、".join(n.rsplit("_", 1)[-1][:4] for n in lack[:8])
            more = f" …等 {len(lack)} 檔" if len(lack) > 8 else ""
            log(f"  ✗ {dtype} {d.strftime('%Y%m%d')}：HDFS 現有 {per_day - len(lack)}/{per_day}"
                f" → 缺 {len(lack)} 檔（時分 {head}{more}）")
            missing[(dtype, d)] = lack
    return missing


def run(cmd: list) -> int:
    log("  $ " + " ".join(cmd))
    return subprocess.run(cmd).returncode


def main():
    p = argparse.ArgumentParser(
        description="TDCS 檔級補漏：以 HDFS 為準，精確補缺的檔（DAG --auto / 手動深掃共用）")
    p.add_argument("--auto", action="store_true",
                   help=f"Airflow 自動模式：窗口預設 {SCAN_DAYS_AUTO} 天、"
                        f"單次補檔上限 {MAX_FILES_AUTO}")
    p.add_argument("--dtype", choices=["M03A", "M06A"], default=None,
                   help="只處理指定 type；不給則兩個都處理（先 M06A 再 M03A）。")
    p.add_argument("--days", type=int, default=None,
                   help=f"往回掃幾天（預設：手動 {SCAN_DAYS_MANUAL}、--auto {SCAN_DAYS_AUTO}）")
    p.add_argument("--offset-days", type=int, default=OFFSET_DAYS,
                   help=f"掃描最新邊界=今天-N（預設 {OFFSET_DAYS}，對齊壓縮延遲）")
    p.add_argument("--max-files", type=int, default=None,
                   help=f"單次最多補幾個檔（預設：手動不限、--auto {MAX_FILES_AUTO}；"
                        "超出部分留給下次，窗口滾動會收斂）")
    p.add_argument("--check-only", action="store_true",
                   help="只報告缺哪些檔、不下載不上傳")
    p.add_argument("--dry-run", action="store_true",
                   help="列出會直抓的 URL 與會跑的 put，不實際執行")
    a = p.parse_args()

    dtypes = [a.dtype] if a.dtype else ["M06A", "M03A"]
    n_days = a.days if a.days is not None else (SCAN_DAYS_AUTO if a.auto else SCAN_DAYS_MANUAL)
    budget = a.max_files if a.max_files is not None else (MAX_FILES_AUTO if a.auto else None)

    today = dt.date.today()
    newest = today - dt.timedelta(days=a.offset_days)
    days = [newest - dt.timedelta(days=i) for i in range(n_days)]  # 由新到舊
    oldest = days[-1]

    log(f"========== 補漏檢查：{oldest} ~ {newest}（{n_days} 天） dtypes={dtypes}"
        f" auto={a.auto} check_only={a.check_only} dry_run={a.dry_run}"
        f" max_files={budget} ==========")

    # --- 1) 快照 + 差集（快：每 type 一次 hdfs 呼叫）---
    try:
        missing = scan_missing_files(dtypes, days)
    except RuntimeError as e:
        log(f"!! HDFS 快照失敗，中止（不誤判為全缺）：{e}")
        sys.exit(2)

    total_lack = sum(len(v) for v in missing.values())
    if not missing:
        log("========== 全部完整，無需補 ==========")
        sys.exit(0)
    est_h = total_lack * fetcher.THROTTLE_SEC / 3600.0
    log(f"========== 缺 {len(missing)} 天、共 {total_lack} 檔"
        f"（逐檔直抓預估 ≈ {est_h:.1f} 小時節流時間）==========")

    # 整天全缺且已進入 tar.gz 壓縮區（約 35 天前）的日子 → 逐檔直抓是錯的工具：
    # 一天要 288 次請求，且未壓縮區可能已下架;tar.gz 一天一包、一次請求搞定。
    tar_cut = today - dt.timedelta(days=35)
    old_full = sorted(k[1].strftime("%Y%m%d") + " " + k[0] for k, v in missing.items()
                      if len(v) == expected_count(k[0]) and k[1] < tar_cut)
    if old_full:
        log(f"  ⚠️ 其中 {len(old_full)} 天「整天全缺」且早於 {tar_cut}（來源已壓成 tar.gz）"
            "→ 這些天請改走 01_historical_backfill（downloader → extract → put），"
            "不要用本支逐檔直抓。")

    if a.check_only:
        log("========== --check-only：只報告不補，結束 ==========")
        sys.exit(0)

    # --- 2) 檔級補抓：抓一天 → 立刻 put 一天（新→舊）---
    #   為什麼「抓完立刻 put」而非全部抓完才 put：這批動輒上萬檔、要跑好幾天，
    #   dtadm 又每晚關機。若拖到最後才 put，中途一重啟就整批沒落地、下次掃 HDFS
    #   還是全缺 → 從頭再抓，永遠等不到能一次跑完的機會。逐天 put 後，每抓完一天
    #   就 durable 進 HDFS，下次掃描直接跳過該天，關機也只損失「當天未抓完」的部分。
    totals = {"saved": 0, "fail": 0, "source_missing": 0, "skipped_exist": 0}
    put_fail = 0
    left_over = 0
    for dtype in dtypes:
        hour_cache = {}      # 404 fallback 的時目錄列表快取（同 type 共用）
        for d in days:       # 已由新到舊
            lack = missing.get((dtype, d))
            if not lack:
                continue
            if budget is not None and budget <= 0:
                left_over += len(lack)
                continue
            take = lack if budget is None else lack[:budget]
            if budget is not None:
                budget -= len(take)
                left_over += len(lack) - len(take)
            log(f"--- 補 {dtype} {d.strftime('%Y%m%d')}：{len(take)} 檔（直抓）---")
            stat = fetcher.fetch_specific_files(dtype, d, take, dry=a.dry_run,
                                                hour_listing_cache=hour_cache)
            for k in totals:
                totals[k] += stat[k]
            # 這天在 staging 有資料就立刻 put（saved=這輪新抓；skipped_exist=前一輪
            # 已抓進 staging 還沒 put，例如中途重啟續跑）。put -f 增量、不動既有檔。
            if stat["saved"] > 0 or stat["skipped_exist"] > 0:
                if a.dry_run:
                    log(f"  (dry) 將執行：bash {PUT_SCRIPT} {dtype} {d.year:04d} {d.month:02d}")
                else:
                    log(f"--- put {dtype} {d:%Y%m%d} → HDFS ---")
                    rc = run(["bash", PUT_SCRIPT, dtype, f"{d.year:04d}", f"{d.month:02d}"])
                    if rc != 0:
                        log(f"  ! put 失敗（rc={rc}）：{dtype} {d:%Y%m%d}")
                        put_fail += 1

    if left_over:
        log(f"  ⚠️ 上限已滿，剩 {left_over} 檔本次未補（窗口滾動，下次會繼續；"
            "急的話手動跑本支不帶 --auto）")

    log(f"========== 補漏完成：saved={totals['saved']} fail={totals['fail']}"
        f" source_missing={totals['source_missing']}"
        f" skipped_exist={totals['skipped_exist']} put_fail={put_fail}"
        f" left_over={left_over} ==========")
    if totals["source_missing"]:
        log(f"  ∅ 來源本身缺 {totals['source_missing']} 檔（已確認、不算失敗；"
            "窗口滾出後不再重問）")

    # 來源缺檔不算失敗；只有下載失敗 / put 失敗才回非零讓 Airflow 標紅
    sys.exit(1 if (totals["fail"] or put_fail) else 0)


if __name__ == "__main__":
    main()
