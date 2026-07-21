# -*- coding: utf-8 -*-
"""
convert_batch.py — TDCS 原始 CSV → /dataset/ 純 Parquet（M03A / M06A 共用，自我修復）
==============================================================================
2026-07-17 起改回直接寫 /dataset/（取代先前的 Iceberg 版；原版備份
convert_batch.py.bak-iceberg-20260717）。網站（pair2api）只讀 /dataset/ 的
Hive 目錄結構，本支輸出跟既有結構完全一致：

  M03A（日分割）：/dataset/M03A/year=YYYY/month=MM/day=DD/*.parquet
  M06A（月分割）：/dataset/M06A/year=YYYY/month=MM/*.parquet

檔內只有原始資料欄位（不塞 year/month/day 欄位，讀取時由目錄路徑推斷——
既有檔案就是這樣，塞了反而跟 basePath 分區欄位相撞）。schema 對齊
/dataset 既有 2026 年檔案實測：M03A 的 VehicleType/Volume=int；
M06A 的 VehicleType=int、TripLength=double。（注意：M06A 2024-2025 的
歷史上傳版是全字串 schema，跨 2025/2026 邊界查詢本來就有型別衝突，
與本支無關、也不要在這裡「修」——那要整批重寫歷史檔才解得掉。）

兩種用法（CLI 跟 Iceberg 版相同，DAG 不用改）：
  1) Airflow 每日自動（DAG 第四個 task convert_parquet）：
       spark-submit [executor 參數] convert_batch.py M06A --auto
       spark-submit [executor 參數] convert_batch.py M03A --auto
     掃 D-5~D-24（跟 raw 的 verify_and_heal 同窗口）。
  2) 手動補轉一段歷史（例如剛把 raw 大批補回來後）：
       spark-submit [executor 參數] convert_batch.py M03A --start 2026-06-01 --end 2026-06-30
     已轉過的自動跳過；強制重轉加 --force。

冪等判斷：
  M03A 逐日三態（跟 put_to_hdfs.sh 同精神）：
    raw 無檔                     → 跳過（raw 還沒到）
    raw < 288 且非 --force       → 跳過（等 verify_and_heal 補齊再轉）
    day 目錄已存在 且非 --force  → 跳過（已轉過）
    其餘                         → 轉換（寫 tmp、驗筆數、rename 換入）
  M06A 逐月（月分割沒辦法只換一天，用「raw 檔數戳記」判斷新舊）：
    轉完在月目錄放一個空檔 _raw_count=<N>（_ 開頭，Spark 讀取自動忽略），
    N = 當時 raw 該月的 CSV 檔數。下次跑先比對現在的 raw 檔數：
      相同 → 跳過（沒新東西）
      不同 → 整月重轉（heal 補了舊檔，或當月又長出新的日子）
      月目錄存在但沒戳記（歷史上傳版/手動轉的）→ 狀態不明，重轉一次補上戳記
    當月每天 raw 會一直長，所以整個當月每天重轉一次是預期行為。

寫入安全：先寫到 /dataset/<DTYPE>/.tmp_convert/ 下、驗完筆數才 rename
換入正式路徑（api 隨時在服務查詢，不能讓它讀到寫一半的目錄）。
每次啟動先清掉 .tmp_convert 殘骸（上次中斷留下的）；本支靠 DAG 的
max_active_runs=1 保證不並發，手動跑請避開 DAG 的 convert 時段。
"""
import argparse
import datetime as dt
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType,
                               IntegerType, DoubleType)

DATASET_BASE = "hdfs:///dataset"
RAW_BASE     = "hdfs:///raw"

# ============================================================
# 每個 DTYPE 的差異集中在此；要新增類型只加一筆
# ============================================================
CONFIGS = {
    "M03A": {
        "schema": StructType([
            StructField("TimeInterval", StringType()),
            StructField("GantryID",     StringType()),
            StructField("Direction",    StringType()),
            StructField("VehicleType",  IntegerType()),
            StructField("Volume",       IntegerType()),
        ]),
        "granularity": "day",       # /dataset/M03A/year=/month=/day=/
        "expected": 288,            # 每 5 分 1 檔
        "files_per_unit": 1,        # 一天約 1MB，單檔剛好
        "shuffle_partitions": "20",
    },
    "M06A": {
        "schema": StructType([
            StructField("VehicleType",      IntegerType()),
            StructField("DetectionTime_O",  StringType()),
            StructField("GantryID_O",       StringType()),
            StructField("DetectionTime_D",  StringType()),
            StructField("GantryID_D",       StringType()),
            StructField("TripLength",       DoubleType()),
            StructField("TripEnd",          StringType()),
            StructField("TripInformation",  StringType()),   # 保留，不清洗
        ]),
        "granularity": "month",     # /dataset/M06A/year=/month=/
        "expected": 24,             # 每小時 1 檔（判斷單日完整用不到，留參考）
        "files_per_unit": 1,        # 跟既有月份一致：月內單檔
        "shuffle_partitions": "10",
    },
}

OFFSET_DAYS_AUTO = 5    # --auto 窗口最新端 = 今天 - N（對齊來源壓縮延遲）
SCAN_DAYS_AUTO   = 20   # --auto 往回掃幾天（對齊 raw 的 verify_and_heal 窗口）


def log(msg):
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


# ---------------- Hadoop FS 小工具（globStatus 不進 Spark，很快） ----------------

def _fs(spark):
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    return fs, jvm


def glob_count(spark, pattern):
    fs, jvm = _fs(spark)
    st = fs.globStatus(jvm.org.apache.hadoop.fs.Path(pattern))
    return 0 if st is None else len(st)


def path_exists(spark, path):
    fs, jvm = _fs(spark)
    return fs.exists(jvm.org.apache.hadoop.fs.Path(path))


def delete_path(spark, path):
    fs, jvm = _fs(spark)
    fs.delete(jvm.org.apache.hadoop.fs.Path(path), True)


def swap_into(spark, tmp, dst):
    """tmp 目錄換入正式路徑：確保父目錄在 → 刪舊 dst → rename。"""
    fs, jvm = _fs(spark)
    Path = jvm.org.apache.hadoop.fs.Path
    fs.mkdirs(Path(dst).getParent())
    if fs.exists(Path(dst)):
        fs.delete(Path(dst), True)
    if not fs.rename(Path(tmp), Path(dst)):
        raise RuntimeError(f"rename 失敗：{tmp} → {dst}")


# ---------------- M06A 的 raw 檔數戳記（空檔 _raw_count=<N>） ----------------

def read_marker(spark, month_dir):
    """回傳戳記數字；沒有戳記回傳 None。"""
    fs, jvm = _fs(spark)
    st = fs.globStatus(jvm.org.apache.hadoop.fs.Path(f"{month_dir}/_raw_count=*"))
    if not st:
        return None
    name = st[0].getPath().getName()
    try:
        return int(name.split("=", 1)[1])
    except ValueError:
        return None


def write_marker(spark, month_dir, n):
    fs, jvm = _fs(spark)
    Path = jvm.org.apache.hadoop.fs.Path
    for old in (fs.globStatus(Path(f"{month_dir}/_raw_count=*")) or []):
        fs.delete(old.getPath(), False)
    fs.create(Path(f"{month_dir}/_raw_count={n}"), True).close()


# ---------------- 轉換共用核心：csv → tmp → 驗筆數 → 換入 ----------------

def convert_unit(spark, dtype, cfg, src_glob, dst, tag):
    """轉一個單位（M03A 一天 / M06A 一月）。回傳 True=成功。"""
    tmp = f"{DATASET_BASE}/{dtype}/.tmp_convert/{tag}-{os.getpid()}"
    df = spark.read.option("header", False).schema(cfg["schema"]).csv(src_glob)
    src_count = df.count()
    df.coalesce(cfg["files_per_unit"]).write.mode("overwrite").parquet(tmp)
    dst_count = spark.read.parquet(tmp).count()
    if src_count != dst_count:
        log(f"  ! {tag}：筆數不符 CSV {src_count} / parquet {dst_count}，不換入")
        delete_path(spark, tmp)
        return False
    swap_into(spark, tmp, dst)
    log(f"  ✓ {tag}：完成（{src_count} 筆）")
    return True


# ---------------- M03A：逐日 ----------------

def run_m03a(spark, cfg, dates, force):
    conv = skip = fail = 0
    for d in dates:
        tag = f"M03A {d:%Y%m%d}"
        src = (f"{RAW_BASE}/M03A/year={d.year}/month={d.month:02d}/"
               f"TDCS_M03A_{d:%Y%m%d}_*.csv")
        dst = (f"{DATASET_BASE}/M03A/year={d.year}/month={d.month:02d}/"
               f"day={d.day:02d}")
        rc = glob_count(spark, src)
        if rc == 0:
            log(f"  - {tag}：raw 無檔，跳過"); skip += 1; continue
        if not force and rc < cfg["expected"]:
            log(f"  - {tag}：raw {rc}/{cfg['expected']} 不完整，跳過（等補齊後再轉）")
            skip += 1; continue
        if not force and path_exists(spark, dst):
            log(f"  ✓ {tag}：day 目錄已存在，跳過"); skip += 1; continue
        try:
            log(f"  → {tag}：轉換中（raw {rc} 檔）…")
            ok = convert_unit(spark, "M03A", cfg, src, dst, f"{d:%Y%m%d}")
            conv += ok; fail += not ok
        except Exception as e:  # noqa: BLE001
            log(f"  ! {tag}：轉換失敗：{e}"); fail += 1
        finally:
            spark.catalog.clearCache()
    return conv, skip, fail


# ---------------- M06A：逐月（raw 檔數戳記） ----------------

def run_m06a(spark, cfg, dates, force):
    months = sorted({(d.year, d.month) for d in dates}, reverse=True)
    conv = skip = fail = 0
    for y, m in months:
        tag = f"M06A {y}-{m:02d}"
        src = f"{RAW_BASE}/M06A/year={y}/month={m:02d}/TDCS_M06A_*.csv"
        dst = f"{DATASET_BASE}/M06A/year={y}/month={m:02d}"
        rc = glob_count(spark, src)
        if rc == 0:
            log(f"  - {tag}：raw 無檔，跳過"); skip += 1; continue
        marker = read_marker(spark, dst) if path_exists(spark, dst) else None
        if not force and marker == rc:
            log(f"  ✓ {tag}：raw 檔數沒變（{rc}），跳過"); skip += 1; continue
        why = ("--force" if force else
               f"戳記 {marker} → 現在 {rc}" if marker is not None else
               "月目錄無戳記（狀態不明）" if path_exists(spark, dst) else "月目錄不存在")
        try:
            log(f"  → {tag}：整月重轉（{why}；raw {rc} 檔）…")
            ok = convert_unit(spark, "M06A", cfg, src, dst, f"{y}{m:02d}")
            if ok:
                write_marker(spark, dst, rc)
            conv += ok; fail += not ok
        except Exception as e:  # noqa: BLE001
            log(f"  ! {tag}：轉換失敗：{e}"); fail += 1
        finally:
            spark.catalog.clearCache()
    return conv, skip, fail


# ---------------- CLI（跟 Iceberg 版相同，DAG 不用改） ----------------

def resolve_dates(a):
    """--auto → D-5~D-24 窗口；否則用 --start/--end。回傳由新到舊的日期清單。"""
    if a.auto and (a.start or a.end):
        raise SystemExit("--auto 不能跟 --start/--end 一起用，請擇一")
    if a.auto:
        newest = dt.date.today() - dt.timedelta(days=a.offset_days)
        return [newest - dt.timedelta(days=i) for i in range(a.days)]
    if a.start and a.end:
        s = dt.datetime.strptime(a.start, "%Y-%m-%d").date()
        e = dt.datetime.strptime(a.end, "%Y-%m-%d").date()
        if s > e:
            raise SystemExit(f"--start ({a.start}) 不能晚於 --end ({a.end})")
        return list(reversed(list(daterange(s, e))))   # 也由新到舊
    raise SystemExit("需指定 --auto 或 --start 且 --end")


def main():
    p = argparse.ArgumentParser(description="TDCS CSV→/dataset Parquet（M03A/M06A 合併、冪等自我修復）")
    p.add_argument("dtype", choices=list(CONFIGS), help="資料類型")
    p.add_argument("--auto", action="store_true",
                   help=f"自動窗口：D-{OFFSET_DAYS_AUTO} 往回 {SCAN_DAYS_AUTO} 天（DAG 用）")
    p.add_argument("--start", help="手動範圍起 YYYY-MM-DD（需與 --end 成對）")
    p.add_argument("--end", help="手動範圍迄 YYYY-MM-DD")
    p.add_argument("--offset-days", type=int, default=OFFSET_DAYS_AUTO,
                   help=f"--auto 窗口最新端=今天-N（預設 {OFFSET_DAYS_AUTO}）")
    p.add_argument("--days", type=int, default=SCAN_DAYS_AUTO,
                   help=f"--auto 往回幾天（預設 {SCAN_DAYS_AUTO}）")
    p.add_argument("--force", action="store_true",
                   help="忽略完整度與已存在，強制重轉")
    a = p.parse_args()

    dtype = a.dtype
    cfg = CONFIGS[dtype]
    dates = resolve_dates(a)
    log(f"===== convert {dtype}：{dates[-1]} ~ {dates[0]}"
        f"（{len(dates)} 天）force={a.force} =====")

    spark = (SparkSession.builder
             .appName(f"ETC_{dtype}_convert")
             # dtadm 的 spark-defaults.conf 曾被加上 spark.task.cpus=4，跟 DAG
             # spark-submit 的 --executor-cores 1 相衝，SparkContext 會拒絕啟動
             # （2026-06 起每晚 convert task 因此全紅）。在此明確鎖回 1。
             .config("spark.task.cpus", "1")
             .config("spark.sql.shuffle.partitions", cfg["shuffle_partitions"])
             .getOrCreate())

    # 清掉上次中斷留下的 tmp 殘骸（正式路徑只在 rename 一瞬間被動到）
    delete_path(spark, f"{DATASET_BASE}/{dtype}/.tmp_convert")

    if cfg["granularity"] == "day":
        conv, skip, fail = run_m03a(spark, cfg, dates, a.force)
    else:
        conv, skip, fail = run_m06a(spark, cfg, dates, a.force)

    log(f"===== {dtype} 完成：轉換 {conv} | 跳過 {skip} | 失敗 {fail} =====")
    spark.stop()
    raise SystemExit(1 if fail else 0)


if __name__ == "__main__":
    main()
