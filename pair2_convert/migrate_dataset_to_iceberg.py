#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migrate_dataset_to_iceberg.py — /dataset/<DTYPE> Parquet → Iceberg（一次性遷移）
==============================================================================
把既有的 hdfs:///dataset/<DTYPE>/year=/month=/day=/*.parquet（day 分區）
搬進 Iceberg table iceberg.tdcs.<dtype>（只分 year/month；day 降為一般欄位）。

為什麼用 Iceberg 而不是直接重寫 Parquet 目錄：
  - 分區改成 year/month 後，若仍是純 Parquet + dynamic overwrite，每日轉檔的
    覆蓋粒度會變成「整個月」，等於每天都要重寫該月全部資料。
  - Iceberg 的 overwrite(條件) 可以原子性地只汰換單一天的資料檔，日常轉檔
    （convert_batch.py）成本跟原本 day 分區一樣低。
  - day 保留為一般欄位，靠 Iceberg 的 min/max 檔案統計照樣能做天級過濾。

catalog / 位置（跟 convert_batch.py 共用同一組常數，兩邊要一致）：
  - hadoop catalog（純 HDFS，不經 Hive metastore；沿用 yaml/icetable.py 慣例）
  - warehouse：hdfs:///iceberg → table 實體在 hdfs:///iceberg/tdcs/<dtype>/

用法（在 dtadm 內跑；先 M03A 全月 OK，再跑 M06A）——建議用 wrapper：
  bash ~/wulin/pair2/convert_parquet/run_migrate.sh M03A

  wrapper 每輪起一個全新 spark-submit、只跑 --max-months 個月就整個 JVM 結束，
  再起下一輪（state file 自動接續）。這是本環境驗證過的模式（同 run_batch.sh）：
  dtadm pod 有 cgroup pids 上限（thread 也算 pid），單一長壽 JVM 逐月累積 thread
  會撞 pthread_create EAGAIN；每輪換新 JVM 就徹底沒這個問題。

  也可以手動單輪跑（跑完 exit code 3 = 還有月份沒做完，再跑一次即可）：
  spark-submit --master yarn --deploy-mode client \
      --driver-memory 2g --num-executors 2 --executor-memory 3g --executor-cores 1 \
      --conf spark.task.cpus=1 \
      ~/wulin/pair2/convert_parquet/migrate_dataset_to_iceberg.py M03A --max-months 3
  （--conf spark.task.cpus=1 是因為 dtadm 的 spark-defaults.conf 被改成 task.cpus=4，
    跟 executor-cores 1 相衝；明確蓋回來才不會依賴那份飄移的 conf。）

前置需求（一次性）：
  Iceberg runtime jar 要進 Spark classpath。本腳本預設用 spark.jars.packages
  自動抓（首跑會下載到 ~/.ivy2 快取）；若 dtadm 對外網受限，改手動放 jar：
    /opt/zfs/sys/spark-3.4.4-bin-hadoop3/jars/iceberg-spark-runtime-3.4_2.12-1.9.0.jar
  （lkh 環境的 wk/lkh/bin/lkhprep 就是這樣裝的）放好後把 ICEBERG_PKG 設成 ""。

逐月冪等、可中斷續跑：
  - 逐 (year, month) 處理；每月用 overwrite(year=.. AND month=..) 寫入，
    重跑同一個月只會原子性換掉該月資料，不會重複。
  - 每月寫完立刻讀回驗筆數，結果 append 進 state file（JSONL）；
    下次啟動自動跳過已標 OK 的月份。MISMATCH/ERROR 的月份會重做。
  - 沿用 repartition_m03a.py 踩過的坑：HDFS 操作全走 driver JVM 內的
    FileSystem API（不開 subprocess），並可 --restart-every 定期重啟
    SparkSession 釋放 thread。

遷移完成後的切換順序（重要，不要顛倒）：
  1. 本腳本把歷史資料全部搬進 Iceberg 且全月 OK。
  2. 換上新版 convert_batch.py（寫 Iceberg），Airflow DAG 不用改
     （路徑/參數都沒變）。
  3. 下游讀取端改用 table 讀：
       spark.read.table("iceberg.tdcs.m03a")   # 取代 spark.read.parquet("/dataset/M03A")
     目前會動到的：pair2/gantry/*.py、webapp（pair2 模式）。
  4. 舊的 hdfs:///dataset/<DTYPE>/ 確認沒人讀之後才刪。
"""
import argparse
import datetime as dt
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# ---- 跟 convert_batch.py 保持一致的常數 --------------------------------
ICEBERG_PKG       = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.9.0"
ICEBERG_WAREHOUSE = "hdfs:///iceberg"
TARGET_FILE_MB    = 128     # 每個輸出檔目標大小（估 coalesce 數用）

TABLES = {
    "M03A": {
        "src":   "hdfs:///dataset/M03A",
        "table": "iceberg.tdcs.m03a",
        "casts": {"VehicleType": "int", "Volume": "int"},
        "ddl": """CREATE TABLE IF NOT EXISTS iceberg.tdcs.m03a (
                    TimeInterval string,
                    GantryID     string,
                    Direction    string,
                    VehicleType  int,
                    Volume       int,
                    year         string,
                    month        string,
                    day          string)
                  USING iceberg
                  PARTITIONED BY (year, month)
                  TBLPROPERTIES ('write.target-file-size-bytes'='134217728')""",
    },
    "M06A": {
        # 2026-07-16 更新：來源改為統一後的 /dataset/M06A（year=/month= 月檔、
        # 型別已由 unify_m06a_schema.py 統一為 int/double、含 TripInformation）。
        # 涵蓋範圍以樹上實際存在的月份為準（2024 起；舊被清洗的資料已作廢）。
        "src":   "hdfs:///dataset/M06A",
        "table": "iceberg.tdcs.m06a",
        "day_from": "DetectionTime_O",   # 來源沒有 day 欄位時，從這欄的日期補
        "casts": {"VehicleType": "int", "TripLength": "double"},
        "ddl": """CREATE TABLE IF NOT EXISTS iceberg.tdcs.m06a (
                    VehicleType      int,
                    DetectionTime_O  string,
                    GantryID_O       string,
                    DetectionTime_D  string,
                    GantryID_D       string,
                    TripLength       double,
                    TripEnd          string,
                    TripInformation  string,
                    year             string,
                    month            string,
                    day              string)
                  USING iceberg
                  PARTITIONED BY (year, month)
                  TBLPROPERTIES ('write.target-file-size-bytes'='134217728')""",
    },
}


def log(msg):
    print(f"[{dt.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


# ---- HDFS 操作：一律走 driver JVM 的 FileSystem API，不開 subprocess ----

def _fs(spark):
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    return fs, jvm


def list_months(spark, src_base):
    """列出來源實際存在的 (year, month) 清單，由舊到新。"""
    fs, jvm = _fs(spark)
    months = []
    base = jvm.org.apache.hadoop.fs.Path(src_base)
    if not fs.exists(base):
        return months
    for ydir in fs.listStatus(base):
        yname = ydir.getPath().getName()
        if not yname.startswith("year="):
            continue
        for mdir in fs.listStatus(ydir.getPath()):
            mname = mdir.getPath().getName()
            if mname.startswith("month="):
                months.append((yname.split("=")[1], mname.split("=")[1]))
    return sorted(months)


def dir_size_bytes(spark, path):
    fs, jvm = _fs(spark)
    p = jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(p):
        return 0
    return int(fs.getContentSummary(p).getLength())


# ---- state file（JSONL，append + fsync，斷點續跑） ----------------------

def load_done(state_file):
    done = {}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    r = json.loads(line)
                    done[(r["year"], r["month"])] = r["status"]
                except (json.JSONDecodeError, KeyError):
                    continue
    except FileNotFoundError:
        return set()
    return {k for k, v in done.items() if v == "OK"}


def append_state(state_file, year, month, status, src_count, dst_count, secs):
    rec = {"year": year, "month": month, "status": status,
           "src_count": src_count, "dst_count": dst_count,
           "duration_sec": round(secs, 1),
           "timestamp": dt.datetime.now().isoformat(timespec="seconds")}
    with open(state_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())


# ---- Spark session（Iceberg hadoop catalog） ----------------------------

def build_spark(dtype):
    b = (SparkSession.builder
         .appName(f"migrate_{dtype}_to_iceberg")
         .config("spark.sql.extensions",
                 "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.iceberg.type", "hadoop")
         .config("spark.sql.catalog.iceberg.warehouse", ICEBERG_WAREHOUSE)
         # 關掉分區型別推斷：year=2024/month=07 直接以字串 "2024"/"07" 進來，
         # 保留補零、也跟 Iceberg table 的 string 欄位型別一致
         .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false"))
    if ICEBERG_PKG:
        b = b.config("spark.jars.packages", ICEBERG_PKG)
    return b.getOrCreate()


def migrate_month(spark, cfg, year, month):
    """搬一個月：讀 day 分區 parquet → coalesce → 原子 overwrite 該月。回傳 (status, src, dst)。"""
    src_path = f"{cfg['src']}/year={year}/month={month}"

    size = dir_size_bytes(spark, src_path)
    n_files = max(1, round(size / (TARGET_FILE_MB * 1024 * 1024)))
    log(f"  [{year}-{month}] 來源 {size / 1048576:.1f} MB → 目標 {n_files} 檔")

    # basePath 讓 year/month（及 day，若來源有這層）以分區欄位身分進 schema
    # （字串型別，見上面設定）
    df = spark.read.option("basePath", cfg["src"]).parquet(src_path)
    if "day" not in df.columns and cfg.get("day_from"):
        # 月檔來源沒有 day 層：day 取自資料時間戳的「日」。分區 year/month 仍
        # 以檔案路徑（歸檔月）為準，跟 convert_batch 的「檔名日期」精神一致。
        df = df.withColumn(
            "day",
            F.lpad(F.dayofmonth(F.to_timestamp(F.col(cfg["day_from"])))
                   .cast("string"), 2, "0"))
    # 對齊 table 欄位型別（來源 parquet 型別可能與 DDL 有出入，明確 cast 最保險）
    for c, t in cfg.get("casts", {}).items():
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(t))
    src_count = df.count()

    # coalesce 不 shuffle、保留原始資料順序（壓縮率較好，見 repartition_m03a.py 的教訓）；
    # 目標數 >= 現有分區數時 coalesce 是 no-op，直接呼叫即可
    df = df.coalesce(n_files)

    (df.writeTo(cfg["table"])
       .overwrite((col("year") == year) & (col("month") == month)))

    dst_count = (spark.table(cfg["table"])
                 .where((col("year") == year) & (col("month") == month))
                 .count())

    status = "OK" if src_count == dst_count else "MISMATCH"
    log(f"  [{year}-{month}] {status}：來源 {src_count} / 寫入 {dst_count}")
    return status, src_count, dst_count


def main():
    p = argparse.ArgumentParser(description="/dataset Parquet → Iceberg 一次性遷移")
    p.add_argument("dtype", choices=list(TABLES))
    p.add_argument("--state-file", default=None,
                   help="斷點續跑進度檔（預設 migrate_<dtype>_state.jsonl）")
    p.add_argument("--force-redo", action="store_true",
                   help="忽略 state file 全部重跑（overwrite 冪等，安全）")
    p.add_argument("--max-months", type=int, default=0,
                   help="本輪最多處理幾個月就結束（exit 3=還有剩），0=不限。"
                        "配 run_migrate.sh 每輪換全新 JVM，避開 pod 的 pids 上限")
    p.add_argument("--restart-every", type=int, default=4,
                   help="每處理幾個月重啟一次 SparkSession 釋放 thread 額度"
                        "（用 run_migrate.sh 時用不到；0=不重啟）")
    a = p.parse_args()

    cfg = TABLES[a.dtype]
    state_file = a.state_file or f"migrate_{a.dtype.lower()}_state.jsonl"
    done = set() if a.force_redo else load_done(state_file)

    spark = build_spark(a.dtype)
    spark.sql(cfg["ddl"])
    log(f"===== 遷移 {a.dtype} → {cfg['table']}（state: {state_file}，已完成 {len(done)} 月）=====")

    months = list_months(spark, cfg["src"])
    if not months:
        raise SystemExit(f"來源 {cfg['src']} 底下找不到任何 year=/month= 目錄")
    log(f"來源共 {len(months)} 個月：{months[0]} ~ {months[-1]}")

    todo = [m for m in months if m not in done]
    batch = todo[:a.max_months] if a.max_months > 0 else todo
    log(f"待處理 {len(todo)} 個月，本輪跑 {len(batch)} 個")

    ok = fail = 0
    since_restart = 0
    t_all = time.time()
    for year, month in batch:
        t0 = time.time()
        try:
            status, sc, dc = migrate_month(spark, cfg, year, month)
        except Exception as e:  # noqa: BLE001
            status, sc, dc = "ERROR", 0, 0
            log(f"  [{year}-{month}] ERROR：{e}")
        append_state(state_file, year, month, status, sc, dc, time.time() - t0)
        ok += status == "OK"
        fail += status != "OK"

        spark.catalog.clearCache()
        since_restart += 1
        if a.restart_every > 0 and since_restart >= a.restart_every:
            log(f"已處理 {since_restart} 個月，重啟 SparkSession 釋放資源…")
            spark.stop()
            time.sleep(3)
            spark = build_spark(a.dtype)
            since_restart = 0

    remaining = len(todo) - len(batch)
    log(f"===== {a.dtype} 本輪結束：OK {ok} | 失敗 {fail} | 還剩 {remaining} 個月"
        f"（{time.time() - t_all:.0f}s）=====")
    if fail:
        log("有月份失敗或筆數不符，修正後直接重跑即可（只會補做未 OK 的月份）")
    spark.stop()
    # exit code：1=有失敗；3=本輪額度用完但還有月份沒做（wrapper 據此續跑）；0=全部完成
    raise SystemExit(1 if fail else (3 if remaining else 0))


if __name__ == "__main__":
    main()
