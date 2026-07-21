# 配合新版 webapp（POST /api/query、/api/stats、/api/stats/meta + 靜態檔案）
# 舊的 GET /api/m03a、/api/m06a 仍保留向下相容。
# M06A 的查詢方式維持：只要 TripInformation 有依序經過 A→B 就算一筆。
import json
import os
import re
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, date

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from functools import reduce

RAW_M03A = "/dataset/M03A"
RAW_M06A = "/dataset/M06A"
DIM_GANTRY_CSV = "/dataset/dim/gantry_summary.csv"   # 門架對照表（含新舊編號）
MAX_LIMIT = 100_000
MAX_QUERY_ROWS = 200_000   # /api/query 單次回傳列數上限（聚合後）
PAD_DAYS = 1  # 邊界日各多讀一天，再靠時間欄位精修，避開時區/歸檔日誤差

# webapp 靜態檔（index.html、js/、css/、data/）放在本檔同層的 webapp/ 目錄，
# 或用環境變數 WEBAPP_DIR 指定
WEBAPP_DIR = os.environ.get(
    "WEBAPP_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "webapp"))

TS_PAT = r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"
VEHICLE_ORDER = ["31", "32", "41", "42", "5"]

spark = SparkSession.builder.appName("tdcs-direct-query-api").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
app = FastAPI(title="TDCS Direct Query API")

# Spark 查詢改到 threadpool 執行後（見 CHANGES_2026-07-17.md），重查詢不再凍住
# 整個 event loop；但同時跑太多 Spark job 會把 8g heap / pids-limit(2048) 撐爆，
# 用號誌把同時進 Spark 的查詢數壓在上限內，超過的在執行緒裡排隊等。
SPARK_QUERY_SEMAPHORE = threading.Semaphore(
    int(os.environ.get("MAX_CONCURRENT_SPARK_QUERIES", "4")))


# ---------- 工具函式 ----------
def parse_dt(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s)   # 接受 'YYYY-MM-DD HH:MM[:SS]' 或 ...T...
    except (ValueError, TypeError):
        raise HTTPException(400, f"時間格式錯誤: {s}，請用 YYYY-MM-DD HH:MM:SS")

def parse_date_str(s) -> date:
    try:
        return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
    except (ValueError, TypeError):
        raise HTTPException(400, f"日期格式錯誤: {s}，請用 YYYY-MM-DD")

def day_paths(base: str, start: datetime, end: datetime):
    """M03A 目前正從 year/month/day 三層遷移到 year/month 兩層（與 M06A 統一）。
    兩種 glob 都列出來，交給 existing() 留下實際存在的那種——遷移前後都能查。"""
    d = (start - timedelta(days=PAD_DAYS)).date()
    last = (end + timedelta(days=PAD_DAYS)).date()
    out = []
    months_seen = set()
    while d <= last:
        out.append(f"{base}/year={d.year}/month={d.month:02d}/day={d.day:02d}/*.parquet")
        months_seen.add((d.year, d.month))
        d += timedelta(days=1)
    for y, m in sorted(months_seen):          # 新的月檔層（day 資訊靠時間欄位過濾）
        out.append(f"{base}/year={y}/month={m:02d}/*.parquet")
    return out

def month_paths(base: str, start: datetime, end: datetime):
    """M06A 只切到 month 層（year=YYYY/month=MM/data.parquet），沒有 day= 目錄。"""
    d = (start - timedelta(days=PAD_DAYS)).date().replace(day=1)
    last = (end + timedelta(days=PAD_DAYS)).date().replace(day=1)
    out = []
    while d <= last:
        out.append(f"{base}/year={d.year}/month={d.month:02d}/*.parquet")
        d = (d + timedelta(days=32)).replace(day=1)
    return out

def hdfs_fs():
    jvm = spark._jvm
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    return fs, jvm.org.apache.hadoop.fs.Path

def existing(paths):
    """只保留真的存在的路徑，避免缺檔日造成 'Path does not exist' 例外。"""
    fs, Path = hdfs_fs()
    keep = []
    for p in paths:
        st = fs.globStatus(Path(p))
        if st and len(st) > 0:
            keep.append(p)
    return keep

def csv_list(v):
    if v is None or v.strip() == "" or v.strip().lower() == "all":
        return None                        # None = 不加 filter = 全選
    return [x.strip() for x in v.split(",") if x.strip()]

def str_list(v):
    """payload 裡的 list 欄位（如 gantry_ids）；空 list / None = 全選。"""
    if not v:
        return None
    if isinstance(v, str):
        return csv_list(v)
    out = [str(x).strip() for x in v if str(x).strip()]
    return out or None

def rows_to_json(rows):
    out = []
    for r in rows:
        d = r.asDict()
        for k, val in d.items():
            if isinstance(val, (datetime, date)):
                d[k] = val.isoformat()
        out.append(d)
    return out

def passed_through_pattern(o_list, d_list):
    """產生 regex：TripInformation 中存在某個 A（o_list）出現在某個 B（d_list）之前。
    用 JVM 端 rlike/regexp_extract 取代 Python UDF——掃整月大 parquet 時 Python worker
    會吃爆記憶體把 Spark JVM 弄死，改用 regex 後全程在 JVM 內完成。"""
    o_pat = "|".join(re.escape(o) for o in o_list)
    d_pat = "|".join(re.escape(d) for d in d_list)
    return f"({o_pat}).*({d_pat})"

def trip_time_pattern(o_list, d_list):
    """擷取 TripInformation 中通過 A 的時間（group 1）與最後一次通過 B 的時間（group 2）。
    格式：'YYYY-MM-DD HH:MM:SS+門架ID; ...'"""
    o_pat = "|".join(re.escape(o) for o in o_list)
    d_pat = "|".join(re.escape(d) for d in d_list)
    return f"({TS_PAT})\\+(?:{o_pat}).*({TS_PAT})\\+(?:{d_pat})"

def route_ids_from_trip(trip_info, o_list, d_list):
    """從單筆 TripInformation 取出「首次通過 A → 最後通過 B」之間的門架序列（含 A、B）。
    取「最後 B」與 trip_time_pattern 的貪婪比對一致。順序不對（B 在 A 前）回 None。"""
    gids = [seg.rsplit("+", 1)[-1].strip() for seg in str(trip_info).split(";")]
    o_set, d_set = set(o_list), set(d_list)
    i = next((idx for idx, g in enumerate(gids) if g in o_set), None)
    if i is None:
        return None
    j = next((idx for idx in range(len(gids) - 1, i, -1) if gids[idx] in d_set), None)
    if j is None:
        return None
    return gids[i:j + 1]

def find_m06a_route_ids(df, o_list, d_list):
    """抽樣少量符合條件的旅次，回傳最常見的實際門架路徑（前端地圖畫線用）。
    limit 讓 Spark 掃到樣本就提前結束；純屬顯示輔助，失敗一律回 None、不影響主查詢。"""
    try:
        samples = df.select("TripInformation").limit(24).collect()
    except Exception as exc:
        print(f"[api/query] route 抽樣失敗（忽略）：{exc}", flush=True)
        return None
    routes = Counter()
    for row in samples:
        ids = route_ids_from_trip(row["TripInformation"], o_list, d_list)
        if ids and len(ids) >= 2:
            routes[tuple(ids)] += 1
    if not routes:
        return None
    return list(routes.most_common(1)[0][0])


# ---------- 門架新舊編號對照（讀 HDFS /dataset/dim/gantry_summary.csv）----------
# 部分路段的門架曾改編號（如 01F0155N → 01F0153N，2024-04-03 切換）。查詢時間
# 跨到切換日的話，只用單一編號過濾會漏掉另一個編號時期的資料、結果被低估。
# 這裡把對照表建成「別名群組」：查任何一個編號，自動展開成整組新舊編號一起查。
_gantry_alias = None    # gid -> [同一實體門架的所有編號（含自己）]
_gantry_canon = None    # gid -> 該群組目前有效（is_active）的代表編號

def load_gantry_alias():
    global _gantry_alias, _gantry_canon
    if _gantry_alias is not None:
        return _gantry_alias, _gantry_canon
    alias, canon = {}, {}
    try:
        rows = spark.read.option("header", True).csv(DIM_GANTRY_CSV).collect()
        # 欄位（表頭含 BOM，一律用位置索引）：
        # 0=門架代碼 6=is_active 7=舊門架代碼 8=新門架代碼
        parent = {}

        def find(x):
            parent.setdefault(x, x)
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a, b):
            parent[find(a)] = find(b)

        active = set()
        for r in rows:
            gid = (r[0] or "").strip()
            if not gid:
                continue
            find(gid)
            if (r[6] or "").strip() == "True":
                active.add(gid)
            for other in ((r[7] or "").strip(), (r[8] or "").strip()):
                if other:
                    union(gid, other)

        groups = defaultdict(set)
        for gid in list(parent):
            groups[find(gid)].add(gid)
        for members in groups.values():
            if len(members) < 2:
                continue                      # 沒改過編號的門架不用進表
            ordered = sorted(members)
            rep = next((m for m in ordered if m in active), ordered[-1])
            for m in members:
                alias[m] = ordered
                canon[m] = rep
        print(f"[gantry-alias] 載入 {DIM_GANTRY_CSV}：{len(alias)} 個編號屬於 "
              f"{sum(1 for ms in groups.values() if len(ms) > 1)} 組新舊對照", flush=True)
    except Exception as exc:  # noqa: BLE001
        # 對照表讀不到就退回原樣查詢（功能降級、不擋查詢），下次呼叫再重試
        print(f"[gantry-alias] 載入失敗（本次以原樣編號查詢）：{exc}", flush=True)
        return {}, {}
    _gantry_alias, _gantry_canon = alias, canon
    return alias, canon


def expand_gantry_ids(ids):
    """把使用者選的門架編號展開成含新舊編號的完整清單（沒改過編號的原樣返回）。"""
    if not ids:
        return ids
    alias, _ = load_gantry_alias()
    out, seen = [], set()
    for g in ids:
        for a in alias.get(g, [g]):
            if a not in seen:
                seen.add(a)
                out.append(a)
    return out


def alias_to_queried_map(original_ids, expanded_ids):
    """回傳 {別名: 使用者原本查的編號}，用來把結果歸戶回同一條序列。
    只涵蓋本次查詢展開到的群組；使用者本來就查的編號不改寫。"""
    if not original_ids or expanded_ids == original_ids:
        return {}
    alias, _ = load_gantry_alias()
    orig = set(original_ids)
    m = {}
    for g in original_ids:
        for a in alias.get(g, []):
            if a not in orig:
                m[a] = g
    return m


# ---------- 門架群組（road_ranking 用；讀 webapp/data/gantry_groups_frontend.json）----------
_gantry_lookup = None

def load_gantry_lookup():
    global _gantry_lookup
    if _gantry_lookup is not None:
        return _gantry_lookup

    path = os.path.join(WEBAPP_DIR, "data", "gantry_groups_frontend.json")
    lookup = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            raw_groups = json.load(f)
        for group in raw_groups:
            current_id = str(group.get("current_gantry_id") or group.get("gantry_id") or "")
            gantry_ids = group.get("gantry_ids") or ([current_id] if current_id else [])
            normalized = {
                "group_id": str(group.get("group_id") or f"G_{current_id}"),
                "current_gantry_id": current_id,
                "name": str(group.get("display_name") or group.get("name") or current_id or "未命名路段"),
                "direction": "S" if group.get("direction") == "S" else "N",
                "direction_label": str(
                    group.get("direction_label")
                    or ("南下" if group.get("direction") == "S" else "北上")),
            }
            for gid in gantry_ids:
                if gid:
                    lookup[str(gid)] = normalized
            if current_id:
                lookup[current_id] = normalized
    _gantry_lookup = lookup
    return lookup


# ---------- 資料日期範圍（掃 HDFS 分割目錄，不讀資料）----------
def m03a_date_range():
    """day 層存在時精確到日；遷移成 month 層後改用月份近似（同 M06A 作法）。"""
    fs, Path = hdfs_fs()
    st = fs.globStatus(Path(f"{RAW_M03A}/year=*/month=*/day=*"))
    dates = []
    for s in (st or []):
        m = re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", s.getPath().toString())
        if m:
            try:
                dates.append(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
            except ValueError:
                pass
    if dates:
        return min(dates), max(dates)
    st = fs.globStatus(Path(f"{RAW_M03A}/year=*/month=*"))
    months = []
    for s in (st or []):
        m = re.search(r"year=(\d+)/month=(\d+)", s.getPath().toString())
        if m:
            try:
                months.append(date(int(m.group(1)), int(m.group(2)), 1))
            except ValueError:
                pass
    if not months:
        return None, None
    last = (max(months) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    return min(months), last

def m06a_date_range():
    """M06A 只切到 month 層，回傳最早月份第一天～最晚月份最後一天。"""
    fs, Path = hdfs_fs()
    st = fs.globStatus(Path(f"{RAW_M06A}/year=*/month=*"))
    months = []
    for s in (st or []):
        m = re.search(r"year=(\d+)/month=(\d+)", s.getPath().toString())
        if m:
            try:
                months.append(date(int(m.group(1)), int(m.group(2)), 1))
            except ValueError:
                pass
    if not months:
        return None, None
    last = (max(months) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    return min(months), last


# ---------- 舊版端點（向下相容）----------
@app.get("/health")
def health():
    return {"status": "ok", "spark": spark.version}

@app.get("/api/health")
def api_health():
    return {"ok": True, "message": "TDCS 查詢服務正在執行。", "spark": spark.version}

@app.get("/api/m03a")
def m03a(
    start: str = Query(..., description="起始時間 YYYY-MM-DD HH:MM:SS"),
    end: str = Query(..., description="結束時間"),
    gantry: str = Query("all", description="門架代號，逗號分隔或 all"),
    vehicle_type: str = Query("all", description="車種，逗號分隔或 all"),
    limit: int = Query(1000, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
):
    s, e = parse_dt(start), parse_dt(end)
    paths = existing(day_paths(RAW_M03A, s, e))
    if not paths:
        return {"count": 0, "data": []}

    # TimeInterval 是字串（'YYYY-MM-DD HH:MM'，台灣當地時間）。
    # 容器時區是 UTC，轉 timestamp 會偏移 8 小時，改用字串比對（此格式字典序＝時間序）。
    lo, hi = s.strftime("%Y-%m-%d %H:%M"), e.strftime("%Y-%m-%d %H:%M")
    df = (spark.read.option("basePath", RAW_M03A).parquet(*paths)
          .filter((F.col("TimeInterval") >= lo) & (F.col("TimeInterval") <= hi)))

    g = expand_gantry_ids(csv_list(gantry))   # 展開新舊編號，跨切換日不漏資料
    if g:
        df = df.filter(F.col("GantryID").isin(g))
    vt = csv_list(vehicle_type)
    if vt:
        df = df.filter(F.col("VehicleType").isin([int(x) for x in vt]))

    df = df.select("TimeInterval", "GantryID", "Direction", "VehicleType", "Volume")
    rows = df.orderBy("TimeInterval", "GantryID").limit(offset + limit).collect()[offset:]
    return {"count": len(rows), "data": rows_to_json(rows)}

@app.get("/api/m06a")
def m06a(
    start: str = Query(..., description="起始時間（比對 DetectionTimeO）"),
    end: str = Query(...),
    gantry_o: str = Query(..., description="經過門架 A，必填，逗號分隔"),
    gantry_d: str = Query(..., description="經過門架 B，必填，逗號分隔"),
    vehicle_type: str = Query("all"),
    limit: int = Query(1000, ge=1, le=MAX_LIMIT),
    offset: int = Query(0, ge=0),
):
    s, e = parse_dt(start), parse_dt(end)
    go, gd = csv_list(gantry_o), csv_list(gantry_d)
    if not go or not gd:
        raise HTTPException(400, "gantry_o 與 gantry_d 為必填，不可全選")
    # 展開新舊編號：TripInformation 裡的門架編號是「當時」的編號，
    # 跨切換日的查詢必須同時比對整組新舊編號才不會漏旅次
    go, gd = expand_gantry_ids(go), expand_gantry_ids(gd)

    paths = existing(month_paths(RAW_M06A, s, e))
    if not paths:
        return {"count": 0, "data": []}

    # 實際欄位名是 DetectionTime_O（字串 'YYYY-MM-DD HH:MM:SS'，台灣當地時間）。
    # 同 M03A：用字串比對避開時區偏移。
    lo, hi = s.strftime("%Y-%m-%d %H:%M:%S"), e.strftime("%Y-%m-%d %H:%M:%S")
    df = (spark.read.option("basePath", RAW_M06A).parquet(*paths)
          .filter((F.col("DetectionTime_O") >= lo) & (F.col("DetectionTime_O") <= hi)))

    # 第一層：粗篩，字串是否包含任一 A、任一 B（cheap，pushdown-friendly）
    o_contains = reduce(lambda a, b: a | b, [F.instr(F.col("TripInformation"), o) > 0 for o in go])
    d_contains = reduce(lambda a, b: a | b, [F.instr(F.col("TripInformation"), d) > 0 for d in gd])
    df = df.filter(o_contains & d_contains)

    # 第二層：精篩，確認 A 真的出現在 B 之前（順序正確）
    df = df.filter(F.col("TripInformation").rlike(passed_through_pattern(go, gd)))

    vt = csv_list(vehicle_type)
    if vt:
        # M06A 的 VehicleType 是字串型別，直接用字串比對
        df = df.filter(F.col("VehicleType").isin(vt))

    # 對外欄位名沿用 webapp 期望的格式（無底線），從實際欄位 alias 過去
    df = df.select(
        "VehicleType",
        F.col("DetectionTime_O").alias("DetectionTimeO"),
        F.col("GantryID_O").alias("GantryO"),
        F.col("DetectionTime_D").alias("DetectionTimeD"),
        F.col("GantryID_D").alias("GantryD"),
        "TripLength", "TripEnd", "TripInformation")
    rows = df.orderBy("DetectionTimeO").limit(offset + limit).collect()[offset:]
    return {"count": len(rows), "data": rows_to_json(rows)}


# ---------- 新版端點：POST /api/query（webapp 查詢頁）----------
def query_m03a_new(payload: dict):
    s, e = parse_dt(payload.get("start_time")), parse_dt(payload.get("end_time"))
    gantry_ids = str_list(payload.get("gantry_ids"))
    direction = str(payload.get("direction") or "").strip().upper()
    vehicle_type = str(payload.get("vehicle_type") or "").strip()

    paths = existing(day_paths(RAW_M03A, s, e))
    if not paths:
        return []

    lo, hi = s.strftime("%Y-%m-%d %H:%M"), e.strftime("%Y-%m-%d %H:%M")
    df = (spark.read.option("basePath", RAW_M03A).parquet(*paths)
          .filter((F.col("TimeInterval") >= lo) & (F.col("TimeInterval") <= hi)))

    expanded_ids = expand_gantry_ids(gantry_ids)
    if expanded_ids:
        df = df.filter(F.col("GantryID").isin(expanded_ids))
        # 把舊編號的資料歸戶回使用者查的編號，圖表才會是一條連續序列
        # （不然切換日前後會斷成兩條各自不完整的線）
        remap = alias_to_queried_map(gantry_ids, expanded_ids)
        if remap:
            df = df.na.replace(remap, subset=["GantryID"])
    if direction in ("N", "S"):
        df = df.filter(F.col("Direction") == direction)
    if vehicle_type:
        df = df.filter(F.col("VehicleType") == int(vehicle_type))

    # 依查詢跨度自動決定時間粒度（跟前端圖表彙整邏輯一致），
    # 沒選門架（全部門架）時至少彙整到小時，避免回傳量爆炸。
    span_days = (e - s).total_seconds() / 86400
    if span_days > 14:
        bucket = F.substring("TimeInterval", 1, 10)                 # 每日
    elif span_days > 2 or not gantry_ids:
        bucket = F.concat(F.substring("TimeInterval", 1, 13), F.lit(":00"))  # 每小時
    else:
        bucket = F.col("TimeInterval")                              # 原始 5 分鐘

    df = (df.groupBy(bucket.alias("time"), "GantryID", "Direction", "VehicleType")
            .agg(F.sum("Volume").alias("traffic_volume"))
            .select(
                "time",
                F.col("GantryID").alias("gantry_id"),
                F.col("Direction").alias("direction"),
                F.col("VehicleType").cast("string").alias("vehicle_type"),
                "traffic_volume"))
    rows = df.orderBy("time", "gantry_id").limit(MAX_QUERY_ROWS).collect()
    return rows_to_json(rows)

def query_m06a_new(payload: dict):
    s, e = parse_dt(payload.get("start_time")), parse_dt(payload.get("end_time"))
    go = str_list(payload.get("start_gantry_ids")) or str_list(payload.get("start_gantry"))
    gd = str_list(payload.get("end_gantry_ids")) or str_list(payload.get("end_gantry"))
    vehicle_type = str(payload.get("vehicle_type") or "").strip()
    if not go or not gd:
        raise HTTPException(400, "M06A 請選擇起始與終點門架。")
    # 展開新舊編號（TripInformation 記的是旅次當時的編號）
    go, gd = expand_gantry_ids(go), expand_gantry_ids(gd)

    paths = existing(month_paths(RAW_M06A, s, e))
    if not paths:
        return [], None

    # 粗篩用 DetectionTime_O：旅次一定在通過 A 之前出發，下界多留一天餘裕
    lo, hi = s.strftime("%Y-%m-%d %H:%M:%S"), e.strftime("%Y-%m-%d %H:%M:%S")
    lo_pad = (s - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    df = (spark.read.option("basePath", RAW_M06A).parquet(*paths)
          .filter((F.col("DetectionTime_O") >= lo_pad) & (F.col("DetectionTime_O") <= hi)))

    o_contains = reduce(lambda a, b: a | b, [F.instr(F.col("TripInformation"), o) > 0 for o in go])
    d_contains = reduce(lambda a, b: a | b, [F.instr(F.col("TripInformation"), d) > 0 for d in gd])
    df = df.filter(o_contains & d_contains)

    if vehicle_type:
        df = df.filter(F.col("VehicleType") == vehicle_type)

    # 地圖畫線用：留一個只含順序正確旅次的分支，稍後抽樣取實際路徑
    matched_df = df.filter(F.col("TripInformation").rlike(passed_through_pattern(go, gd)))

    # 從 TripInformation 擷取通過 A、B 的實際時間；擷取失敗（順序不對）的列同時被淘汰
    pat = trip_time_pattern(go, gd)
    df = (df.withColumn("time_a", F.regexp_extract("TripInformation", pat, 1))
            .withColumn("time_b", F.regexp_extract("TripInformation", pat, 2))
            .filter(F.col("time_a") != ""))

    # 精修時間範圍：以通過 A 的時間為準
    df = df.filter((F.col("time_a") >= lo) & (F.col("time_a") <= hi))

    # 依 15 分鐘 + 車種聚合（前端會再依查詢跨度重新彙整成小時/日/月）。
    # unix_timestamp/from_unixtime 皆用容器時區解析與輸出，往返後字面時間不變；
    # 900 秒取整不受時區位移影響。
    ts_a = F.unix_timestamp("time_a", "yyyy-MM-dd HH:mm:ss")
    ts_b = F.unix_timestamp("time_b", "yyyy-MM-dd HH:mm:ss")
    df = (df.withColumn("origin_time",
                        F.from_unixtime(F.floor(ts_a / 900) * 900, "yyyy-MM-dd HH:mm"))
            .withColumn("travel_minutes", (ts_b - ts_a) / 60.0)
            .groupBy("origin_time", "VehicleType")
            .agg(F.count(F.lit(1)).alias("trip_count"),
                 F.round(F.avg("travel_minutes"), 1).alias("avg_travel_time_minutes"))
            .select(
                "origin_time",
                F.col("VehicleType").cast("string").alias("vehicle_type"),
                "trip_count", "avg_travel_time_minutes"))

    rows = rows_to_json(df.orderBy("origin_time", "vehicle_type").limit(MAX_QUERY_ROWS).collect())
    start_gantry = str(payload.get("start_gantry") or go[0])
    end_gantry = str(payload.get("end_gantry") or gd[0])
    for r in rows:
        r["start_gantry"] = start_gantry
        r["end_gantry"] = end_gantry

    route_ids = find_m06a_route_ids(matched_df, go, gd) if rows else None
    return rows, route_ids

def _run_spark_query(fn, *args):
    """在 threadpool 執行緒裡跑 Spark 查詢，並用號誌限制併發數。"""
    with SPARK_QUERY_SEMAPHORE:
        return fn(*args)


@app.post("/api/query")
async def api_query(request: Request):
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(400, "請求內容必須是 JSON。")

    dataset = payload.get("dataset")
    s, e = parse_dt(payload.get("start_time")), parse_dt(payload.get("end_time"))
    if e < s:
        raise HTTPException(400, "終點時間不能早於起始時間。")

    # 跨度上限：範圍太大 Spark 掃不完會逾時，直接擋下並給明確訊息
    max_span = {"M03A": 366, "M06A": 92}.get(dataset)
    if max_span and (e - s).days > max_span:
        raise HTTPException(400, f"{dataset} 單次查詢最長 {max_span} 天，請縮短時間範圍、分次查詢。")

    t0 = time.monotonic()
    route_gantry_ids = None
    if dataset == "M03A":
        rows = await run_in_threadpool(_run_spark_query, query_m03a_new, payload)
    elif dataset == "M06A":
        rows, route_gantry_ids = await run_in_threadpool(_run_spark_query, query_m06a_new, payload)
    else:
        raise HTTPException(400, "dataset 必須是 M03A 或 M06A。")
    print(f"[api/query] {dataset} {payload.get('start_time')}~{payload.get('end_time')} "
          f"o={payload.get('start_gantry_ids') or payload.get('gantry_ids')} "
          f"d={payload.get('end_gantry_ids')} vt={payload.get('vehicle_type')!r} "
          f"rows={len(rows)} {time.monotonic() - t0:.1f}s", flush=True)

    return {
        "ok": True,
        "dataset": dataset,
        "row_count": len(rows),
        "rows": rows,
        "truncated": len(rows) >= MAX_QUERY_ROWS,
        "route_gantry_ids": route_gantry_ids,
        "query": payload,
    }


# ---------- 新版端點：/api/stats（webapp 統計儀表板頁）----------
def make_stats_range_label(period_type: str, start_date: date, end_date: date) -> str:
    labels = {"daily": "日流量", "weekly": "週流量", "monthly": "月流量"}
    title = labels.get(period_type, "統計")
    if start_date == end_date:
        return f"{title}｜{start_date:%Y/%m/%d}"
    return f"{title}｜{start_date:%Y/%m/%d} ～ {end_date:%Y/%m/%d}"

@app.get("/api/query/meta")
def query_meta():
    """查詢頁用：兩個資料集各自的資料涵蓋範圍，前端用來限制日期選擇器。"""
    m03_min, m03_max = m03a_date_range()
    m06_min, m06_max = m06a_date_range()
    return {
        "ok": True,
        "m03a": {
            "min_date": m03_min.isoformat() if m03_min else None,
            "max_date": m03_max.isoformat() if m03_max else None,
        },
        "m06a": {
            "min_date": m06_min.isoformat() if m06_min else None,
            "max_date": m06_max.isoformat() if m06_max else None,
        },
    }

@app.get("/api/stats/meta")
def stats_meta():
    min_date, max_date = m03a_date_range()
    return {
        "ok": True,
        "min_date": min_date.isoformat() if min_date else None,
        "max_date": max_date.isoformat() if max_date else None,
        "default_date": max_date.isoformat() if max_date else None,
        "dataset": "M03A",
        "scope": "全部門架、全部方向、全部車種",
    }

@app.post("/api/stats")
async def stats(request: Request):
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(400, "請求內容必須是 JSON。")

    period_type = str(payload.get("period_type") or "")
    if period_type not in {"daily", "weekly", "monthly"}:
        raise HTTPException(400, "period_type 格式錯誤。")

    start_date = parse_date_str(payload.get("start_date"))
    end_date = parse_date_str(payload.get("end_date"))
    if end_date < start_date:
        raise HTTPException(400, "end_date 不能早於 start_date。")
    if (end_date - start_date).days > 366:
        raise HTTPException(400, "單次統計期間不可超過 366 天。")

    return await run_in_threadpool(
        _run_spark_query, _stats_impl, period_type, start_date, end_date)


def _stats_impl(period_type: str, start_date: date, end_date: date):
    s = datetime.combine(start_date, datetime.min.time())
    e = datetime.combine(end_date, datetime.max.time())
    paths = existing(day_paths(RAW_M03A, s, e))

    empty = {
        "ok": True, "dataset": "M03A", "period_type": period_type,
        "range": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "label": make_stats_range_label(period_type, start_date, end_date),
        },
        "summary": {"total_volume": 0},
        "road_ranking": [], "hourly_series": [], "daily_series": [],
        "vehicle_share": [], "row_count": 0,
    }
    if not paths:
        return empty

    lo = start_date.strftime("%Y-%m-%d 00:00")
    hi = end_date.strftime("%Y-%m-%d 23:59")

    # 單趟聚合：Spark 只跑一個 job，把原始明細壓到「時間桶 × 門架 × 方向 × 車種」
    # 粒度（一個月約 5 萬列）收回 Python，總量／趨勢／排行／車種佔比全在 Python 算。
    # 日流量頁時間桶取到小時（供每小時趨勢圖），其餘週期取到日。
    # 舊版是 persist 整批原始明細再跑 5 個聚合 job：一個月要 cache 1,400 萬列，
    # 366 天會撐爆 8g heap（native thread OOM），改法詳見 CHANGES_2026-07-17.md。
    if period_type == "daily":
        bucket = F.concat(F.substring("TimeInterval", 1, 13), F.lit(":00"))
    else:
        bucket = F.substring("TimeInterval", 1, 10)

    t0 = time.monotonic()
    agg_rows = (spark.read.option("basePath", RAW_M03A).parquet(*paths)
                .filter((F.col("TimeInterval") >= lo) & (F.col("TimeInterval") <= hi))
                .groupBy(bucket.alias("bucket"), "GantryID", "Direction", "VehicleType")
                .agg(F.sum("Volume").alias("volume"),
                     F.count(F.lit(1)).alias("cnt"))
                .collect())
    if not agg_rows:
        return empty

    row_count = 0
    total_volume = 0
    hourly_totals = defaultdict(int)
    daily_totals = defaultdict(int)
    vehicle_totals = defaultdict(int)
    gantry_totals = defaultdict(int)
    for r in agg_rows:
        v = int(r["volume"])
        row_count += r["cnt"]
        total_volume += v
        b = r["bucket"]
        if period_type == "daily":
            hourly_totals[b] += v
            daily_totals[b[:10]] += v
        else:
            daily_totals[b] += v
        vehicle_totals[str(r["VehicleType"])] += v
        gantry_totals[(str(r["GantryID"]), r["Direction"])] += v

    hourly_series = [{"time": t, "volume": v} for t, v in sorted(hourly_totals.items())]
    daily_series = [{"date": d, "volume": v} for d, v in sorted(daily_totals.items())]

    print(f"[api/stats] {period_type} {start_date}~{end_date} "
          f"rows={row_count} agg={len(agg_rows)} {time.monotonic() - t0:.1f}s", flush=True)

    # 門架 → 路段群組排行（群組定義來自 webapp/data/gantry_groups_frontend.json）
    lookup = load_gantry_lookup()
    _, canon = load_gantry_alias()
    road_totals = {}
    for (gid, direction), volume in gantry_totals.items():
        # 舊編號先歸戶到現行編號再找群組，改編號前後的流量才會算在同一路段
        group = lookup.get(gid) or lookup.get(canon.get(gid, gid))
        if group:
            key = group["group_id"]
            entry = road_totals.setdefault(key, {
                "group_id": group["group_id"],
                "gantry_id": group["current_gantry_id"],
                "name": group["name"],
                "direction": group["direction"],
                "direction_label": group["direction_label"],
                "volume": 0,
            })
        else:
            key = f"RAW_{gid}"
            d = "S" if direction == "S" else "N"
            entry = road_totals.setdefault(key, {
                "group_id": key, "gantry_id": gid, "name": gid,
                "direction": d, "direction_label": "南下" if d == "S" else "北上",
                "volume": 0,
            })
        entry["volume"] += volume

    ranking = sorted(
        road_totals.values(),
        key=lambda item: (-item["volume"], item["name"], item["direction"]))[:10]
    for entry in ranking:
        entry["share"] = round(entry["volume"] / total_volume * 100, 1) if total_volume else 0.0

    vehicle_share = [
        {
            "vehicle_type": vt,
            "volume": vehicle_totals.get(vt, 0),
            "share": round(vehicle_totals.get(vt, 0) / total_volume * 100, 1) if total_volume else 0.0,
        }
        for vt in VEHICLE_ORDER]

    return {
        "ok": True,
        "dataset": "M03A",
        "period_type": period_type,
        "range": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "label": make_stats_range_label(period_type, start_date, end_date),
        },
        "summary": {"total_volume": int(total_volume)},
        "road_ranking": ranking,
        "hourly_series": hourly_series,
        "daily_series": daily_series,
        "vehicle_share": vehicle_share,
        "row_count": row_count,
    }


# ---------- webapp 靜態檔案（要放在所有 /api 路由之後）----------
if os.path.isdir(WEBAPP_DIR):
    for sub in ("css", "js", "data"):
        subdir = os.path.join(WEBAPP_DIR, sub)
        if os.path.isdir(subdir):
            app.mount(f"/{sub}", StaticFiles(directory=subdir), name=sub)

    @app.get("/")
    def index_page():
        return FileResponse(os.path.join(WEBAPP_DIR, "index.html"))

    @app.get("/{filename}")
    def root_files(filename: str):
        if filename in {"index.html", "query.html", "stats.html"}:
            return FileResponse(os.path.join(WEBAPP_DIR, filename))
        return JSONResponse({"ok": False, "error": "找不到檔案"}, status_code=404)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
