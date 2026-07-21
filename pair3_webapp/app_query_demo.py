from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, request, send_from_directory

BASE_DIR = Path(__file__).resolve().parent
SAMPLE_DIR = BASE_DIR / "data" / "m03a_samples"
GANTRY_GROUP_PATH = BASE_DIR / "data" / "gantry_groups_frontend.json"

app = Flask(__name__)


@app.get("/")
def index_page():
    return send_from_directory(BASE_DIR, "index.html")


@app.get("/<path:filename>")
def root_files(filename):
    if filename in {"index.html", "query.html", "stats.html"}:
        return send_from_directory(BASE_DIR, filename)
    return jsonify({"ok": False, "error": "找不到檔案"}), 404


@app.get("/css/<path:filename>")
def css_files(filename):
    return send_from_directory(BASE_DIR / "css", filename)


@app.get("/js/<path:filename>")
def js_files(filename):
    return send_from_directory(BASE_DIR / "js", filename)


@app.get("/data/<path:filename>")
def data_files(filename):
    return send_from_directory(BASE_DIR / "data", filename)


def parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value.strip(), fmt)
        except ValueError:
            pass

    return None


def parse_date(value: Any) -> date | None:
    if not isinstance(value, str):
        return None

    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d").date()
    except ValueError:
        return None


@lru_cache(maxsize=1)
def read_m03a_rows() -> tuple[dict[str, Any], ...]:
    rows: list[dict[str, Any]] = []

    for path in sorted(SAMPLE_DIR.glob("TDCS_M03A_*.csv")):
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            for values in csv.reader(file):
                if len(values) != 5:
                    continue

                time_text, gantry_id, direction, vehicle_type, volume_text = [
                    value.strip() for value in values
                ]
                row_time = parse_time(time_text)

                if row_time is None:
                    continue

                try:
                    volume = int(volume_text)
                except ValueError:
                    continue

                rows.append({
                    "_time": row_time,
                    "time": row_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "gantry_id": gantry_id,
                    "direction": direction,
                    "vehicle_type": vehicle_type,
                    "traffic_volume": volume,
                })

    return tuple(rows)


@lru_cache(maxsize=1)
def load_gantry_lookup() -> dict[str, dict[str, Any]]:
    if not GANTRY_GROUP_PATH.exists():
        return {}

    raw_groups = json.loads(GANTRY_GROUP_PATH.read_text(encoding="utf-8"))
    lookup: dict[str, dict[str, Any]] = {}

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
                or ("南下" if group.get("direction") == "S" else "北上")
            ),
        }

        for gantry_id in gantry_ids:
            if gantry_id:
                lookup[str(gantry_id)] = normalized

        if current_id:
            lookup[current_id] = normalized

    return lookup


def query_m03a(payload: dict[str, Any]) -> list[dict[str, Any]]:
    start = parse_time(payload.get("start_time"))
    end = parse_time(payload.get("end_time"))
    direction = str(payload.get("direction") or "")
    vehicle_type = str(payload.get("vehicle_type") or "")
    gantry_ids = {str(item) for item in payload.get("gantry_ids") or [] if item}

    result = []

    for row in read_m03a_rows():
        if start and row["_time"] < start:
            continue
        if end and row["_time"] > end:
            continue
        if direction and row["direction"] != direction:
            continue
        if vehicle_type and row["vehicle_type"] != vehicle_type:
            continue
        if gantry_ids and row["gantry_id"] not in gantry_ids:
            continue

        result.append({key: value for key, value in row.items() if key != "_time"})

    return result


def query_m06a_demo(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """
    這是前端畫面測試用的合成資料，不代表真實交通資料。
    每列已依 15 分鐘及車種聚合，trip_count 為該時段旅次數。
    """
    start = parse_time(payload.get("start_time"))
    end = parse_time(payload.get("end_time"))

    if start is None or end is None:
        return []

    selected_vehicle = str(payload.get("vehicle_type") or "")
    vehicle_types = [selected_vehicle] if selected_vehicle else ["31", "32", "41", "42", "5"]
    start_gantry = str(payload.get("start_gantry") or "")
    end_gantry = str(payload.get("end_gantry") or "")

    rows = []
    current = start.replace(minute=(start.minute // 15) * 15, second=0)
    index = 0

    while current <= end:
        hour_wave = 1 + 0.35 * math.sin((current.hour - 7) / 24 * math.pi * 2)
        peak_wave = 1.45 if current.hour in {7, 8, 17, 18} else 1

        base_counts = {
            "31": 82,
            "32": 16,
            "41": 5,
            "42": 11,
            "5": 7,
        }

        for vehicle_type in vehicle_types:
            trip_count = max(
                1,
                round(base_counts.get(vehicle_type, 8) * hour_wave * peak_wave + (index % 5))
            )
            travel_minutes = 22 + (index % 6) * 1.4 + (5 if peak_wave > 1 else 0)

            rows.append({
                "origin_time": current.strftime("%Y-%m-%d %H:%M:%S"),
                "destination_time": (current + timedelta(minutes=travel_minutes)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "start_gantry": start_gantry,
                "end_gantry": end_gantry,
                "vehicle_type": vehicle_type,
                "trip_count": trip_count,
                "avg_travel_time_minutes": round(travel_minutes, 1),
            })

        current += timedelta(minutes=15)
        index += 1

    return rows


def get_m03a_date_range() -> tuple[date | None, date | None]:
    rows = read_m03a_rows()
    if not rows:
        return None, None

    dates = [row["_time"].date() for row in rows]
    return min(dates), max(dates)


def make_stats_range_label(period_type: str, start_date: date, end_date: date) -> str:
    labels = {
        "daily": "日流量",
        "weekly": "週流量",
        "monthly": "月流量",
    }
    title = labels.get(period_type, "統計")

    if start_date == end_date:
        return f"{title}｜{start_date:%Y/%m/%d}"

    return f"{title}｜{start_date:%Y/%m/%d} ～ {end_date:%Y/%m/%d}"


def aggregate_stats(period_type: str, start_date: date, end_date: date) -> dict[str, Any]:
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)
    rows = [row for row in read_m03a_rows() if start_dt <= row["_time"] <= end_dt]

    total_volume = sum(int(row["traffic_volume"]) for row in rows)
    gantry_lookup = load_gantry_lookup()

    road_totals: dict[str, dict[str, Any]] = {}
    hourly_totals: defaultdict[datetime, int] = defaultdict(int)
    daily_totals: defaultdict[date, int] = defaultdict(int)
    vehicle_totals: defaultdict[str, int] = defaultdict(int)

    for row in rows:
        volume = int(row["traffic_volume"])
        row_time: datetime = row["_time"]
        hour_bucket = row_time.replace(minute=0, second=0, microsecond=0)
        hourly_totals[hour_bucket] += volume
        daily_totals[row_time.date()] += volume
        vehicle_totals[str(row["vehicle_type"])] += volume

        group = gantry_lookup.get(str(row["gantry_id"]))
        if group:
            road_key = group["group_id"]
            entry = road_totals.setdefault(road_key, {
                "group_id": group["group_id"],
                "gantry_id": group["current_gantry_id"],
                "name": group["name"],
                "direction": group["direction"],
                "direction_label": group["direction_label"],
                "volume": 0,
            })
        else:
            road_key = f"RAW_{row['gantry_id']}"
            direction = "S" if row["direction"] == "S" else "N"
            entry = road_totals.setdefault(road_key, {
                "group_id": road_key,
                "gantry_id": row["gantry_id"],
                "name": row["gantry_id"],
                "direction": direction,
                "direction_label": "南下" if direction == "S" else "北上",
                "volume": 0,
            })

        entry["volume"] += volume

    ranking = sorted(
        road_totals.values(),
        key=lambda item: (-item["volume"], item["name"], item["direction"]),
    )[:10]

    for entry in ranking:
        entry["share"] = round(entry["volume"] / total_volume * 100, 1) if total_volume else 0.0

    hourly_series = [
        {
            "time": bucket.strftime("%Y-%m-%d %H:00"),
            "volume": volume,
        }
        for bucket, volume in sorted(hourly_totals.items())
    ]

    daily_series = [
        {
            "date": bucket.strftime("%Y-%m-%d"),
            "volume": volume,
        }
        for bucket, volume in sorted(daily_totals.items())
    ]

    vehicle_order = ["31", "32", "41", "42", "5"]
    vehicle_share = [
        {
            "vehicle_type": vehicle_type,
            "volume": vehicle_totals.get(vehicle_type, 0),
            "share": round(vehicle_totals.get(vehicle_type, 0) / total_volume * 100, 1)
            if total_volume
            else 0.0,
        }
        for vehicle_type in vehicle_order
    ]

    min_available, max_available = get_m03a_date_range()
    coverage_note = "資料以實際可用範圍為準。"
    if min_available and max_available:
        coverage_note = (
            f"本機示範資料涵蓋 {min_available:%Y/%m/%d} ～ {max_available:%Y/%m/%d}；"
            "正式部署後由資料庫查詢服務提供完整期間資料。"
        )

    return {
        "ok": True,
        "dataset": "M03A",
        "period_type": period_type,
        "range": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "label": make_stats_range_label(period_type, start_date, end_date),
        },
        "summary": {
            "total_volume": total_volume,
        },
        "road_ranking": ranking,
        "hourly_series": hourly_series,
        "daily_series": daily_series,
        "vehicle_share": vehicle_share,
        "row_count": len(rows),
        "coverage_note": coverage_note,
        "demo": True,
    }


@app.get("/api/health")
def health():
    return jsonify({
        "ok": True,
        "message": "M03A + M06A + Stats 本機前端測試服務正在執行。",
        "m03a_files": [path.name for path in sorted(SAMPLE_DIR.glob("TDCS_M03A_*.csv"))],
        "m06a_note": "M06A 使用合成資料，只用於測試畫面與圖表。",
    })


@app.get("/api/stats/meta")
def stats_meta():
    min_date, max_date = get_m03a_date_range()

    return jsonify({
        "ok": True,
        "min_date": min_date.isoformat() if min_date else None,
        "max_date": max_date.isoformat() if max_date else None,
        "default_date": max_date.isoformat() if max_date else None,
        "dataset": "M03A",
        "scope": "全部門架、全部方向、全部車種",
        "demo": True,
    })


@app.post("/api/stats")
def stats():
    payload = request.get_json(silent=True)

    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "請求內容必須是 JSON。"}), 400

    period_type = str(payload.get("period_type") or "")
    if period_type not in {"daily", "weekly", "monthly"}:
        return jsonify({"ok": False, "error": "period_type 格式錯誤。"}), 400

    start_date = parse_date(payload.get("start_date"))
    end_date = parse_date(payload.get("end_date"))

    if start_date is None or end_date is None:
        return jsonify({"ok": False, "error": "請提供 YYYY-MM-DD 格式的 start_date 與 end_date。"}), 400

    if end_date < start_date:
        return jsonify({"ok": False, "error": "end_date 不能早於 start_date。"}), 400

    if (end_date - start_date).days > 366:
        return jsonify({"ok": False, "error": "單次統計期間不可超過 366 天。"}), 400

    return jsonify(aggregate_stats(period_type, start_date, end_date))


@app.post("/api/query")
def query():
    payload = request.get_json(silent=True)

    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "請求內容必須是 JSON。"}), 400

    dataset = payload.get("dataset")
    start = parse_time(payload.get("start_time"))
    end = parse_time(payload.get("end_time"))

    if start is None or end is None or end < start:
        return jsonify({"ok": False, "error": "查詢時間格式錯誤。"}), 400

    if dataset == "M03A":
        rows = query_m03a(payload)
    elif dataset == "M06A":
        if not payload.get("start_gantry") or not payload.get("end_gantry"):
            return jsonify({"ok": False, "error": "M06A 請選擇起始與終點門架。"}), 400
        rows = query_m06a_demo(payload)
    else:
        return jsonify({"ok": False, "error": "dataset 必須是 M03A 或 M06A。"}), 400

    return jsonify({
        "ok": True,
        "dataset": dataset,
        "row_count": len(rows),
        "rows": rows,
        "query": payload,
        "demo": dataset == "M06A",
    })


if __name__ == "__main__":
    print("網站：http://127.0.0.1:5000/")
    print("健康檢查：http://127.0.0.1:5000/api/health")
    print("統計資料範圍：http://127.0.0.1:5000/api/stats/meta")
    app.run(host="127.0.0.1", port=5000, debug=True)
