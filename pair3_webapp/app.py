"""webapp/app.py — 前端靜態檔 + API 代理層。

所有 /api/* 請求轉送到 pair2_api（FastAPI + Spark），由它查詢
HDFS 的 /dataset/M03A 與 /dataset/M06A，webapp 本身不持有資料。
上游位址由 webapp-config 的 PAIR2_API_URL 提供。
本機只測前端畫面時，可直接執行 app_query_demo.py。
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from pathlib import Path

from flask import Flask, Response, jsonify, request, send_from_directory

BASE_DIR = Path(__file__).resolve().parent
PAIR2_API_URL = os.environ.get(
    "PAIR2_API_URL", "http://pair2-api.dt.svc.cluster.local:8000"
).rstrip("/")
# Spark 查詢可能很慢；gunicorn timeout 是 120s，上游要留在它之內。
UPSTREAM_TIMEOUT = float(os.environ.get("PAIR2_API_TIMEOUT", "110"))

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
    response = send_from_directory(BASE_DIR / "css", filename)
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/js/<path:filename>")
def js_files(filename):
    response = send_from_directory(BASE_DIR / "js", filename)
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/data/<path:filename>")
def data_files(filename):
    return send_from_directory(BASE_DIR / "data", filename)


@app.route("/api/<path:subpath>", methods=["GET", "POST"])
def api_proxy(subpath):
    url = f"{PAIR2_API_URL}/api/{subpath}"
    if request.query_string:
        url += "?" + request.query_string.decode()

    data = None
    headers = {"Accept": "application/json"}
    if request.method == "POST":
        data = request.get_data()
        headers["Content-Type"] = request.headers.get(
            "Content-Type", "application/json"
        )

    upstream = urllib.request.Request(
        url, data=data, headers=headers, method=request.method
    )

    try:
        with urllib.request.urlopen(upstream, timeout=UPSTREAM_TIMEOUT) as resp:
            return Response(
                resp.read(),
                status=resp.status,
                content_type=resp.headers.get("Content-Type", "application/json"),
            )
    except urllib.error.HTTPError as exc:
        body = exc.read()
        content_type = (exc.headers.get("Content-Type") or "") if exc.headers else ""
        if "application/json" not in content_type:
            body = json.dumps(
                {"ok": False, "error": f"查詢服務回應錯誤（HTTP {exc.code}）。"}
            ).encode("utf-8")
            content_type = "application/json"
        return Response(body, status=exc.code, content_type=content_type)
    except (urllib.error.URLError, TimeoutError) as exc:
        return jsonify({"ok": False, "error": f"無法連線查詢服務：{exc}"}), 502


if __name__ == "__main__":
    print("網站：http://127.0.0.1:5000/")
    print(f"API 上游：{PAIR2_API_URL}")
    app.run(host="127.0.0.1", port=5000, debug=True)
