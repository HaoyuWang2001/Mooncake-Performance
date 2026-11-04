#!/usr/bin/env python3
"""webui/backend/main — HTTP 骨架与入口（路由实现见 routers/）。

零第三方依赖（stdlib http.server），双平台（Windows / Linux）。启动：
    python -m webui.backend.main [--port 8080] [--bind 127.0.0.1]

完整 API 文档见 webui/API.md（GET /api/help 同内容，前端 API 页渲染）。
安全模型：内网工具无鉴权；文件访问限定白名单根目录（outputs/、
webui/data/runs/ 可读文件；configs/、schedule/ 可读写 JSON）。
前端磁盘目录为 webui/，URL 前缀保持 /static/。
"""
import argparse
import json
import os
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from . import runmgr
from .common import ApiError
from .paths import ROOT, STATIC_DIR
from .routers import ROUTES

STATIC_MIME = {".html": "text/html; charset=utf-8",
               ".js": "application/javascript; charset=utf-8",
               ".mjs": "application/javascript; charset=utf-8",
               ".css": "text/css; charset=utf-8",
               ".json": "application/json; charset=utf-8",
               ".map": "application/json; charset=utf-8",
               ".svg": "image/svg+xml",
               ".png": "image/png",
               ".woff2": "font/woff2"}


class Handler(BaseHTTPRequestHandler):
    server_version = "MooncakeWebUI/1.0"

    def log_message(self, fmt, *args):
        sys.stderr.write("[webui] {} {}\n".format(
            self.address_string(), fmt % args))

    # -- responses ----------------------------------------------------------

    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_raw(self, data, ctype):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_static(self, name):
        # normalize: only relative subpaths of webui/web/ are served
        name = (name or "").replace("\\", "/").lstrip("/")
        full = os.path.realpath(os.path.join(STATIC_DIR, name))
        inside = full.startswith(os.path.realpath(STATIC_DIR) + os.sep)
        if not inside or not os.path.isfile(full):
            self._send_json({"error": "not found: " + name}, 404)
            return
        ext = os.path.splitext(full)[1].lower()
        with open(full, "rb") as f:
            self._send_raw(f.read(), STATIC_MIME.get(ext, "application/octet-stream"))

    # -- dispatch -----------------------------------------------------------

    def _dispatch(self, method):
        from urllib.parse import unquote_plus
        parsed = self.path.split("?", 1)
        path = parsed[0]
        query = {}
        if len(parsed) > 1:
            for kv in parsed[1].split("&"):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    query.setdefault(unquote_plus(k), []).append(unquote_plus(v))
        if path == "/" or path == "/index.html":
            if method == "GET":
                return self._send_static("index.html")
        if path.startswith("/static/"):
            if method == "GET":
                return self._send_static(path[len("/static/"):])
        if path == "/docs":
            if method == "GET":
                self._send_raw(DOCS_HTML.encode("utf-8"), "text/html; charset=utf-8")
            return
        if path == "/api/openapi.json":
            if method == "GET":
                return self._send_json(_generate_openapi())
        if path.startswith("/api/"):
            for m, pattern, fn in ROUTES:
                if m != method:
                    continue
                match = re.match(pattern, path)
                if match:
                    try:
                        result = fn(self, query, **match.groupdict())
                    except ApiError as exc:
                        return self._send_json({"error": exc.message}, exc.code)
                    except runmgr.SlotBusy as exc:
                        return self._send_json({"error": exc.reason}, 409)
                    except Exception as exc:  # never crash the server
                        return self._send_json({"error": "internal: {}".format(exc)}, 500)
                    if result is not None:
                        return self._send_json(result)
                    return
            return self._send_json({"error": "no such api: {} {}".format(method, path)}, 404)
        self._send_json({"error": "not found"}, 404)

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_PUT(self):
        self._dispatch("PUT")


def _generate_openapi():
    """Static OpenAPI 3.0 spec for the HTTP API endpoints."""
    def _p(summary, method="get"):
        return {method: {"summary": summary, "responses": {"200": {"description": "OK"}}}}
    def _q(name, typ="string", required=False, default=None):
        d = {"name": name, "in": "query", "required": required, "schema": {"type": typ}}
        if default is not None:
            d["schema"]["default"] = default
        return d
    def _post(summary):
        return {"post": {"summary": summary,
                         "requestBody": {"required": True,
                                         "content": {"application/json": {"schema": {"type": "object"}}}},
                         "responses": {"200": {"description": "OK"}}}}
    def _put(summary):
        return {"put": {"summary": summary,
                        "requestBody": {"required": True,
                                        "content": {"application/json": {"schema": {"type": "object"}}}},
                        "responses": {"200": {"description": "OK"}}}}

    paths = {}
    paths["/api/status"] = _p("System status + active run/scheduler")
    paths["/api/help"] = _p("API.md content as markdown")
    paths["/api/configs"] = _p("Config inventory (general + trees + candidates)")
    paths["/api/config"] = _p("Read a config file")
    paths["/api/config"]["get"]["parameters"] = [_q("path", required=True)]
    paths["/api/config"].update(_put("Save config (backup + validate)"))
    paths["/api/config/validate"] = _post("Validate config content without saving")
    paths["/api/run"] = _post("Launch benchmark run (mode: ssh/k8s)")
    paths["/api/run"]["post"]["responses"]["409"] = {"description": "Slot busy"}
    paths["/api/runs"] = _p("Run history (max 60)")
    paths["/api/runs/{id}"] = _p("Run metadata by id")
    paths["/api/runs/{id}/log"] = _p("Incremental log fetch by byte offset")
    paths["/api/runs/{id}/log"]["get"]["parameters"] = [_q("offset", "integer", False, 0)]
    paths["/api/runs/{id}/stop"] = _post("Stop a run (cross-platform kill + remote reap)")
    paths["/api/outputs"] = _p("List output runs")
    paths["/api/outputs/{ts}/tree"] = _p("File tree of one output run")
    paths["/api/file"] = _p("Read file (text/raw/tail)")
    paths["/api/file"]["get"]["parameters"] = [
        _q("path", required=True), _q("mode", default="text"),
        _q("offset", "integer", False, 0), _q("tail", "integer")]
    paths["/api/csv"] = _p("CSV to JSON table")
    paths["/api/csv"]["get"]["parameters"] = [_q("path", required=True), _q("max_rows", "integer", False, 5000)]
    paths["/api/schedule/plans"] = _p("List schedule plans")
    paths["/api/schedule/plan/{name}"] = _p("Read one schedule plan")
    paths["/api/schedule/plan/{name}"].update(_put("Create/overwrite plan (validate + backup)"))
    paths["/api/schedule/start"] = _post("Start scheduler")
    paths["/api/schedule/stop"] = _post("Stop scheduler")
    paths["/api/schedule/status"] = _p("Scheduler status")

    return {
        "openapi": "3.0.0",
        "info": {"title": "Mooncake-Performance WebUI API", "version": "1.0",
                 "description": "HTTP API for benchmark orchestration, results, config and scheduling"},
        "paths": paths,
    }


DOCS_HTML = """<!DOCTYPE html>
<html>
<head>
  <title>Mooncake-Performance API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = () => SwaggerUIBundle({
      url: '/api/openapi.json',
      dom_id: '#swagger-ui',
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: 'BaseLayout',
      tryItOutEnabled: true,
    });
  </script>
</body>
</html>"""


def create_server(bind="127.0.0.1", port=0):
    """构建 ThreadingHTTPServer（不启动循环）；tests 用 port=0 取随机端口。"""
    return ThreadingHTTPServer((bind, port), Handler)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Mooncake-Performance WebUI")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--bind", default="127.0.0.1",
                        help="bind address (use 0.0.0.0 to expose on LAN)")
    args = parser.parse_args(argv)
    httpd = create_server(args.bind, args.port)
    print("[webui] serving on http://{}:{}/  (root: {})".format(
        args.bind, httpd.server_address[1], ROOT), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("[webui] stopped")


if __name__ == "__main__":
    main()
