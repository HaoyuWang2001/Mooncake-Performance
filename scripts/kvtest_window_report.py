"""Small HTML/JSON view of the same kvtest rows written to the scenario CSV."""
import html
import json
from pathlib import Path


def generate(local_log_dir, cfg, rows):
    out = Path(local_log_dir) / "report"
    out.mkdir(parents=True, exist_ok=True)
    report = cfg.get("report") or {}
    start = report.get("start_s", 0)
    end = report.get("end_s")
    end = cfg.get("duration") if end is None else end
    columns = ["data_size", "role", "node", "instance", "operation", "total",
               "success", "fail", "wall_s", "qps", "throughput_mb_s", "avg_ms",
               "p50_ms", "p99_ms", "p999_ms", "p9999_ms", "max_ms",
               "queue_wait_avg_ms", "queue_wait_max_ms", "notify_avg_ms",
               "notify_max_ms"]
    payload = {"report_window": [start, end], "time_basis": "request completion",
               "rows": rows}
    (out / "stats.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                                    encoding="utf-8")
    def esc(value):
        return html.escape(str(value))
    table = '<tr>' + ''.join('<th>' + esc(c) + '</th>' for c in columns) + '</tr>'
    for row in rows:
        table += '<tr>' + ''.join('<td>' + esc(row.get(c, '')) + '</td>'
                                 for c in columns) + '</tr>'
    document = '''<!doctype html><html lang="zh-CN"><meta charset="utf-8">
<title>kvtest 区间报告</title><style>body{font:15px system-ui;margin:28px}
.table{overflow:auto}table{border-collapse:collapse}td,th{padding:8px;border:1px solid #ddd;
white-space:nowrap;text-align:right}th{background:#f3f5f7}</style>
<h1>kvtest 区间报告</h1>'''
    document += '<p>统计区间：[' + esc(start) + ', ' + esc(end) + ') 秒，相对正式测量开始，按请求完成时间归窗。</p>'
    document += '<p>QPS 与吞吐以各行 wall_s 为分母。原始 samples、summary 和报告上下文保留。'
    document += '区间内无法从逐请求样本还原的全程计数（如校验失败、队列峰值）不参与区间汇总。</p>'
    document += '<div class="table"><table>' + table + '</table></div></html>'
    target = out / 'report.html'
    target.write_text(document, encoding="utf-8")
    return str(target)
