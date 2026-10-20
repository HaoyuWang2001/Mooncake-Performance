# schedule/ — 夜间定时串行执行

`run_schedule.py` 按计划文件在指定时刻串行运行命令（如夜间性能测试），命令输出
追加写入各自指定的文件。纯标准库，无第三方依赖。

## 用法

```bash
cp schedule/plan.example.json schedule/plan.json    # 编辑自己的计划（plan.json 已被 gitignore）
python schedule/run_schedule.py --plan schedule/plan.json
python schedule/run_schedule.py --plan schedule/plan.json --dry-run   # 核对执行计划（不真跑）
```

## 计划文件字段

| 字段 | 说明 |
| --- | --- |
| `wait_seconds` | 链式间隔（上一条完成到下一条启动的等待秒数），默认 60 |
| `on_fail` | `continue`（默认，失败记录后继续队列）/ `abort`（失败即停） |
| `tasks[].date` / `time` | 触发时刻（本地时区；`HH:MM` 或 `HH:MM:SS`）。首条必填；后续省略 = 链式（上一条完成 + wait_seconds，已过点则立即执行） |
| `tasks[].command` | 完整 shell 命令行 |
| `tasks[].output` | 该命令 stdout+stderr 的追加写入文件（自动建目录，带起止分隔头） |

详细语义见仓库主 README 10.2 节。
