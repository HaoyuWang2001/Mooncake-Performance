"use strict";
/* spdiag 文本树解析：spdiag_tree.py 生成的 spdiag_tree.<interface>.txt。
   格式约定（与生成端一致）：
   - `#` 头部注释；UNMATCHED 尾行
   - 节点行：[4字符组前缀(│   /    )]* + (├─ |└─ )? + KIND [o/n] point module stats|nodata
   - GLUE / RESIDUAL 行归属其深度-1 的节点（所属块内最后一行）
   - 数值带单位（ns/us/ms），解析后统一为 ns（ticks 无单位） */

function _sdtParseVal(s) {
  if (s === undefined || s === null) return null;
  const m = String(s).match(/^(-?[\d.]+)(ns|us|ms)?$/);
  if (!m) return null;
  let v = parseFloat(m[1]);
  if (m[2] === "us") v *= 1e3;
  else if (m[2] === "ms") v *= 1e6;
  return v;
}

function _sdtFmtNs(v) {
  if (v === null || v === undefined) return "N/A";
  const a = Math.abs(v);
  if (a >= 1e6) return (v / 1e6).toFixed(2) + "ms";
  if (a >= 1e3) return (v / 1e3).toFixed(2) + "µs";
  return v + "ns";
}

function parseTreeText(text) {
  const meta = {};
  const invariants = [];
  const unmatched = [];
  const stack = [];           // one node per depth
  let root = null;
  for (const raw of (text || "").split("\n")) {
    const line = raw.replace(/\s+$/, "");
    if (!line) continue;
    if (line.startsWith("#")) {
      const m = line.match(/^#\s*([a-z_]+):\s*(.*)$/);
      if (m) {
        if (m[1] === "invariant") invariants.push(m[2]);
        else meta[m[1]] = m[2];
      }
      continue;
    }
    if (line.startsWith("UNMATCHED ")) {
      unmatched.push(line.slice("UNMATCHED ".length));
      continue;
    }
    const m = line.match(/^((?:[│ ]{3} )*)([├└]─\u0020)?(.+)$/);
    const prefix = m[1] || "";
    const hasConn = !!m[2];
    const content = m[3];
    const depth = prefix.length / 4 + (hasConn ? 1 : 0);
    if (content.startsWith("GLUE ")) {
      const owner = stack[depth - 1];
      if (owner) owner.glue = content.slice(5);
      continue;
    }
    if (content.startsWith("RESIDUAL ")) {
      const owner = stack[depth - 1];
      const rm = content.match(/^RESIDUAL (\S+) r=(\S+) parent=(\S+) \[(OK|ALERT)\]$/);
      if (owner && rm) {
        owner.residual = { point: rm[1], r: _sdtParseVal(rm[2]),
                           parent: _sdtParseVal(rm[3]), status: rm[4] };
      }
      continue;
    }
    const nm = content.match(/^(ROOT|PHASE|NESTED|LEAF)(?: (\d+)\/(\d+))? (\S+) (\S+) (.+)$/);
    if (!nm) continue;
    const node = {
      kind: nm[1], order: nm[2] ? parseInt(nm[2], 10) : null,
      point: nm[4], module: nm[5],
      stats: null, glue: null, residual: null,
      children: [], open: true,
    };
    if (nm[6] !== "nodata") {
      const stats = {};
      for (const kv of nm[6].split(/\s+/)) {
        const i = kv.indexOf("=");
        if (i > 0) stats[kv.slice(0, i)] = _sdtParseVal(kv.slice(i + 1));
      }
      node.stats = stats;
    }
    stack.length = depth;
    if (depth === 0) {
      root = node;
    } else if (stack[depth - 1]) {
      stack[depth - 1].children.push(node);
    }
    stack[depth] = node;
  }
  return { meta, invariants, root, unmatched };
}
