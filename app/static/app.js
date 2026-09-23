// 锁序解剖室前端
const $ = (id) => document.getElementById(id);
let state = {
  runId: null, branch: "main", index: null, candidate: 0,
  data: null, tab: "wait", edges: [], edgeIndex: new Map(),
};

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" }, ...opts,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${text.slice(0, 200)}`);
  }
  return res.json();
}

function toast(msg, bad = false) {
  const t = $("toast");
  t.textContent = msg;
  t.style.borderColor = bad ? "var(--bad)" : "var(--accent)";
  t.style.display = "block";
  clearTimeout(toast._t);
  toast._t = setTimeout(() => (t.style.display = "none"), 2600);
}

function shortLock(k) {
  return k.includes("::") ? k.split("::")[1] : k;
}
function esc(s) {
  return String(s ?? "").replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}

async function loadRuns() {
  const runs = await api("/api/runs");
  $("runSel").innerHTML = runs
    .map((r) => `<option value="${r.id}">${esc(r.name)} (${r.id.slice(0, 6)})</option>`)
    .join("");
  if (runs.length) {
    state.runId = runs[0].id;
    await selectRun();
  }
}

async function loadSamples() {
  const s = await api("/api/samples");
  $("sampleSel").innerHTML = Object.entries(s)
    .map(([k, v]) => `<option value="${k}">${esc(v.label)}</option>`).join("");
}

async function createRun(sample) {
  const label = sample
    ? $("sampleSel").selectedOptions[0].textContent
    : "空白轨迹";
  const run = await api("/api/runs", {
    method: "POST",
    body: JSON.stringify({ name: sample ? `样例·${label}` : `运行 ${Date.now() % 100000}`, sample }),
  });
  state.runId = run.id;
  await loadRunsKeep();
  $("runSel").value = run.id;
  await selectRun();
  toast(`已创建运行，载入 ${run.loaded_events} 条事件`);
}

async function loadRunsKeep() {
  const runs = await api("/api/runs");
  $("runSel").innerHTML = runs
    .map((r) => `<option value="${r.id}">${esc(r.name)} (${r.id.slice(0, 6)})</option>`)
    .join("");
}

async function selectRun() {
  state.runId = $("runSel").value;
  await loadBranches();
  await refreshEventsAndState();
  const run = await api(`/api/runs/${state.runId}`);
  $("runMeta").textContent = `${run.event_count} 条事件`;
}

async function loadBranches() {
  const branches = await api(`/api/runs/${state.runId}/branches`);
  const opts = branches
    .map((b) => `<option value="${b.id}">${esc(b.label)} ${b.id === "main" ? "" : "⎇" + b.id.slice(0, 4)}</option>`)
    .join("");
  $("branchSel").innerHTML = opts;
  $("cmpA").innerHTML = opts;
  $("cmpB").innerHTML = opts;
  $("branchSel").value = state.branch = "main";
  $("cmpA").value = "main";
  if (branches.length > 1) $("cmpB").value = branches[1].id;
}

async function refreshEventsAndState() {
  const ev = await api(
    `/api/runs/${state.runId}/branches/${state.branch}/events`);
  const total = ev.count;
  const slider = $("timeSlider");
  slider.max = total;
  if (state.index === null || state.index > total) state.index = total;
  slider.value = state.index;
  $("sliderVal").textContent = `${state.index}/${total}`;
  await fetchState();
}

async function fetchState() {
  const q = new URLSearchParams({
    index: state.index, candidate: state.candidate,
  });
  const payload = await api(
    `/api/runs/${state.runId}/branches/${state.branch}/state?${q}`);
  state.data = payload;
  state.candidate = payload.selected;
  renderAll();
}

function st() {
  return state.data.state;
}

function renderAll() {
  renderHeader();
  renderAnomalies();
  renderLanes();
  renderGraph();
  renderCycles();
  renderEventsMaybe();
}

function renderHeader() {
  const s = st();
  const conf = s.confidence;
  $("confTag").innerHTML = `<span class="tag ${conf}">置信 ${conf}</span>`;
  const d = state.data;
  $("candSel").innerHTML = Array.from({ length: d.candidate_count }, (_, i) =>
    `<option value="${i}" ${i === d.selected ? "selected" : ""}>候选 ${i}${
      i === d.canonical_index ? "（规范）" : ""}</option>`).join("");
  $("candInfo").innerHTML = d.ambiguous
    ? `检测到同时间戳乱序，共 <b>${d.candidate_count}</b> 个结构不同的合法候选图；并列重建全部保留。`
    : "事件顺序唯一确定，仅一个候选图。";
}

function renderAnomalies() {
  const items = st().anomalies;
  if (!items.length) {
    $("anomalyList").innerHTML =
      '<div class="muted">无异常：持有/等待均由显式事件支撑。</div>';
    return;
  }
  $("anomalyList").innerHTML = items.map((a) => `
    <div class="list-item">
      <span class="tag ${a.severity}">${a.severity}</span>
      <b>${esc(a.code)}</b>
      <div class="muted">${esc(a.message)}</div>
      <div class="muted mono">event=${a.event ?? "-"} thread=${esc(a.thread ?? "-")} lock=${esc(a.lock ?? "-")}</div>
    </div>`).join("");
}

// ---------------- 泳道 ----------------

function renderLanes() {
  const s = st();
  const tl = s.timeline;
  const threads = Object.keys(s.threads).sort();
  const allTs = [...tl.hold_spans, ...tl.wait_spans].flatMap((p) =>
    [p.start_ts, p.end_ts]).filter((x) => x != null);
  const events = s.holding_graph.edges.length || s.waiting_graph.edges.length
    ? null : null;
  let t0 = Math.min(...allTs, 0), t1 = Math.max(...allTs, 1);
  if (t1 === t0) t1 = t0 + 1;
  const x = (t) => 60 + ((t - t0) / (t1 - t0)) * (100 - 60 / 300 * 100);
  const xpct = (t) => {
    const usable = 100 - 22;
    return 20 + ((t - t0) / (t1 - t0)) * usable;
  };
  const tl2 = $("timeline");
  tl2.innerHTML = threads.map((t) => `<div class="lane" data-t="${esc(t)}">
      <span class="label">${esc(t)}</span></div>`).join("");
  const lanes = [...tl2.querySelectorAll(".lane")];
  const byThread = Object.fromEntries(threads.map((t) => [t, []]));
  for (const h of tl.hold_spans) {
    (byThread[h.thread] ||= []).push(h);
  }
  lanes.forEach((lane) => {
    const t = lane.dataset.t;
    for (const h of byThread[t] || []) {
      if (h.end_ts != null && h.start_ts >= t1) continue;
      const start = xpct(Math.max(h.start_ts, t0));
      const end = xpct(Math.min(h.end_ts ?? t1, t1));
      const w = Math.max(2, end - start);
      const bar = document.createElement("div");
      bar.className = "bar hold";
      bar.style.left = start + "%";
      bar.style.width = w + "%";
      bar.title = `持有 ${shortLock(h.lock)} [${h.mode}] ${h.start_ts}→${h.end_ts ?? "now"}`;
      lane.appendChild(bar);
    }
    for (const w0 of tl.wait_spans.filter((p) => p.thread === t)) {
      const start = xpct(Math.max(w0.start_ts, t0));
      const end = xpct(Math.min(w0.end_ts ?? t1, t1));
      const bar = document.createElement("div");
      bar.className = "bar " + (w0.kind === "cond" || w0.kind === "cond_reacquire"
        ? "cond" : w0.outcome === "cancelled" || w0.outcome === "thread_exit"
          ? "cancel" : "wait");
      bar.style.left = start + "%";
      bar.style.width = Math.max(2, end - start) + "%";
      bar.title = `${w0.kind} 等 ${shortLock(w0.lock)} ${w0.outcome}`;
      lane.appendChild(bar);
    }
  });
  const cursor = document.createElement("div");
  cursor.id = "cursor";
  const ratio = state.index / Math.max(1, s.total);
  cursor.style.left = `calc(20% + ${ratio * 78}% )`;
  tl2.appendChild(cursor);
}

// ---------------- 图 ----------------

function buildPositions(s, edges) {
  const threads = new Set();
  const locks = new Set();
  for (const e of edges) {
    if (e.source_kind === "thread") threads.add(e.source);
    if (e.target_kind === "thread") threads.add(e.target);
    if (e.source_kind === "lock") locks.add(e.source);
    if (e.target_kind === "lock") locks.add(e.target);
  }
  Object.keys(s.threads).forEach((t) => threads.add(t));
  const tArr = [...threads].sort();
  const lArr = [...locks].sort();
  const W = 820, H = 380;
  const pos = {};
  tArr.forEach((t, i) => {
    pos[t] = { x: 90, y: 40 + (i + 0.5) * (H - 80) / Math.max(1, tArr.length), kind: "thread" };
  });
  lArr.forEach((l, i) => {
    pos[l] = { x: W - 90, y: 40 + (i + 0.5) * (H - 80) / Math.max(1, lArr.length), kind: "lock" };
  });
  return { pos, W, H };
}

function renderGraph() {
  const s = st();
  const g = state.tab === "hold" ? s.holding_graph : s.waiting_graph;
  const edges = g.edges;
  state.edges = edges;
  state.edgeIndex = new Map();
  edges.forEach((e, i) => state.edgeIndex.set(edgeKey(e), e));
  const { pos, W, H } = buildPositions(s, edges);
  const cycleEdges = new Set();
  for (const c of s.cycles) {
    for (const ce of c.edges) {
      cycleEdges.add(`${ce.waiter}->${ce.gen}::${ce.lock}->${ce.holder}`);
    }
  }
  let svg = "";
  for (const [name, p] of Object.entries(pos)) {
    const inCycle = s.cycles.some((c) => c.threads.includes(name));
    const fill = p.kind === "thread" ? "#16314d" : "#2a2140";
    const stroke = inCycle ? "var(--bad)" : (p.kind === "thread" ? "var(--thread)" : "var(--lock)");
    svg += `<rect x="${p.x - 46}" y="${p.y - 15}" width="92" height="30" rx="15"
      fill="${fill}" stroke="${stroke}" stroke-width="${inCycle ? 2.4 : 1.4}"/>`;
    svg += `<text x="${p.x}" y="${p.y + 4}" text-anchor="middle" fill="var(--text)"
      font-size="11">${esc(p.kind === "lock" ? shortLock(name) : name)}</text>`;
  }
  edges.forEach((e, i) => {
    const a = pos[e.source], b = pos[e.target];
    if (!a || !b) return;
    const fromX = a.x + (a.x < b.x ? 46 : -46);
    const toX = b.x + (a.x < b.x ? -46 : 46);
    let color = "var(--muted)";
    if (e.kind === "holds") color = "var(--accent)";
    if (e.kind === "wait") color = "var(--warn)";
    if (e.kind === "cond_sleep") color = "var(--lock)";
    if (e.kind === "held_by") color = "#7d8bb0";
    const isCycle = cycleEdges.has(`${e.thread ? e.waiter || e.target : ""}->${e.gen}::${e.lock}->${e.thread || e.target}`);
    if (e.kind === "held_by") {
      const sig = `${e.waiter}->${e.gen}::${e.lock}->${e.thread}`;
      if (cycleEdges.has(sig)) color = "var(--bad)";
    }
    const marker = `url(#arrow-${i})`;
    svg += `<defs><marker id="arrow-${i}" markerWidth="8" markerHeight="8" refX="7" refY="3"
      orient="auto"><path d="M0,0 L7,3 L0,6" fill="${color}"/></marker></defs>`;
    svg += `<line x1="${fromX}" y1="${a.y}" x2="${toX}" y2="${b.y}"
      stroke="${color}" stroke-width="${color === "var(--bad)" ? 2.6 : 1.3}"
      marker-end="${marker}" data-ei="${i}" class="graph-edge" style="cursor:pointer"/>`;
    const lx = (fromX + toX) / 2, ly = (a.y + b.y) / 2 - 4;
    const label = e.kind === "holds" ? `持有 ${e.mode}`
      : e.kind === "wait" ? `等待 ${e.mode}`
      : e.kind === "cond_sleep" ? `cond:${shortLock(e.cond || "")}`
      : `占用 ${e.mode}`;
    svg += `<text x="${lx}" y="${ly}" text-anchor="middle" class="edge-label"
      data-ei="${i}" style="cursor:pointer">${esc(label)}</text>`;
  });
  $("graphSvg").setAttribute("viewBox", `0 0 ${W} ${H}`);
  $("graphSvg").innerHTML = svg;
  $("graphSvg").querySelectorAll("[data-ei]").forEach((el) => {
    el.addEventListener("click", () => showEdge(edges[+el.dataset.ei]));
  });
}

function edgeKey(e) {
  return `${e.source}->${e.target}:${e.kind}:${e.event}:${e.thread ?? ""}`;
}

function showEdge(e) {
  const eventList = (e.events || [e.event]).filter((x) => x != null);
  const stack = e.stack && e.stack.length
    ? e.stack.map((f) => esc(f)).join("\n")
    : "（该事件未携带栈）";
  $("edgeInfo").innerHTML = `
    <div><span class="tag ${e.kind === "cond_sleep" ? "low" : e.confidence}">${esc(e.confidence)}</span>
      <b>${esc(e.kind)}</b> ${esc(e.source)} → ${esc(shortLock(e.target))}</div>
    <div class="muted">锁类型 ${esc(e.lock_type)} · 模式 ${esc(e.mode)} · 代次 ${esc(e.gen)}</div>
    <div>回到事件 id：<b class="mono">${eventList.join(", ")}</b></div>
    <pre>${stack}</pre>`;
}

// ---------------- 环证据 ----------------

function renderCycles() {
  const s = st();
  const cycles = s.cycles;
  $("cycleCount").innerHTML = cycles.length
    ? `<span class="tag cycle">${cycles.length} 个环</span>` : "";
  if (!cycles.length) {
    $("cycleList").innerHTML = '<div class="muted">当前时点无等待环。</div>';
    return;
  }
  $("cycleList").innerHTML = cycles.map((c, i) => `
    <div class="list-item" data-ci="${i}">
      <span class="tag cycle">环 ${i + 1}</span>
      长度 ${c.length} · 线程 ${c.threads.map(esc).join("、")}
      <pre>${c.nodes.map((n) =>
        `${n.thread}  等锁 ${n.waits.lock}(${n.waits.mode}) ← ${n.waits_for}\n  等待栈: ${(n.waits.stack.join(" › ") || "—")}\n  持有: ${n.holds.map((h) => h.lock + "(" + h.mode + ")").join(", ") || "无"}`
      ).join("\n\n")}</pre>
      <div class="muted">持有/等待栈均带事件 id（见代码块每行的事件溯源面板）。</div>
    </div>`).join("");
  $("cycleList").querySelectorAll(".list-item").forEach((el) => {
    el.addEventListener("click", () => {
      const c = cycles[+el.dataset.ci];
      const n0 = c.nodes[0];
      const waitEdge = state.edges.find((x) =>
        x.kind === "wait" && x.thread === n0.thread && x.lock === n0.waits.lock);
      if (waitEdge) showEdge(waitEdge);
    });
  });
}

// ---------------- 事件表 ----------------

async function renderEventsMaybe() {
  if (state.tab !== "events") {
    $("eventTable").style.display = "none";
    $("graphArea").style.display = "";
    return;
  }
  $("eventTable").style.display = "";
  $("graphArea").style.display = "none";
  const evs = await api(
    `/api/runs/${state.runId}/branches/${state.branch}/events`);
  const rows = evs.events.map((e) => {
    const cut = state.index;
    return `<tr${e.seq + 1 <= cut ? "" : ' style="opacity:.35"'}>
      <td class="mono">${e.id}</td><td>${esc(e.gen)}</td>
      <td class="mono">${e.ts}</td><td>${esc(e.type)}</td>
      <td>${esc(e.thread)}</td><td>${esc(e.lock ?? "")}</td>
      <td>${esc(e.mode === "S" ? "S" : e.mode === "X" ? "X" : e.mode)}</td>
      <td>${esc(e.cond ?? "")}</td>
      <td title="${esc((e.stack || []).join(" / "))}">${(e.stack || []).length}</td>
    </tr>`;
  }).join("");
  $("eventTable").innerHTML = `<table><thead><tr>
    <th>id</th><th>代</th><th>ts</th><th>类型</th><th>线程</th>
    <th>锁</th><th>模式</th><th>cond</th><th>栈</th>
    </tr></thead><tbody>${rows}</tbody></table>`;
}

// ---------------- 缩减 / 比较 ----------------

async function reduceTrace() {
  const r = await api(`/api/runs/${state.runId}/reduce`, {
    method: "POST",
    body: JSON.stringify({ branch_id: state.branch }),
  });
  if (!r.found) {
    $("reduceOut").innerHTML = '<div class="muted">该轨迹当前不含环，无法缩减复现片段。</div>';
    return;
  }
  $("reduceOut").innerHTML = `
    <div class="list-item active">
      <span class="tag cycle">最小片段</span>
      保留 ${r.events.length} 条 / ${r.unit_count} 个配对单元中的 ${r.kept_units} 个，
      删除 ${r.removed_events} 条；仍复现环
      <pre>${r.events.map((e) =>
        `#${e.id} t=${e.ts} ${e.thread} ${e.type} ${e.lock ?? ""} ${e.cond ?? ""}`).join("\n")}</pre>
      <div class="muted">配对与 condition 语义随单元保持完整。</div>
    </div>`;
}

async function compareBranches() {
  const body = { branch_a: $("cmpA").value, branch_b: $("cmpB").value };
  const r = await api(`/api/runs/${state.runId}/compare`, {
    method: "POST", body: JSON.stringify(body),
  });
  const row = (label, a, b, good) => `<tr>
    <td>${label}</td><td class="${good ? "good" : ""}">${a}</td>
    <td class="${good ? "good" : ""}">${b}</td></tr>`;
  $("cmpOut").innerHTML = `<table>
    <tr><th></th><th>分支 A</th><th>分支 B</th></tr>
    ${row("置信度", r.confidence.a, r.confidence.b)}
    ${row("异常数", r.anomaly_count.a, r.anomaly_count.b)}
    ${row("环数", r.cycles_a.length, r.cycles_b.length, r.cycle_sets_equal)}
    ${row("独有事件", r.events_only_in_a.join(","), r.events_only_in_b.join(","))}
    </table>
    <div class="muted" style="margin-top:4px">
    环集合${r.cycle_sets_equal ? "一致" : "<b style='color:var(--bad)'>不一致</b>"}
    </div>
    <pre>A 异常:\n${r.anomalies_a.map((a) => a.code + " " + (a.thread || "")).join("\n") || "无"}
B 异常:
${r.anomalies_b.map((a) => a.code + " " + (a.thread || "")).join("\n") || "无"}</pre>`;
}

// ---------------- 事件追加 / 校正 ----------------

async function appendEvent() {
  const stack = $("evStack").value.split(",").map((s) => s.trim()).filter(Boolean);
  const payload = {
    type: $("evType").value,
    thread: $("evThread").value.trim(),
    lock: $("evLock").value.trim() || null,
    cond: $("evCond").value.trim() || null,
    ts: parseFloat($("evTs").value || "0"),
    mode: $("evMode").value,
    gen: $("evGen").value.trim() || "g0",
    stack,
  };
  if (!payload.thread) return toast("线程名必填", true);
  const r = await api(
    `/api/runs/${state.runId}/branches/${state.branch}/events`,
    { method: "POST", body: JSON.stringify(payload) });
  $("recomputeInfo").innerHTML =
    `重算区间 [${r.recomputed_span[0]}, ${r.recomputed_span[1]}]：` +
    (r.incremental ? "仅新增时间段（增量）" : "乱序到达，整段重算");
  await refreshEventsAndState();
  toast(`追加成功，共 ${r.total} 条事件`);
}

async function applyCorrection() {
  const evId = $("fixEvent").value.trim();
  const action = $("fixAction").value;
  const correction = action === "drop"
    ? { drop_event: parseInt(evId, 10) }
    : { override: { [evId]: JSON.parse($("fixPatch").value || "{}") } };
  const label = action === "drop" ? `删除事件 ${evId}` : `改事件 ${evId}`;
  const b = await api(`/api/runs/${state.runId}/branches`, {
    method: "POST",
    body: JSON.stringify({ parent_id: state.branch, label: `校正·${label}`, correction }),
  });
  await loadBranches();
  state.branch = b.id;
  $("branchSel").value = b.id;
  await refreshEventsAndState();
  toast(`已创建校正分支 ${b.id.slice(0, 6)}`);
}

async function declareLockType() {
  await api(`/api/runs/${state.runId}/branches/${state.branch}/lock-type`, {
    method: "POST",
    body: JSON.stringify({
      gen: $("ltGen").value.trim() || "g0",
      lock: $("ltLock").value.trim(),
      type: $("ltType").value,
    }),
  });
  await fetchState();
  toast("锁类型已声明（候选图按声明重建）");
}

// ---------------- 事件绑定 ----------------

$("newRun").addEventListener("click", () => createRun(null));
$("loadSample").addEventListener("click", () => createRun($("sampleSel").value));
$("runSel").addEventListener("change", selectRun);
$("branchSel").addEventListener("change", async () => {
  state.branch = $("branchSel").value;
  state.index = null;
  state.candidate = 0;
  await refreshEventsAndState();
});
$("timeSlider").addEventListener("input", async () => {
  state.index = parseInt($("timeSlider").value, 10);
  $("sliderVal").textContent = `${state.index}/${$("timeSlider").max}`;
  await fetchState();
});
$("candSel").addEventListener("change", async () => {
  state.candidate = parseInt($("candSel").value, 10);
  await fetchState();
});
document.querySelectorAll(".tabs button").forEach((b) => {
  b.addEventListener("click", async () => {
    document.querySelectorAll(".tabs button").forEach((x) => x.classList.remove("active"));
    b.classList.add("active");
    state.tab = b.dataset.tab;
    await renderEventsMaybe();
    renderGraph();
  });
});
$("appendEv").addEventListener("click", () => appendEvent().catch((e) => toast(e.message, true)));
$("applyFix").addEventListener("click", () => applyCorrection().catch((e) => toast(e.message, true)));
$("ltBtn").addEventListener("click", () => declareLockType().catch((e) => toast(e.message, true)));
$("reduceBtn").addEventListener("click", () => reduceTrace().catch((e) => toast(e.message, true)));
$("cmpBtn").addEventListener("click", () => compareBranches().catch((e) => toast(e.message, true)));

(async function init() {
  await loadSamples();
  await loadRuns();
})();
