let currentBranch = "main";
let timeline = { events: [], lanes: {}, generations: [] };
let cycleMoments = [];

const $ = (id) => document.getElementById(id);

async function api(path, opts) {
  const res = await fetch(path, opts);
  if (!res.ok) throw new Error((await res.text()) || res.status);
  return res.json();
}

async function loadBranches() {
  const data = await api("/api/branches");
  for (const id of ["branchSel", "cmpA", "cmpB"]) {
    const sel = $(id);
    sel.innerHTML = "";
    for (const b of data.branches) {
      const o = document.createElement("option");
      o.value = b.name; o.textContent = b.name;
      sel.appendChild(o);
    }
  }
  $("branchSel").value = currentBranch;
  $("cmpA").value = currentBranch;
}

async function loadTimeline() {
  timeline = await api("/api/timeline?branch=" + encodeURIComponent(currentBranch));
  const moments = await api(
    "/api/cycles?branch=" + encodeURIComponent(currentBranch)
  ).catch(() => ({ moments: [] }));
  cycleMoments = moments.moments || [];
  const slider = $("timeSlider");
  slider.max = timeline.events.length;
  slider.value = timeline.events.length;
  renderLanes();
  renderCyclePick();
  await refreshState();
}

function renderLanes() {
  const box = $("lanes");
  box.innerHTML = "";
  for (const [tid, evs] of Object.entries(timeline.lanes)) {
    const lane = document.createElement("div");
    lane.className = "lane";
    const genOf = evs.length ? evs[0].gen : "";
    lane.innerHTML = '<div class="tid">' + tid + ' <small>[' + genOf + ']</small></div>';
    for (const e of evs) {
      const span = document.createElement("span");
      span.className = "ev " + e.type + (e.late ? " late" : "");
      span.title = "seq=" + e.seq + " ts=" + (e.ts ?? "-") +
        (e.lock ? " lock=" + e.lock : "") + (e.cond ? " cond=" + e.cond : "");
      span.textContent = labelEvent(e);
      lane.appendChild(span);
    }
    box.appendChild(lane);
  }
}

function labelEvent(e) {
  const map = {
    request:"请求", acquired:"获得", release:"释放", timeout:"超时",
    thread_start:"开始", thread_end:"结束", cond_wait:"等待",
    cond_notify:"通知", cond_notify_all:"通知全部", restart:"重启",
  };
  let t = map[e.type] || e.type;
  if (e.mode === "read") t += "(读)";
  if (e.mode === "write" && (e.type === "request")) t += "(写)";
  if (e.lock) t += " " + e.lock;
  return t;
}

$("timeSlider").addEventListener("input", refreshState);
$("branchSel").addEventListener("change", async () => {
  currentBranch = $("branchSel").value;
  await loadTimeline();
});

async function refreshState() {
  const idx = parseInt($("timeSlider").value, 10);
  const snap = await api(
    "/api/state?branch=" + encodeURIComponent(currentBranch) + "&index=" + idx
  );
  const lastEvent = timeline.events[idx - 1];
  $("timeLabel").textContent =
    "时点 " + idx + "/" + snap.total +
    (lastEvent ? "  ts=" + (lastEvent.ts ?? "-") + " 代次=" + lastEvent.gen : "");
  renderGraph(snap);
  renderCycles(snap.cycles);
  renderAnomalies(snap);
  const badge = $("confBadge");
  badge.textContent = "置信度：" + (snap.confidence === "high" ? "高" : "候选/存疑");
  badge.className = snap.confidence === "high" ? "conf-high" : "conf-tentative";
  badge.title = (snap.confidence_reasons || []).join(", ");
}

function renderGraph(snap) {
  const svg = $("graphSvg");
  const W = svg.clientWidth || 760, H = 360;
  svg.setAttribute("viewBox", "0 0 " + W + " " + H);
  svg.innerHTML = "";
  const threads = snap.nodes.threads;
  const locks = snap.nodes.locks;
  const pos = {};
  threads.forEach((t, i) => {
    pos["t:" + t] = { x: 90, y: 40 + i * ((H - 80) / Math.max(1, threads.length - 1)) };
  });
  locks.forEach((l, i) => {
    pos["l:" + l] = { x: W - 120, y: 40 + i * ((H - 80) / Math.max(1, locks.length - 1)) };
  });
  const ns = "http://www.w3.org/2000/svg";
  const addLine = (a, b, color, dashed, title) => {
    const ln = document.createElementNS(ns, "line");
    ln.setAttribute("x1", a.x); ln.setAttribute("y1", a.y);
    ln.setAttribute("x2", b.x); ln.setAttribute("y2", b.y);
    ln.setAttribute("stroke", color);
    ln.setAttribute("stroke-width", 1.6);
    if (dashed) ln.setAttribute("stroke-dasharray", "5,4");
    const ti = document.createElementNS(ns, "title");
    ti.textContent = title;
    ln.appendChild(ti);
    svg.appendChild(ln);
  };
  for (const e of snap.hold_edges) {
    addLine(pos["t:" + e.thread], pos["l:" + e.lock],
      e.mode === "read" ? "#7fd1ff" : "var(--hold)", false,
      "持有 " + e.lock + " (" + e.mode + ") req#" + e.evidence.request_seq);
  }
  // 线程 -> 线程等待边，映射到它们争夺的锁
  for (const e of snap.wait_edges) {
    const mid = pos["l:" + e.lock];
    if (!mid) continue;
    addLine(pos["t:" + e.thread], mid, "var(--wait)", true,
      e.thread + " 等待 " + e.lock + (e.upgrading ? "(升级)" : "") +
      "，被 " + e.blocker + " 阻塞 req#" + e.evidence.request_seq);
    addLine(mid, pos["t:" + e.blocker], "var(--bad)", false,
      e.lock + " 持有者 " + e.blocker);
  }
  for (const [key, p] of Object.entries(pos)) {
    const c = document.createElementNS(ns, "circle");
    c.setAttribute("cx", p.x); c.setAttribute("cy", p.y); c.setAttribute("r", 16);
    c.setAttribute("fill", key.startsWith("t:") ? "#26405f" : "#3a2e1c");
    c.setAttribute("stroke", key.startsWith("t:") ? "var(--accent)" : "var(--wait)");
    svg.appendChild(c);
    const txt = document.createElementNS(ns, "text");
    txt.setAttribute("x", p.x); txt.setAttribute("y", p.y + 30);
    txt.setAttribute("text-anchor", "middle");
    txt.textContent = key.slice(2);
    svg.appendChild(txt);
  }
}

function renderCycles(cycles) {
  const box = $("cycles");
  box.innerHTML = "";
  if (!cycles.length) {
    box.innerHTML = '<div class="sub">当前时点无环。拖动滑块回看历史时点。</div>';
    return;
  }
  for (const c of cycles) {
    const div = document.createElement("div");
    div.className = "cycle";
    let html = '<div class="sig">环 ' + c.signature + '</div>';
    for (const s of c.steps) {
      html += '<div class="step">线程 <b>' + s.thread + '</b> 持有 <b>' +
        (s.holding_lock || "?") + "</b>(" + (s.hold_mode || "?") +
        ")，请求 <b>" + (s.waiting_for_lock || "?") + "</b>(" +
        (s.wait_mode || "?") + (s.upgrading ? "/升级" : "") +
        ")，被 <b>" + s.blocker + "</b> 阻塞" +
        '<div class="stack">持有栈: ' + esc((s.hold_stack || []).join(" ▸ ") || "（无采集栈）") +
        '<br/>等待栈: ' + esc((s.wait_stack || []).join(" ▸ ") || "（无采集栈）") +
        '<br/>证据事件: acquired#' + (s.hold_evidence?.acquired_seq ?? "-") +
        ' request#' + (s.wait_evidence?.request_seq ?? "-") + "</div></div>";
    }
    div.innerHTML = html;
    box.appendChild(div);
  }
}

function renderAnomalies(snap) {
  const box = $("anoms");
  box.innerHTML = snap.anomalies
    .map((a) => '<div class="anom">[' + a.seq + '] ' + a.code + "：" + a.message +
      (a.lock ? "（" + a.lock + "）" : ""))
    .join("") || '<div class="sub">无异常</div>';
}

function renderCyclePick() {
  const sel = $("cyclePick");
  sel.innerHTML = "";
  if (!cycleMoments.length) {
    sel.innerHTML = '<option>无环</option>';
    return;
  }
  for (const m of cycleMoments) {
    const o = document.createElement("option");
    o.value = m.signature;
    o.textContent = m.signature + " @ts" + (m.ts ?? "?");
    sel.appendChild(o);
  }
}

async function minimizeTrace() {
  const sig = $("cyclePick").value;
  const data = await api("/api/minimize?branch=" + encodeURIComponent(currentBranch) +
    "&cycle=" + encodeURIComponent(sig));
  if (!data.found) { $("minOut").textContent = data.message || "无环"; return; }
  $("minOut").innerHTML =
    "<div>原始 " + data.original_count + " → 最小 " + data.reduced_count +
    ' 条（复现 ' + (data.reproduced ? "成功" : "失败") + "）</div>" +
    data.events.map((e) => '<span class="ev ' + e.type + '">' +
      labelEvent(e) + "</span>").join(" ");
}

async function createBranch() {
  const name = $("branchName").value.trim();
  if (!name) return alert("请输入新分支名");
  const kind = $("corrType").value;
  const target = $("corrTarget").value.trim();
  const value = $("corrValue").value.trim();
  const correction = {};
  if (kind === "remove") correction.remove = [target];
  if (kind === "update") correction.updates = { [target]: { type: value } };
  if (kind === "declare") correction.declarations = { [target]: value };
  try {
    await api("/api/branches", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, parent: currentBranch, correction }),
    });
    await loadBranches();
    currentBranch = name;
    $("branchSel").value = name;
    await loadTimeline();
  } catch (e) { alert(e.message); }
}

async function compareBranches() {
  const a = $("cmpA").value, b = $("cmpB").value;
  const data = await api("/api/compare?a=" + encodeURIComponent(a) +
    "&b=" + encodeURIComponent(b));
  $("compareOut").innerHTML =
    "<pre>" + esc(JSON.stringify({
      a: { confidence: data.a.confidence, cycles: data.a.cycle_signatures,
           holds: data.a.hold_count, waits: data.a.wait_count },
      b: { confidence: data.b.confidence, cycles: data.b.cycle_signatures,
           holds: data.b.hold_count, waits: data.b.wait_count },
      cycles_equal: data.cycles_equal,
    }, null, 2)) + "</pre>";
}

async function loadSample() {
  await api("/api/sample", { method: "POST" });
  await loadTimeline();
}

function openImport() { $("fileInput").click(); }
$("fileInput").addEventListener("change", async (ev) => {
  const file = ev.target.files[0];
  if (!file) return;
  const parsed = JSON.parse(await file.text());
  const events = Array.isArray(parsed) ? parsed : parsed.events;
  await api("/api/import", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ events, branch: "main", reset: true }),
  });
  await loadTimeline();
});

function esc(s) {
  return String(s).replace(/[&<>]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));
}

(async function init() {
  await loadBranches();
  await loadTimeline();
})();
