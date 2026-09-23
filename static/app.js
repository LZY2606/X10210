"use strict";

const state = {
  branchId: 0,
  events: [],
  snapshot: null,
  incidents: [],
  compareTarget: 0,
  selectedCycleEdges: new Set(),
};

const KIND_LABEL = {
  acquire_req: "请求",
  acquire_ok: "获得",
  release: "释放",
  thread_start: "线程开始",
  thread_end: "线程结束",
  cond_wait: "cond等待",
  cond_wake: "cond唤醒",
  cond_notify: "notify",
  cond_notify_all: "notifyAll",
  cancel: "超时/取消",
};

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`${response.status}: ${text}`);
  }
  return response.json();
}

async function loadBranches(selectId = true) {
  const data = await api("/api/branches");
  const select = document.getElementById("branchSelect");
  const current = Number(select.value || 0);
  select.innerHTML = "";
  data.branches.forEach((branch) => {
    const option = document.createElement("option");
    option.value = branch.id;
    option.textContent = `#${branch.id} ${branch.name}`;
    select.appendChild(option);
  });
  select.value = state.branchId;
  if (selectId) state.branchId = Number(select.value) || current;
}

async function loadEvents() {
  const data = await api(`/api/events?branch_id=${state.branchId}`);
  state.events = data.events;
  const slider = document.getElementById("timeSlider");
  slider.max = String(Math.max(1, state.events.length));
  if (Number(slider.value) > slider.max || slider.value === "100") {
    slider.value = String(slider.max);
  }
  renderGens();
  await refreshSnapshot();
  await refreshIncidents();
}

function renderGens() {
  const select = document.getElementById("genSelect");
  const gens = [...new Set(state.events.map((event) => event.gen))].sort((a, b) => a - b);
  const keep = select.value;
  select.innerHTML = '<option value="">全部</option>';
  gens.forEach((gen) => {
    const option = document.createElement("option");
    option.value = gen;
    option.textContent = `第 ${gen} 代`;
    select.appendChild(option);
  });
  select.value = keep;
}

function currentTs() {
  const index = Number(document.getElementById("timeSlider").value);
  const event = state.events[Math.min(index, state.events.length) - 1];
  return event ? event.ts : null;
}

async function refreshSnapshot() {
  const ts = currentTs();
  const gen = document.getElementById("genSelect").value;
  const params = new URLSearchParams({ branch_id: state.branchId });
  if (ts !== null) params.set("ts", ts);
  if (gen !== "") params.set("gen", gen);
  state.snapshot = await api(`/api/snapshot?${params}`);
  document.getElementById("timeLabel").textContent =
    ts === null ? "—" : `ts=${ts} (${state.snapshot.index}/${state.snapshot.total})`;
  document.getElementById("replayInfo").textContent =
    state.snapshot.replayed ? `增量补算 ${state.snapshot.replayed} 个事件` : "";
  renderAll();
}

async function refreshIncidents() {
  const data = await api(`/api/incidents?branch_id=${state.branchId}`);
  state.incidents = data.incidents;
  renderIncidents();
}

// -------------------------------------------------------------- swimlanes
function renderSwimlanes() {
  const container = document.getElementById("swimlanes");
  const events = state.events;
  if (!events.length) {
    container.innerHTML = '<p class="muted">暂无事件，点击“载入示例轨迹”开始。</p>';
    return;
  }
  const tmin = events[0].ts;
  const tmax = events[events.length - 1].ts || events[0].ts + 1;
  const span = Math.max(1e-9, tmax - tmin);
  const threads = [...new Set(events.map((event) => event.thread))].sort();
  const byThread = new Map(threads.map((thread) => [thread, []]));
  events.forEach((event) => byThread.get(event.thread).push(event));

  container.innerHTML = "";
  threads.forEach((thread) => {
    const row = document.createElement("div");
    row.className = "lane";
    const name = document.createElement("div");
    name.className = "lane-name";
    name.textContent = thread;
    const track = document.createElement("div");
    track.className = "lane-track";

    const pending = new Map();
    byThread.get(thread).forEach((event) => {
      const x = ((event.ts - tmin) / span) * 100;
      const mark = document.createElement("div");
      mark.className = `evt ${event.kind}`;
      mark.style.left = `calc(${x}% - 7px)`;
      mark.title = `${KIND_LABEL[event.kind]} ts=${event.ts}${event.lock ? " " + event.lock : ""}`;
      mark.onclick = () => showEvent(event);
      track.appendChild(mark);

      if (event.kind === "acquire_req") pending.set(`${event.gen}:${event.lock}`, event);
      if (event.kind === "acquire_ok" || event.kind === "cancel") {
        const start = pending.get(`${event.gen}:${event.lock}`);
        if (start && event.kind === "cancel") {
          addWaitBar(track, start.ts, event.ts, tmin, span);
        }
        pending.delete(`${event.gen}:${event.lock}`);
      }
    });

    const cursor = document.createElement("div");
    cursor.className = "cursor-line";
    const ts = currentTs();
    cursor.style.left = ts === null ? "100%" : `${((ts - tmin) / span) * 100}%`;
    track.appendChild(cursor);

    row.appendChild(name);
    row.appendChild(track);
    container.appendChild(row);
  });
}

function addWaitBar(track, t0, t1, tmin, span) {
  const bar = document.createElement("div");
  bar.className = "wait-bar";
  bar.style.left = `${((t0 - tmin) / span) * 100}%`;
  bar.style.width = `${Math.max(0.4, ((t1 - t0) / span) * 100)}%`;
  track.appendChild(bar);
}

// ---------------------------------------------------------------- graphs
function layoutGraph(svgId, nodes, edges) {
  const svg = document.getElementById(svgId);
  svg.innerHTML = "";
  const width = 420;
  const columns = [...new Set(nodes.map((node) => node.column))].sort();
  const colX = Object.fromEntries(columns.map((column, i) => [column, 70 + i * (width - 140) / Math.max(1, columns.length - 1)]));
  const positions = new Map();
  columns.forEach((column) => {
    const members = nodes.filter((node) => node.column === column);
    members.forEach((node, i) => {
      positions.set(node.id, {
        x: colX[column],
        y: 40 + (i + 0.5) * (240 / Math.max(1, members.length)),
      });
    });
  });

  edges.forEach((edge) => {
    const p0 = positions.get(edge.from);
    const p1 = positions.get(edge.to);
    if (!p0 || !p1) return;
    const dx = p1.x - p0.x;
    const midX = p0.x + dx * 0.5;
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    const cyc = state.selectedCycleEdges.has(edge.ref);
    path.setAttribute(
      "d",
      `M ${p0.x} ${p0.y} C ${midX} ${p0.y}, ${midX} ${p1.y}, ${p1.x} ${p1.y}`
    );
    path.setAttribute(
      "class",
      `edge-path ${edge.kind === "wait" ? "wait" : ""} ${cyc ? "cycle" : ""} conf-${edge.confidence}`
    );
    path.onclick = () => showEdge(edge.raw);
    svg.appendChild(path);
    addArrow(svg, p0, p1, cyc);
  });

  nodes.forEach((node) => {
    const position = positions.get(node.id);
    const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("x", position.x - 46);
    rect.setAttribute("y", position.y - 15);
    rect.setAttribute("width", 92);
    rect.setAttribute("height", 30);
    rect.setAttribute("rx", 7);
    rect.setAttribute("class", node.kind === "thread" ? "node-thread" : "node-lock");
    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", position.x);
    label.setAttribute("y", position.y + 4);
    label.setAttribute("text-anchor", "middle");
    label.textContent = node.label;
    group.appendChild(rect);
    group.appendChild(label);
    svg.appendChild(group);
  });
}

function addArrow(svg, p0, p1, cyc) {
  const gx = p1.x - 14;
  const gy = p1.y;
  const polygon = document.createElementNS("http://www.w3.org/2000/svg", "polygon");
  polygon.setAttribute("points", `${p1.x - 2},${p1.y} ${gx},${gy - 5} ${gx},${gy + 5}`);
  polygon.setAttribute("fill", cyc ? "#ffc857" : "#ff6b6b");
  svg.appendChild(polygon);
}

function renderGraphs() {
  const snapshot = state.snapshot;
  const threadIds = Object.keys(snapshot.threads);
  const lockIds = Object.keys(snapshot.locks);
  const label = (id) => (id.startsWith("g") && id.includes(":") ? id.split(":")[1] : id);

  const heldNodes = [
    ...lockIds.map((id) => ({ id, label: label(id), column: 0, kind: "lock" })),
    ...[...new Set(snapshot.held_edges.map((edge) => edge.thread))].map((id) => ({
      id, label: id, column: 1, kind: "thread",
    })),
  ];
  const heldEdges = snapshot.held_edges.map((edge) => ({
    from: edge.lock, to: edge.thread, kind: "held",
    confidence: edge.confidence, ref: edge.id, raw: edge,
  }));
  layoutGraph("heldGraph", heldNodes, heldEdges);

  const waitNodes = [
    ...threadIds.map((id) => ({ id, label: id, column: 0, kind: "thread" })),
    ...lockIds.map((id) => ({ id, label: label(id), column: 1, kind: "lock" })),
  ];
  const waitEdges = snapshot.waited_edges.map((edge) => ({
    from: edge.thread, to: edge.lock, kind: "wait",
    confidence: edge.confidence, ref: edge.id, raw: edge,
  }));
  layoutGraph("waitGraph", waitNodes, waitEdges);
}

// -------------------------------------------------------------- evidence
function renderCycles() {
  const container = document.getElementById("cycleEvidence");
  const cycles = state.snapshot.cycles;
  state.selectedCycleEdges = new Set();
  if (!cycles.length) {
    container.innerHTML = '<p class="muted">当前时点未检测到环。</p>';
    return;
  }
  container.innerHTML = "";
  cycles.forEach((cycle, idx) => {
    cycle.edges.forEach((edge) => state.selectedCycleEdges.add(edge.id));
    const card = document.createElement("div");
    card.className = `cycle-card ${cycle.confidence}`;
    const header = document.createElement("div");
    header.innerHTML =
      `<strong>环 #${idx + 1}</strong> ` +
      `<span class="badge ${cycle.confidence}">置信度 ${cycle.confidence}</span><br>` +
      `<span class="muted">线程 ${cycle.threads.join(", ")} ／ 锁 ${cycle.locks.map((l) => l.split(":")[1]).join(", ")}</span>`;
    card.appendChild(header);
    cycle.participants.forEach((part) => {
      const row = document.createElement("div");
      row.className = "participant";
      const role =
        part.role === "wait"
          ? `等待 ${part.wait_kind === "cond" ? "(cond重竞争) " : ""}${part.mode}`
          : `持有 ${part.mode}×深度`;
      row.innerHTML =
        `<div>${part.thread}</div><div>${part.lock}<br><span class="muted">${role}</span></div>` +
        `<div><div class="muted">等待栈</div><pre class="stack">${(part.wait_stack || []).join("\\n") || "—"}</pre></div>` +
        `<div><div class="muted">持有栈</div><pre class="stack">${(part.hold_stack || []).join("\\n") || "—"}</pre></div>`;
      card.appendChild(row);
    });
    const eventButton = document.createElement("button");
    eventButton.textContent = "查看产生该环的事件";
    eventButton.onclick = () => {
      const all = cycle.participants.flatMap((part) => [...part.events, ...(part.hold_events || [])]);
      const unique = new Map();
      all.filter(Boolean).forEach((event) => unique.set(event.seq, event));
      showEventList([...unique.values()].sort((a, b) => a.ts - b.ts));
    };
    card.appendChild(eventButton);
    container.appendChild(card);
  });
}

function renderIncidents() {
  const container = document.getElementById("incidents");
  if (!state.incidents.length) {
    container.innerHTML = '<p class="muted">无持续环区间。</p>';
    return;
  }
  container.innerHTML = "";
  state.incidents.forEach((record) => {
    const item = document.createElement("div");
    item.className = "incident";
    item.innerHTML =
      `<strong>${record.key.filter((node) => !node.startsWith("g")).join(" → ")}</strong><br>` +
      `<span class="muted">ts ${record.start_ts} → ${record.end_ts}（事件 ${record.start_index}-${record.end_index}，置信 ${record.cycle.confidence}）</span>`;
    item.onclick = () => {
      const event = state.events[record.start_index - 1];
      if (event) {
        document.getElementById("timeSlider").value = String(record.start_index);
        refreshSnapshot();
      }
    };
    container.appendChild(item);
  });
}

// ----------------------------------------------------------- provenance
function showEdge(edge) {
  const container = document.getElementById("edgeDetail");
  container.innerHTML =
    `<p><strong>${edge.kind === "held" ? "持有边" : "等待边"}</strong> ` +
    `<span class="badge ${edge.confidence}">${edge.confidence}</span></p>` +
    `<pre>${JSON.stringify(edge, null, 2)}</pre>`;
}

function showEvent(event) {
  showEventList([event]);
}

function showEventList(events) {
  const container = document.getElementById("edgeDetail");
  const rows = events
    .map(
      (event) =>
        `<tr><td>${event.seq}</td><td>${event.ts}</td><td>${KIND_LABEL[event.kind]}</td>` +
        `<td>${event.thread}</td><td>${event.gen}</td><td>${event.lock || ""}</td>` +
        `<td>${event.cond || ""}</td><td>${event.mode}</td></tr>`
    )
    .join("");
  container.innerHTML =
    `<table class="events"><thead><tr><th>seq</th><th>ts</th><th>类型</th><th>线程</th>` +
    `<th>代次</th><th>锁</th><th>cond</th><th>模式</th></tr></thead><tbody>${rows}</tbody></table>` +
    `<pre>${JSON.stringify(events, null, 2)}</pre>`;
}

// --------------------------------------------------------------- branch
async function compareWithMain() {
  const other = state.branchId === 0 ? 1 : 0;
  const ts = currentTs();
  const params = new URLSearchParams({ a: 0, b: state.branchId === 0 ? 1 : state.branchId });
  if (ts !== null) params.set("ts", ts);
  try {
    const result = await api(`/api/compare?${params}`);
    const panel = document.getElementById("comparePanel");
    panel.hidden = false;
    const renderCyclesList = (cycles) =>
      cycles.length
        ? cycles.map((cycle) => `<li>${cycle.threads.join(", ")} / ${cycle.locks.join(", ")} (${cycle.confidence})</li>`).join("")
        : '<li class="muted">无环</li>';
    document.getElementById("compareResult").innerHTML =
      `<p>仅主分支存在：</p><ul>${result.only_in_a.map((nodes) => `<li>${nodes.join(" → ")}</li>`).join("") || '<li class="muted">—</li>'}</ul>` +
      `<p>仅当前分支存在：</p><ul>${result.only_in_b.map((nodes) => `<li>${nodes.join(" → ")}</li>`).join("") || '<li class="muted">—</li>'}</ul>` +
      `<p>受影响事件 seq：${result.changed_event_seqs.join(", ") || "无"}</p>`;
  } catch (error) {
    alert(`比较失败：${error.message}`);
  }
}

async function minimizeCycle() {
  try {
    const result = await api("/api/minimize", {
      method: "POST",
      body: JSON.stringify({ branch_id: state.branchId }),
    });
    const panel = document.getElementById("minimizePanel");
    panel.hidden = false;
    document.getElementById("minimizeResult").innerHTML =
      `<p>环 ${result.cycle_id}：${result.original_count} → <strong>${result.reduced_count}</strong> 个事件，仍复现同一环。</p>` +
      `<table class="services"></table>` +
      `<pre>${result.events
        .map((event) => `#${event.seq} t${event.ts} ${event.thread} ${KIND_LABEL[event.kind]} ${event.lock || event.cond || ""}`)
        .join("\\n")}</pre>`;
  } catch (error) {
    alert(error.message);
  }
}

// --------------------------------------------------------------- dialog
async function submitCorrection(payloadJson) {
  const name = document.getElementById("corrName").value || "校正分支";
  const branch = await api("/api/branches", {
    method: "POST",
    body: JSON.stringify({ name, parent_id: state.branchId }),
  });
  const op = document.getElementById("corrOp").value;
  const seqRaw = document.getElementById("corrSeq").value;
  const payload = payloadJson || {};
  await api(`/api/branches/${branch.branch_id}/corrections`, {
    method: "POST",
    body: JSON.stringify({
      op,
      target_seq: seqRaw === "" ? null : Number(seqRaw),
      payload,
    }),
  });
  await loadBranches(false);
  state.branchId = branch.branch_id;
  document.getElementById("branchSelect").value = String(branch.branch_id);
  await loadEvents();
  alert(`已创建分支 #${branch.branch_id}`);
}

// --------------------------------------------------------------- sample
const SAMPLE = {
  events: [
    { seq: 0, ts: 1, kind: "thread_start", thread: "T1" },
    { seq: 1, ts: 2, kind: "thread_start", thread: "T2" },
    { seq: 2, ts: 3, kind: "acquire", thread: "T1", lock: "0xA", stack: ["main:work:10", "lock_a()"] },
    { seq: 3, ts: 4, kind: "acquired", thread: "T1", lock: "0xA" },
    { seq: 4, ts: 5, kind: "acquire", thread: "T2", lock: "0xB", stack: ["worker:run:22", "lock_b()"] },
    { seq: 5, ts: 6, kind: "acquired", thread: "T2", lock: "0xB" },
    { seq: 6, ts: 7, kind: "acquire", thread: "T1", lock: "0xB", stack: ["main:work:11", "lock_b()"] },
    { seq: 7, ts: 8, kind: "acquire", thread: "T2", lock: "0xA", stack: ["worker:run:23", "lock_a()"] },
  ],
};

async function loadSample() {
  await api("/api/import", { method: "POST", body: JSON.stringify(SAMPLE) });
  document.getElementById("timeSlider").value = document.getElementById("timeSlider").max;
  await loadEvents();
}

function renderAll() {
  renderSwimlanes();
  renderGraphs();
  renderCycles();
}

document.getElementById("branchSelect").onchange = async (event) => {
  state.branchId = Number(event.target.value);
  document.getElementById("timeSlider").value = document.getElementById("timeSlider").max;
  await loadEvents();
};
document.getElementById("timeSlider").oninput = refreshSnapshot;
document.getElementById("genSelect").onchange = refreshSnapshot;
document.getElementById("loadSampleBtn").onclick = loadSample;
document.getElementById("compareBtn").onclick = compareWithMain;
document.getElementById("minimizeBtn").onclick = minimizeCycle;
document.getElementById("importBtn").onclick = () => document.getElementById("importFile").click();
document.getElementById("importFile").onchange = async (event) => {
  const file = event.target.files[0];
  if (!file) return;
  const text = await file.text();
  const parsed = JSON.parse(text);
  const body = Array.isArray(parsed) ? { events: parsed } : parsed;
  await api("/api/import", { method: "POST", body: JSON.stringify(body) });
  await loadEvents();
};
document.getElementById("newBranchBtn").onclick = () => {
  document.getElementById("correctionDialog").showModal();
};
document.getElementById("corrSubmit").onclick = async (clickEvent) => {
  clickEvent.preventDefault();
  let payload = {};
  const raw = document.getElementById("corrPayload").value.trim();
  if (raw) payload = JSON.parse(raw);
  await submitCorrection(payload);
  document.getElementById("correctionDialog").close();
};

(async function init() {
  await loadBranches();
  await loadEvents();
})();
