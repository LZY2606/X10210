let currentBranch = "main";
let analysis = null;
let snapshotIndex = 0;

const $ = (id) => document.getElementById(id);

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || response.statusText);
  }
  return response.json();
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"
  }[char]));
}

async function loadBranches(selectId = "branchSelect") {
  const data = await api("/api/branches");
  const select = $(selectId);
  select.innerHTML = data.branches.map((branch) =>
    `<option value="${esc(branch.id)}">${esc(branch.name)} (${esc(branch.id)})</option>`
  ).join("");
  select.value = currentBranch;
}

async function refresh() {
  await loadBranches();
  $("branchSelect").value = currentBranch;
  await loadAnalysis();
}

async function loadAnalysis() {
  analysis = await api(`/api/branches/${encodeURIComponent(currentBranch)}/analysis`);
  const slider = $("timeSlider");
  const count = Math.max(0, analysis.snapshots.length - 1);
  slider.max = count;
  snapshotIndex = count;
  slider.value = count;
  render();
}

function render() {
  if (!analysis) return;
  const snapshot = analysis.snapshots[snapshotIndex];
  $("timeSlider").max = Math.max(0, analysis.snapshots.length - 1);
  if (!snapshot) {
    $("timeLabel").textContent = "没有事件";
    $("swimlanes").innerHTML = "";
    $("eventList").innerHTML = "";
    $("holdGraph").innerHTML = "";
    $("waitGraph").innerHTML = "";
    $("cycles").innerHTML = "";
    $("anomalies").innerHTML = "";
    return;
  }
  const event = snapshot.event;
  $("timeLabel").textContent = `#${snapshot.index}  采集代次 ${snapshot.generation}  时间 ${snapshot.timestamp}  事件 ${event.id} / ${event.kind}`;
  $("confidence").innerHTML = `置信状态：<span class="confidence-${snapshot.confidence}">${snapshot.confidence}</span>`;
  renderLanes(snapshot);
  renderEvents();
  renderGraphs(snapshot);
}

function renderLanes(snapshot) {
  const events = analysis.events.filter((item) => item.generation === snapshot.generation);
  const threads = Array.from(new Set(events.map((event) => event.thread))).sort();
  const times = events.map((event) => event.timestamp);
  const min = Math.min(...times);
  const max = Math.max(...times);
  const span = max - min || 1;
  const position = (timestamp) => 8 + ((timestamp - min) / span) * 84;
  const visibleEvents = events.slice(0, snapshotIndex + 1);
  $("swimlanes").innerHTML = threads.map((thread) => {
    const chips = visibleEvents.filter((event) => event.thread === thread).map((event) => {
      const cls = event.kind.includes("notify") || event.kind.startsWith("cond") ? "cond" :
        event.kind.includes("request") ? "request" :
        event.kind.includes("success") ? "success" :
        event.kind.includes("release") ? "release" :
        event.kind.includes("cancel") || event.kind.includes("timeout") ? "cancel" :
        event.kind.includes("start") || event.kind.includes("end") ? event.kind.split("_")[1] : "other";
      return `<div class="event-chip ${cls}" style="left:${position(event.timestamp)}%" title="${esc(event.id)}">${esc(event.lock || event.cond || event.kind)}</div>`;
    }).join("");
    return `<div class="lane"><div class="lane-title">${esc(thread)}</div><div class="lane-track">${chips}</div></div>`;
  }).join("");
}

function renderEvents() {
  $("eventList").innerHTML = analysis.snapshots.map((item, index) => {
    const event = item.event;
    return `<div class="event-row ${index === snapshotIndex ? "active" : ""}">
      <span>${event.timestamp}</span><span>${esc(event.kind)}</span>
      <span>${esc(event.thread)} ${esc(event.lock || event.cond || "")} <span class="muted">#${esc(event.id)}</span></span>
    </div>`;
  }).join("");
}

function edgeText(edge) {
  if (edge.kind === "holds") {
    return `${edge.thread} 持有 ${edge.lock}（${edge.mode} × ${edge.depth}）<br><span class="muted">证据：${edge.source_event_id}</span>${stackHtml(edge.stack)}`;
  }
  if (edge.kind === "condition_wait") {
    return `${edge.thread} condition 等待 ${esc(edge.lock)}<br><span class="muted">证据：${edge.source_event_id} / candidate</span>${stackHtml(edge.stack)}`;
  }
  return `${edge.thread} 等待 ${edge.lock}，当前持有者 ${edge.holder_thread || "无（已就绪）"}（${edge.request_mode}→${edge.held_mode || "-"}）<br>
    <span class="muted">等待事件：${edge.source_event_id}；持有事件：${edge.hold_event_id || "-"}；${edge.confidence}</span>
    ${stackHtml(edge.stack, "等待栈")}${stackHtml(edge.hold_stack, "持有栈")}`;
}

function stackHtml(stack, title = "栈") {
  if (!stack || !stack.length) return "";
  return `<div class="stack">${title}：\n${esc(stack.join("\n"))}</div>`;
}

function renderGraphs(snapshot) {
  $("holdGraph").innerHTML = snapshot.holds.map((edge) => `<div class="edge ${edge.kind}">${edgeText(edge)}</div>`).join("") || "<p class='muted'>暂无</p>";
  $("waitGraph").innerHTML = snapshot.waits.map((edge) => `<div class="edge ${edge.kind} ${edge.confidence === "candidate" ? "candidate" : ""}">${edgeText(edge)}</div>`).join("") || "<p class='muted'>暂无</p>";
  $("cycles").innerHTML = snapshot.cycles.map((cycle) => `<div class="cycle-card">
    <strong>环：${esc(cycle.id)}</strong>
    ${cycle.edges.map((edge) => `<div class="edge waits_for">${edgeText(edge)}</div>`).join("")}
  </div>`).join("");
  $("anomalies").innerHTML = snapshot.anomalies.slice().reverse().map((item) => `<div class="anomaly-card ${item.severity}">
    <strong>${esc(item.code)}</strong><p>${esc(item.message)}</p>
    <div class="muted">${esc(item.event_id || "")} ${esc(item.thread || "")} ${esc(item.lock || "")}</div>
  </div>`).join("");
}

async function postEvents(replace) {
  let events;
  try {
    events = JSON.parse($("eventInput").value);
  } catch (error) {
    $("status").textContent = `JSON 错误：${error.message}`;
    return;
  }
  try {
    if (replace) {
      currentBranch = `main-${Date.now()}`;
      await api("/api/branches/main/clone", {
        method: "POST",
        body: JSON.stringify({ new_branch_id: currentBranch, name: "导入重建", note: "替换导入" }),
      });
    }
    const result = await api(`/api/branches/${encodeURIComponent(currentBranch)}/events`, {
      method: "POST",
      body: JSON.stringify({ events }),
    });
    $("status").textContent = `已追加 ${result.event_ids.length} 条；复用前缀 ${result.analysis.reused_events}，重算 ${result.analysis.recomputed_events}`;
    await refresh();
  } catch (error) {
    $("status").textContent = error.message;
  }
}

$("timeSlider").addEventListener("input", () => {
  snapshotIndex = Number($("timeSlider").value);
  render();
});
$("branchSelect").addEventListener("change", async () => {
  currentBranch = $("branchSelect").value;
  await loadAnalysis();
});
$("loadSample").addEventListener("click", async () => {
  const data = await api("/api/sample");
  $("eventInput").value = JSON.stringify(data.events, null, 2);
});
$("importEvents").addEventListener("click", () => postEvents(false));
$("appendInput").addEventListener("click", () => postEvents(false));
$("replaceInput").addEventListener("click", () => postEvents(true));
$("declareType").addEventListener("click", async () => {
  try {
    await api(`/api/branches/${encodeURIComponent(currentBranch)}/lock-types`, {
      method: "PUT",
      body: JSON.stringify({ generation: "default", lock: $("typeLock").value, lock_type: $("typeKind").value }),
    });
    await loadAnalysis();
    $("status").textContent = "锁类型已声明";
  } catch (error) { $("status").textContent = error.message; }
});
$("correctBtn").addEventListener("click", async () => {
  try {
    const patch = JSON.parse($("correctPatch").value);
    const newBranch = $("correctBranch").value || `correction-${Date.now()}`;
    await api(`/api/branches/${encodeURIComponent(currentBranch)}/corrections`, {
      method: "POST",
      body: JSON.stringify({ event_id: $("correctId").value, patch, new_branch_id: newBranch, name: `校正 ${newBranch}` }),
    });
    currentBranch = newBranch;
    await refresh();
    $("status").textContent = `已形成校正分支 ${newBranch}`;
  } catch (error) { $("status").textContent = error.message; }
});
$("reduceBtn").addEventListener("click", async () => {
  try {
    const data = await api(`/api/branches/${encodeURIComponent(currentBranch)}/reduce`, {
      method: "POST",
      body: JSON.stringify({}),
    });
    $("eventInput").value = JSON.stringify(data.events, null, 2);
    $("status").textContent = `最小片段：${data.event_count} 条，移除 ${data.removed_count} 条，仍复现 ${data.target_cycle_id}`;
  } catch (error) { $("status").textContent = error.message; }
});
$("compareBtn").addEventListener("click", async () => {
  await loadBranches("compareSelect");
  $("compareDialog").showModal();
});
$("compareSelect").addEventListener("change", async () => {
  const other = $("compareSelect").value;
  const data = await api(`/api/branches/${encodeURIComponent(currentBranch)}/compare/${encodeURIComponent(other)}`);
  $("compareOutput").textContent = JSON.stringify(data, null, 2);
});
$("closeCompare").addEventListener("click", () => $("compareDialog").close());

refresh().catch((error) => { $("status").textContent = error.message; });
