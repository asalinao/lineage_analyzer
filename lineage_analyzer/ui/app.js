const NODE_W = 230;
const NODE_H = 76;
const COL_GAP = 260;
const EDGE_COLORS = ["#2dd4bf", "#60a5fa", "#fb7185", "#fbbf24", "#22c55e", "#a78bfa", "#f472b6", "#38bdf8", "#f97316", "#c084fc", "#bef264", "#14b8a6", "#818cf8", "#e879f9", "#fde047", "#4ade80", "#f43f5e", "#67e8f9", "#fb923c", "#d946ef", "#93c5fd", "#a3e635"];

const $ = (id) => document.getElementById(id);
const e = {
  loginScreen: $("loginScreen"), appShell: $("appShell"), loginForm: $("loginForm"),
  loginUsername: $("loginUsername"), loginPassword: $("loginPassword"), loginError: $("loginError"), loginButton: $("loginButton"),
  currentUser: $("currentUser"), logoutButton: $("logoutButton"),
  tableList: $("tableList"), tableCountCard: $("tableCountCard"), edgeCountCard: $("edgeCountCard"),
  tableCount: $("tableCount"), edgeCount: $("edgeCount"), graph: $("graph"), detailsTitle: $("detailsTitle"),
  detailsTabs: $("detailsTabs"), selectedTable: $("selectedTable"), attributeTitle: $("attributeTitle"),
  attributeList: $("attributeList"), attributeCount: $("attributeCount"), downstreamToggle: $("downstreamToggle"),
  downstreamTitle: $("downstreamTitle"), downstreamList: $("downstreamList"), downstreamCount: $("downstreamCount"),
  refreshButton: $("refreshButton"), criticalToggleButton: $("criticalToggleButton"), syncPanelButton: $("syncPanelButton"), syncPanel: $("syncPanel"),
  syncCloseButton: $("syncCloseButton"), syncReloadButton: $("syncReloadButton"), syncError: $("syncError"),
  syncList: $("syncList"), syncForm: $("syncForm"), syncId: $("syncId"), syncName: $("syncName"),
  syncSourceType: $("syncSourceType"), syncDsn: $("syncDsn"), syncNamespace: $("syncNamespace"), syncSchema: $("syncSchema"),
  syncCron: $("syncCron"), syncEnabled: $("syncEnabled"), syncSaveButton: $("syncSaveButton"),
};

const s = {
  graph: { nodes: [], edges: [] }, table: null, edge: null, attr: null, mode: "properties",
  history: [], historyLoading: false, historyError: "", oldVersion: "", newVersion: "",
  report: null, reportLoading: false, reportError: "", list: "tables", collapsed: { downstream: false, edgeSql: true },
  downstream: [], critical: new Map(), criticalHighlight: false, pos: new Map(), view: { x: 40, y: 40, scale: 1 }, pointer: null, schedules: [], syncOpen: false,
  user: null,
};

const html = (v) => String(v ?? "").replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;").replaceAll("'", "&#039;");
const svg = (name, attrs = {}) => Object.assign(document.createElementNS("http://www.w3.org/2000/svg", name), ...Object.entries(attrs).map(([k, v]) => ({ [k]: v })));
const setAttrs = (node, attrs) => (Object.entries(attrs).forEach(([k, v]) => node.setAttribute(k, v)), node);
const api = async (url, options = {}) => {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(body.error || `${response.status} ${response.statusText}`);
    return body;
  } catch (error) {
    if (error.name === "AbortError") throw new Error("Запрос выполняется слишком долго. Проверьте сервер и повторите попытку.");
    throw error;
  } finally {
    clearTimeout(timeout);
  }
};

function render() {
  e.loginScreen.hidden = Boolean(s.user);
  e.appShell.hidden = !s.user;
  if (!s.user) return;
  const engineer = isEngineer();
  e.currentUser.textContent = `${s.user.username} · ${s.user.role}`;
  e.criticalToggleButton.hidden = !engineer;
  e.syncPanelButton.hidden = !engineer;
  if (!engineer) Object.assign(s, { criticalHighlight: false, syncOpen: false });
  e.tableCount.textContent = s.graph.nodes.length;
  e.edgeCount.textContent = s.graph.edges.length;
  e.tableCountCard.classList.toggle("active", s.list === "tables");
  e.edgeCountCard.classList.toggle("active", s.list === "edges");
  e.criticalToggleButton.classList.toggle("active", s.criticalHighlight);
  renderList();
  renderGraph();
  renderDetails();
  renderSchedules();
}

async function loadGraph() {
  const [graph, critical] = await Promise.all([api("/graph"), isEngineer() ? api("/analysis/critical") : Promise.resolve([])]);
  s.graph = graph;
  s.critical = new Map(critical.map((x) => [x.table, x]));
  Object.assign(s, { table: null, edge: null, attr: null, downstream: [], pos: layout(s.graph.nodes, s.graph.edges) });
  resetAnalysis();
  fit();
  render();
}

async function initAuth() {
  try {
    const me = await api("/auth/me");
    s.user = me.authenticated ? me.user : null;
    if (s.user) await loadGraph();
    else render();
  } catch {
    s.user = null;
    render();
  }
}

async function login(event) {
  event.preventDefault();
  e.loginError.hidden = true;
  e.loginButton.disabled = true;
  e.loginButton.textContent = "Вход...";
  try {
    const result = await api("/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: e.loginUsername.value, password: e.loginPassword.value }),
    });
    s.user = result.user;
    e.loginPassword.value = "";
    await loadGraph();
  } catch (err) {
    e.loginError.textContent = err.message;
    e.loginError.hidden = false;
  } finally {
    e.loginButton.disabled = false;
    e.loginButton.textContent = "Войти";
  }
}

async function logout() {
  await api("/auth/logout", { method: "POST" }).catch(() => {});
  Object.assign(s, { user: null, graph: { nodes: [], edges: [] }, table: null, edge: null, attr: null, downstream: [], critical: new Map(), criticalHighlight: false, syncOpen: false });
  render();
}

function renderList() {
  const items = s.list === "tables" ? [...s.graph.nodes].sort((a, b) => a.id.localeCompare(b.id)) : [...s.graph.edges].sort((a, b) => edgeName(a).localeCompare(edgeName(b)));
  const downstream = new Set(s.downstream.map((x) => x.table));
  e.tableList.replaceChildren(...items.map((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = s.list === "tables"
      ? ["table-item", item.id === s.table && "active", downstream.has(item.id) && "downstream"].filter(Boolean).join(" ")
      : ["table-item", "edge-item", edgeId(item) === s.edge && "active"].filter(Boolean).join(" ");
    button.innerHTML = s.list === "tables"
      ? `<span class="table-name">${html(shortName(item.id))}</span><span class="table-meta">${item.attributes.length} атриб. · ${inCount(item.id)} in · ${outCount(item.id)} out</span>`
      : `<span class="table-name">${html(shortJobName(item))}</span><span class="table-meta edge-meta"><span>${html(shortName(item.source))} →</span><span>→ ${html(shortName(item.target))}</span><span>${item.attributes.length} трансф.</span></span>`;
    button.onclick = () => s.list === "tables" ? selectTable(item.id) : selectEdge(edgeId(item));
    return button;
  }));
}

async function selectTable(id) {
  Object.assign(s, { table: id, edge: null, attr: null, list: "tables", downstream: [] });
  resetAnalysis();
  s.collapsed.downstream = true;
  render();
  s.downstream = (await api(`/graph/downstream?table=${encodeURIComponent(id)}`)).downstream;
  render();
}

function selectEdge(id) {
  Object.assign(s, { table: null, edge: id, attr: null, list: "edges", downstream: [] });
  resetAnalysis();
  s.collapsed.edgeSql = true;
  render();
}

function renderDetails() {
  const node = s.graph.nodes.find((x) => x.id === s.table);
  const edge = s.graph.edges.find((x) => edgeId(x) === s.edge);
  e.attributeCount.textContent = node ? node.attributes.length : edge ? edge.attributes.length : "";
  if (!node && !edge) return clearDetails();
  e.detailsTitle.textContent = "Свойства объекта";
  e.detailsTabs.hidden = false;
  [...e.detailsTabs.children].forEach((button) => button.classList.toggle("active", button.dataset.mode === s.mode));
  if (s.mode === "versions") return renderVersions(node);
  if (s.mode === "impact") return renderImpact();
  return edge ? renderEdge(edge) : renderNode(node);
}

function clearDetails() {
  e.detailsTitle.textContent = "";
  e.detailsTabs.hidden = true;
  e.selectedTable.className = "empty";
  e.selectedTable.textContent = "";
  e.attributeTitle.textContent = "";
  e.attributeCount.textContent = "";
  e.attributeList.className = "attribute-table empty";
  e.attributeList.textContent = "";
  e.downstreamTitle.textContent = "";
  e.downstreamCount.textContent = "";
  e.downstreamList.hidden = false;
  e.downstreamList.className = "downstream-list empty";
  e.downstreamList.textContent = "";
}

function renderNode(node) {
  e.selectedTable.className = "selected-card";
  e.selectedTable.innerHTML = `<dl><div><dt>Название</dt><dd>${html(tableName(node))}</dd></div><div><dt>База данных</dt><dd>${html(node.namespace)}</dd></div><div><dt>Связи</dt><dd>${inCount(node.id)} входящих · ${outCount(node.id)} исходящих</dd></div>${isEngineer() ? `<div><dt>Рейтинг критичности</dt><dd>${html(criticalScore(node.id))}</dd></div>` : ""}</dl>`;
  e.attributeTitle.textContent = "Атрибуты";
  e.downstreamTitle.textContent = title("Зависимости", "downstream");
  e.downstreamCount.textContent = s.downstream.length;
  renderAttributes(node);
  renderDownstream();
}

function renderEdge(edge) {
  e.selectedTable.className = "selected-card";
  e.selectedTable.innerHTML = `<dl><div><dt>Название</dt><dd>${html(noDb(edgeName(edge)))}</dd></div><div><dt>База данных</dt><dd>${html(edgeDb(edge))}</dd></div><div><dt>Источник</dt><dd>${html(noDb(edge.source))}</dd></div><div><dt>Выход</dt><dd>${html(noDb(edge.target))}</dd></div></dl>`;
  e.attributeTitle.textContent = "Трансформации";
  e.attributeList.className = "attribute-table";
  e.attributeList.innerHTML = edge.attributes.map((a) => `<div class="transformation-card"><strong>${html(a.input_attribute || "dataset")} → ${html(a.output_attribute || "dataset")}</strong><dl><div><dt>Type</dt><dd>${html(a.lineage_subtype || "unknown")}</dd></div>${a.lineage_description ? `<div><dt>Description</dt><dd>${html(a.lineage_description)}</dd></div>` : ""}${a.expression ? `<div><dt>Expression</dt><dd>${html(a.expression)}</dd></div>` : ""}</dl></div>`).join("");
  e.downstreamTitle.textContent = title("SQL код", "edgeSql");
  e.downstreamCount.textContent = "";
  e.downstreamList.hidden = s.collapsed.edgeSql;
  if (!edge.job_sql.length) {
    e.downstreamList.className = "attribute-table empty";
    e.downstreamList.textContent = "SQL код не найден.";
    return;
  }
  e.downstreamList.className = "attribute-table";
  e.downstreamList.replaceChildren(...edge.job_sql.map((item) => {
    const card = document.createElement("article");
    card.className = "sql-code-card";
    const code = document.createElement("pre");
    code.textContent = item.sql;
    card.append(code);
    return card;
  }));
}

function renderAttributes(node) {
  e.attributeList.className = node.attributes.length ? "attribute-table" : "attribute-table empty";
  if (!node.attributes.length) {
    e.attributeList.textContent = "Атрибуты не найдены.";
    return;
  }
  e.attributeList.replaceChildren(...node.attributes.map((a, i) => {
    const open = s.attr === a.name;
    const id = `lineage-${safeId(node.id)}-${safeId(a.name)}-${i}`;
    const item = document.createElement("article");
    item.className = `attribute-item ${open ? "expanded" : ""}`;
    item.innerHTML = `<button class="attribute-header" type="button" aria-expanded="${open}" aria-controls="${id}"><span class="attribute-chevron">${open ? "⌄" : "›"}</span><strong class="attribute-name" title="${html(a.name)}">${html(a.name)}</strong><span class="attribute-type" title="${html(a.data_type)}">${html(a.data_type)}</span></button>`;
    item.firstChild.onclick = () => (s.attr = open ? null : a.name, renderDetails());
    if (open) item.append(attributeLineageView(node.id, a.name, id));
    return item;
  }));
}

function attributeLineageView(table, attr, id) {
  const box = document.createElement("div");
  box.className = "attribute-lineage";
  box.id = id;
  box.innerHTML = `<div class="lineage-title"><span>История преобразования</span></div>`;
  const lineage = attributeLineage(table, attr);
  if (lineage.length <= 1) {
    box.append(stateLineage("Линейдж для атрибута не найден"));
    return box;
  }
  const graph = document.createElement("div");
  graph.className = "lineage-graph";
  graph.innerHTML = `<div class="lineage-rail"></div>`;
  lineage.forEach((step, i) => {
    graph.append(lineageNode(step));
    if (lineage[i + 1]?.transformation) graph.append(lineageTransform(lineage[i + 1].transformation));
  });
  box.append(graph);
  return box;
}

function lineageNode(step) {
  const row = document.createElement("article");
  row.className = `lineage-node ${step.current ? "current" : ""}`;
  const dataset = lineageDataset(step.dataset);
  row.innerHTML = `<span class="lineage-dot"></span><div class="lineage-node-card"><strong title="${html(step.attribute)}">${html(step.attribute)}</strong><span title="${html(dataset)}">${html(dataset)}</span></div>`;
  return row;
}

function lineageTransform(text) {
  const row = document.createElement("div");
  row.className = "lineage-transform-row";
  row.innerHTML = `<span class="lineage-transform-connector"></span><div class="lineage-transform-card"><span class="lineage-transform-label">Преобразование</span><p>${html(text)}</p></div>`;
  return row;
}

function stateLineage(text) {
  const node = document.createElement("div");
  node.className = "lineage-state";
  node.textContent = text;
  return node;
}

function renderDownstream() {
  const distances = distancesFrom(s.table);
  const items = [...s.downstream].sort((a, b) => byDistance(a.table, b.table, distances));
  e.downstreamList.hidden = s.collapsed.downstream;
  if (!items.length) {
    e.downstreamList.className = "downstream-list empty";
    e.downstreamList.textContent = "Зависимости не найдены.";
    return;
  }
  e.downstreamList.className = "downstream-list";
  e.downstreamList.replaceChildren(...items.map((x) => {
    const card = document.createElement("article");
    card.className = "downstream-card";
    card.innerHTML = `<strong>${html(x.table)}</strong>${x.attributes.map((a) => `<div class="dependency-row"><div>${html(a.name)}</div><span>родитель: ${a.parent_attributes.map(html).join(", ")}</span></div>`).join("")}`;
    card.onclick = () => selectTable(x.table);
    return card;
  }));
}

async function loadHistory() {
  const node = s.graph.nodes.find((x) => x.id === s.table);
  const edge = s.graph.edges.find((x) => edgeId(x) === s.edge);
  const type = node ? "table" : "job";
  const name = node ? node.id : edge ? firstJob(edge) : "";
  if (!name) return;
  Object.assign(s, { historyLoading: true, historyError: "", history: [] });
  renderDetails();
  try {
    s.history = await api(`/history/${type}?name=${encodeURIComponent(name)}`);
    const versions = [...s.history].sort((a, b) => Number(a.version) - Number(b.version));
    s.oldVersion = String(versions.at(-2)?.id || versions.at(0)?.id || "");
    s.newVersion = String(versions.at(-1)?.id || "");
  } catch (err) {
    s.historyError = err.message;
  } finally {
    s.historyLoading = false;
    renderDetails();
  }
}

function renderVersions(node) {
  const type = node ? "table" : "job";
  e.selectedTable.className = "empty";
  e.selectedTable.textContent = "";
  e.attributeTitle.textContent = "Версии";
  e.attributeCount.textContent = s.history.length || "";
  e.downstreamTitle.textContent = "";
  e.downstreamCount.textContent = "";
  e.downstreamList.hidden = false;
  e.downstreamList.className = "downstream-list empty";
  e.downstreamList.textContent = "";
  if (s.historyLoading || s.historyError || !s.history.length) {
    e.attributeList.className = "attribute-table empty";
    e.attributeList.textContent = s.historyLoading ? "Загрузка версий..." : s.historyError || "История версий не найдена.";
    return;
  }
  e.attributeList.className = "attribute-table";
  const wrap = document.createElement("div");
  wrap.className = "versions-panel";
  wrap.innerHTML = `<div class="compare-form"><label>Старая версия<select id="oldVersionSelect">${s.history.map((x) => `<option value="${x.id}">v${x.version}</option>`).join("")}</select></label><label>Новая версия<select id="newVersionSelect">${s.history.map((x) => `<option value="${x.id}">v${x.version}</option>`).join("")}</select></label><button type="button">Сравнить</button><span class="compare-error" hidden>Выберите две разные версии.</span></div>`;
  const [oldSelect, newSelect] = wrap.querySelectorAll("select");
  const button = wrap.querySelector("button");
  const error = wrap.querySelector(".compare-error");
  const update = () => {
    s.oldVersion = oldSelect.value;
    s.newVersion = newSelect.value;
    button.disabled = oldSelect.value === newSelect.value;
    error.hidden = oldSelect.value !== newSelect.value;
  };
  oldSelect.value = s.oldVersion;
  newSelect.value = s.newVersion;
  oldSelect.onchange = newSelect.onchange = update;
  button.onclick = () => compareVersions(type);
  update();
  wrap.append(...s.history.map((v) => {
    const row = document.createElement("article");
    row.className = "version-row";
    row.innerHTML = `<strong>v${html(v.version)}${v.is_actual ? " актуальная" : ""}</strong><span>${html(date(v.valid_from || v.created_at))}</span><span>${html(v.attribute_count ?? v.transformation_count ?? 0)} ${v.attribute_count === undefined ? "трансф." : "атриб."}</span>`;
    return row;
  }));
  e.attributeList.replaceChildren(wrap);
}

async function compareVersions(type) {
  if (!s.oldVersion || !s.newVersion || s.oldVersion === s.newVersion) return;
  Object.assign(s, { reportLoading: true, reportError: "", report: null, mode: "impact" });
  renderDetails();
  const params = type === "table" ? `old_table_id=${s.oldVersion}&new_table_id=${s.newVersion}` : `old_job_id=${s.oldVersion}&new_job_id=${s.newVersion}`;
  try {
    s.report = await api(`/analysis/impact/${type}?${params}`);
  } catch (err) {
    s.reportError = err.message;
  } finally {
    s.reportLoading = false;
    try { render(); } catch (err) { s.reportError = `Не удалось отобразить отчет: ${err.message}`; renderImpact(); }
  }
}

function renderImpact() {
  e.selectedTable.className = "empty";
  e.selectedTable.textContent = "";
  e.attributeTitle.textContent = "";
  e.attributeCount.textContent = "";
  e.downstreamTitle.textContent = "";
  e.downstreamCount.textContent = "";
  e.downstreamList.hidden = false;
  e.downstreamList.className = "downstream-list empty";
  e.downstreamList.textContent = "";
  if (s.reportLoading || s.reportError || !s.report) {
    e.attributeList.className = "attribute-table empty";
    e.attributeList.textContent = s.reportLoading ? "Формирование отчета..." : s.reportError || "Выберите две версии и нажмите «Сравнить».";
    return;
  }
  e.attributeList.className = "impact-report";
  e.attributeList.innerHTML = `<section class="impact-block"><h3>Изменения</h3>${s.report.changes.slice(0, 5).map((x) => `<article class="impact-item"><strong>${html(x.attribute || x.output_attribute || x.output_table || "transformation")}</strong><span>${html(x.change_type)}${html(x.old_type || x.new_type ? ` | ${x.old_type || "None"} → ${x.new_type || ""}` : "")}</span></article>`).join("") || "<p>Изменений не найдено.</p>"}</section><section class="impact-block"><h3>Затронутые объекты</h3>${s.report.affected_tables.map((x) => `<article class="impact-object"><div class="impact-object-header"><strong>${html(shortName(x.name))}</strong><span>${html(x.affected_attributes)} атриб. · ${html(distanceLabel(x.distance))}</span></div><div class="impact-object-paths">${x.attributes.map((a) => `<article class="impact-path"><div class="impact-path-content"><strong>${html(a.name)}</strong>${a.change_type ? `<span>${html(a.change_type)}</span>` : ""}</div></article>`).join("") || "<p>Атрибуты не найдены.</p>"}</div></article>`).join("") || "<p>Downstream-объекты не найдены.</p>"}</section>`;
}

function renderSchedules() {
  e.syncPanel.hidden = !s.syncOpen;
  if (!s.syncOpen) return;
  if (!s.schedules.length) {
    e.syncList.className = "sync-list empty";
    e.syncList.textContent = "Расписания не настроены.";
    return;
  }
  e.syncList.className = "sync-list";
  e.syncList.innerHTML = s.schedules.map((x) => `<article class="sync-card" data-id="${x.id}"><header><div><strong>${html(x.name)}</strong><span class="table-meta">${html(x.enabled ? "включено" : "выключено")} · ${html(x.source_type || "postgresql")} · cron ${html(x.cron_expression)}</span></div></header><dl><div><dt>DSN</dt><dd>${html(x.warehouse_dsn || "")}</dd></div><div><dt>База / namespace</dt><dd>${html(x.namespace)}</dd></div><div><dt>Схема</dt><dd>${html(x.schema_name)}</dd></div><div><dt>Последний запуск</dt><dd>${html(date(x.last_run_at))}</dd></div><div><dt>Успешно</dt><dd>${html(date(x.last_success_at))}</dd></div><div><dt>Ошибка</dt><dd>${html(x.last_error || "")}</dd></div></dl><div class="sync-actions"><button type="button" data-action="edit">Редактировать</button><button type="button" data-action="toggle">${x.enabled ? "Выключить" : "Включить"}</button><button type="button" data-action="run">Запустить сейчас</button><button type="button" data-action="delete">Удалить</button></div></article>`).join("");
}

async function loadSchedules() {
  s.schedules = await api("/sync-schedules");
  renderSchedules();
}

function fillSchedule(x) {
  showSyncError();
  e.syncId.value = x.id;
  e.syncName.value = x.name || "";
  e.syncSourceType.value = x.source_type || "postgresql";
  e.syncDsn.value = "";
  e.syncDsn.placeholder = "Оставьте пустым, чтобы не менять DSN";
  e.syncNamespace.value = x.namespace || "";
  e.syncSchema.value = x.schema_name || "";
  e.syncCron.value = x.cron_expression || "";
  e.syncEnabled.checked = Boolean(x.enabled);
}

function resetSchedule() {
  showSyncError();
  e.syncForm.reset();
  e.syncId.value = "";
  e.syncDsn.placeholder = "postgresql://user:password@host:5432/";
  e.syncCron.value = "0 * * * *";
  e.syncSourceType.value = "postgresql";
  e.syncEnabled.checked = true;
}

async function saveSchedule(event) {
  event.preventDefault();
  showSyncError();
  const id = e.syncId.value;
  const body = { name: e.syncName.value, source_type: e.syncSourceType.value, warehouse_dsn: e.syncDsn.value, namespace: e.syncNamespace.value, schema_name: e.syncSchema.value, cron_expression: e.syncCron.value, enabled: e.syncEnabled.checked };
  if (!id && !body.warehouse_dsn) return showSyncError("DSN обязателен для новой настройки.");
  e.syncSaveButton.disabled = true;
  e.syncSaveButton.textContent = "Сохранение...";
  try {
    await api(id ? `/sync-schedules/${id}` : "/sync-schedules", { method: id ? "PUT" : "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    resetSchedule();
    await loadSchedules();
  } catch (err) {
    showSyncError(err.message);
  } finally {
    e.syncSaveButton.disabled = false;
    e.syncSaveButton.textContent = "Сохранить";
  }
}

async function scheduleAction(action, id, button) {
  const item = s.schedules.find((x) => String(x.id) === String(id));
  showSyncError();
  try {
    if (action === "edit") return fillSchedule(item);
    if (action === "delete") {
      if (!confirm("Удалить расписание?")) return;
      await api(`/sync-schedules/${id}`, { method: "DELETE" });
      resetSchedule();
    } else if (action === "toggle") {
      await api(`/sync-schedules/${id}`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ...item, warehouse_dsn: "", enabled: !item.enabled }) });
    } else if (action === "run") {
      const text = button.textContent;
      button.disabled = true;
      button.textContent = "Запуск...";
      await api(`/sync-schedules/${id}/run`, { method: "POST" });
      button.textContent = text;
      await loadGraph();
    }
    await loadSchedules();
  } catch (err) {
    showSyncError(err.message);
    await loadSchedules().catch(() => {});
  } finally {
    if (button) button.disabled = false;
  }
}

function showSyncError(message = "") {
  e.syncError.hidden = !message;
  e.syncError.textContent = message;
}

function renderGraph() {
  const g = e.graph;
  const w = g.clientWidth || 900;
  const h = g.clientHeight || 650;
  const trace = selectedTrace();
  const selectedEdge = s.graph.edges.find((x) => edgeId(x) === s.edge);
  g.replaceChildren();
  g.setAttribute("viewBox", `0 0 ${w} ${h}`);
  const defs = svg("defs");
  defs.innerHTML = EDGE_COLORS.map((c, i) => `<marker id="arrow-${i}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="${c}"></path></marker>`).join("");
  const view = setAttrs(svg("g"), { transform: `translate(${s.view.x}, ${s.view.y}) scale(${s.view.scale})` });
  const edges = svg("g");
  const nodes = svg("g");
  view.append(edges, nodes);
  g.append(defs, view);
  for (const edge of s.graph.edges) {
    const a = s.pos.get(edge.source);
    const b = s.pos.get(edge.target);
    if (!a || !b) continue;
    const active = trace.edges.has(edgeId(edge)) || edgeId(edge) === s.edge;
    const dim = (s.table || s.edge) && !active;
    const color = edgeColor(edge);
    const d = edgePath(a, b);
    const path = setAttrs(svg("path"), { d, class: `edge ${active ? "highlight" : ""} ${dim ? "dimmed" : ""}`, stroke: EDGE_COLORS[color], "marker-end": `url(#arrow-${color})` });
    const hit = setAttrs(svg("path"), { d, class: "edge-hit-area" });
    hit.onpointerdown = (event) => (event.stopPropagation(), selectEdge(edgeId(edge)));
    edges.append(path, hit);
  }
  for (const node of s.graph.nodes) {
    const p = s.pos.get(node.id);
    if (!p) continue;
    const selected = node.id === s.table;
    const endpoint = selectedEdge && (node.id === selectedEdge.source || node.id === selectedEdge.target);
    const downstream = trace.nodes.has(node.id) && !selected;
    const dim = (s.table || s.edge) && !selected && !downstream && !endpoint;
    const critical = s.criticalHighlight && criticalScoreValue(node.id) >= criticalThreshold();
    const group = setAttrs(svg("g"), { class: `node ${selected || endpoint ? "selected" : ""} ${downstream ? "downstream" : ""} ${critical ? "critical" : ""} ${dim ? "dimmed" : ""}`, transform: `translate(${p.x}, ${p.y})`, "data-id": node.id });
    group.innerHTML = `<rect width="${NODE_W}" height="${NODE_H}" rx="8"></rect><text x="14" y="25" class="label">${html(trim(shortName(node.id), 34))}</text><text x="14" y="48" class="small">${node.attributes.length} атриб. · ${inCount(node.id)} in · ${outCount(node.id)} out</text>`;
    group.onpointerdown = (event) => (event.stopPropagation(), selectTable(node.id));
    nodes.append(group);
  }
}

function layout(nodes, edges) {
  const incoming = new Map(nodes.map((x) => [x.id, 0]));
  edges.forEach((x) => incoming.set(x.target, (incoming.get(x.target) || 0) + 1));
  const depth = new Map(nodes.map((x) => [x.id, 0]));
  const queue = nodes.filter((x) => !incoming.get(x.id)).map((x) => x.id);
  while (queue.length) {
    const current = queue.shift();
    edges.filter((x) => x.source === current).forEach((x) => {
      const next = (depth.get(current) || 0) + 1;
      if (next > (depth.get(x.target) || 0)) depth.set(x.target, next), queue.push(x.target);
    });
  }
  const groups = new Map();
  nodes.forEach((x) => groups.set(depth.get(x.id) || 0, [...(groups.get(depth.get(x.id) || 0) || []), x]));
  const pos = new Map();
  [...groups.entries()].sort(([a], [b]) => a - b).forEach(([_, col], ci) => col.sort((a, b) => a.id.localeCompare(b.id)).forEach((x, ri) => pos.set(x.id, { x: ci * (NODE_W + COL_GAP), y: ri * (NODE_H + 100) })));
  return pos;
}

function edgePath(a, b) {
  const x1 = a.x + NODE_W, y1 = a.y + NODE_H / 2, x2 = b.x, y2 = b.y + NODE_H / 2, mid = Math.max(70, Math.abs(x2 - x1) * 0.45);
  return `M ${x1} ${y1} C ${x1 + mid} ${y1}, ${x2 - mid} ${y2}, ${x2} ${y2}`;
}

function fit() {
  if (!s.pos.size) return;
  const xs = [...s.pos.values()].map((p) => p.x);
  const ys = [...s.pos.values()].map((p) => p.y);
  const minX = Math.min(...xs), minY = Math.min(...ys), maxX = Math.max(...xs) + NODE_W, maxY = Math.max(...ys) + NODE_H;
  const scale = Math.min(1.1, Math.max(0.3, Math.min(((e.graph.clientWidth || 900) - 100) / (maxX - minX || 1), ((e.graph.clientHeight || 650) - 100) / (maxY - minY || 1))));
  s.view = { scale, x: 50 - minX * scale, y: 50 - minY * scale };
}

function selectedTrace() {
  const nodes = new Set();
  const edges = new Set();
  if (s.report?.affected_tables?.length) {
    s.report.affected_tables.forEach((x) => nodes.add(x.name));
    return { nodes, edges };
  }
  if (!s.table) return { nodes, edges };
  const map = edgeMap("source");
  const queue = [s.table], seen = new Set(queue);
  while (queue.length) {
    for (const edge of map.get(queue.shift()) || []) {
      edges.add(edgeId(edge));
      nodes.add(edge.target);
      if (!seen.has(edge.target)) seen.add(edge.target), queue.push(edge.target);
    }
  }
  return { nodes, edges };
}

function attributeLineage(table, attr) {
  const found = [], queue = [{ table, attr, depth: 0 }], seen = new Set(), uniq = new Set();
  while (queue.length) {
    const cur = queue.shift(), key = `${cur.table}.${cur.attr}`;
    if (seen.has(key)) continue;
    seen.add(key);
    s.graph.edges.filter((x) => x.target === cur.table).forEach((edge) => edge.attributes.forEach((t) => {
      if (t.output_attribute !== cur.attr || !t.input_attribute) return;
      const sourceKey = `${edge.source}.${t.input_attribute}`;
      if (!uniq.has(sourceKey)) {
        uniq.add(sourceKey);
        found.push({ dataset: edge.source, attribute: t.input_attribute, depth: cur.depth + 1, current: false, transformation: transformText(t) });
      }
      queue.push({ table: edge.source, attr: t.input_attribute, depth: cur.depth + 1 });
    }));
  }
  found.sort((a, b) => a.depth - b.depth || `${a.dataset}.${a.attribute}`.localeCompare(`${b.dataset}.${b.attribute}`));
  return [{ dataset: table, attribute: attr, depth: 0, current: true, transformation: "" }, ...found];
}

function distancesFrom(table) {
  const distances = new Map(), queue = [{ table, d: 0 }], seen = new Set([table]), map = edgeMap("source");
  while (queue.length) {
    const cur = queue.shift();
    for (const edge of map.get(cur.table) || []) if (!seen.has(edge.target)) seen.add(edge.target), distances.set(edge.target, cur.d + 1), queue.push({ table: edge.target, d: cur.d + 1 });
  }
  return distances;
}

function edgeMap(field) {
  const map = new Map();
  s.graph.edges.forEach((x) => map.set(x[field], [...(map.get(x[field]) || []), x]));
  return map;
}

function resetAnalysis() {
  Object.assign(s, { mode: "properties", history: [], historyLoading: false, historyError: "", oldVersion: "", newVersion: "", report: null, reportLoading: false, reportError: "" });
}

function clearSelection() {
  Object.assign(s, { table: null, edge: null, attr: null, downstream: [] });
  resetAnalysis();
  render();
}

function startPan(event) {
  if (event.target.closest?.(".node")) return;
  s.pointer = { x: event.clientX, y: event.clientY, sx: s.view.x, sy: s.view.y, moved: false, empty: event.target === e.graph };
  e.graph.classList.add("dragging");
  e.graph.setPointerCapture(event.pointerId);
}

function movePan(event) {
  if (!s.pointer) return;
  s.pointer.moved ||= Math.abs(event.clientX - s.pointer.x) > 3 || Math.abs(event.clientY - s.pointer.y) > 3;
  s.view.x = s.pointer.sx + event.clientX - s.pointer.x;
  s.view.y = s.pointer.sy + event.clientY - s.pointer.y;
  renderGraph();
}

function endPan() {
  const p = s.pointer;
  s.pointer = null;
  e.graph.classList.remove("dragging");
  if (p && !p.moved && p.empty) clearSelection();
}

function zoom(event) {
  event.preventDefault();
  const rect = e.graph.getBoundingClientRect();
  const before = { x: (event.clientX - rect.left - s.view.x) / s.view.scale, y: (event.clientY - rect.top - s.view.y) / s.view.scale };
  s.view.scale = Math.max(0.25, Math.min(2.2, s.view.scale * (event.deltaY < 0 ? 1.12 : 0.88)));
  s.view.x = event.clientX - rect.left - before.x * s.view.scale;
  s.view.y = event.clientY - rect.top - before.y * s.view.scale;
  renderGraph();
}

const byDistance = (a, b, d) => (d.get(a) ?? Number.MAX_SAFE_INTEGER) - (d.get(b) ?? Number.MAX_SAFE_INTEGER) || String(a).localeCompare(String(b));
const date = (v) => v ? String(v).replace("T", " ").replace(/\.\d+/, "") : "";
const title = (t, key) => `${s.collapsed[key] ? "▸" : "▾"} ${t}`;
const edgeId = (x) => x.id;
const edgeName = (x) => x.jobs.join(", ");
const shortJobName = (x) => x.jobs.map((job) => String(job).split(".").filter(Boolean).at(-1) || String(job)).join(", ");
const firstJob = (x) => x.jobs[0];
const inCount = (id) => s.graph.edges.filter((x) => x.target === id).length;
const outCount = (id) => s.graph.edges.filter((x) => x.source === id).length;
const shortName = (v) => String(v).split(".").filter(Boolean).slice(-2).join(".") || String(v);
const tableName = (x) => x.schema && x.table_name ? `${x.schema}.${x.table_name}` : shortName(x.id);
const lineageDataset = (v) => String(v).split(".").filter(Boolean).slice(1).join(".") || String(v);
const noDb = (v) => String(v).split(",").map((x) => { const p = x.trim().split(".").filter(Boolean); return p.length > 2 ? p.slice(1).join(".") : x.trim(); }).join(", ");
const edgeDb = (x) => [x.source, x.target, ...x.jobs].map((v) => String(v).split(".").filter(Boolean)).find((p) => p.length > 2)?.[0] || "";
const safeId = (v) => String(v).replace(/[^a-zA-Z0-9_-]+/g, "-");
const trim = (v, n) => String(v).length <= n ? String(v) : `${String(v).slice(0, Math.floor(n / 2) - 1)}...${String(v).slice(-Math.floor(n / 2))}`;
const transformText = (t) => String(t.lineage_subtype || "").toUpperCase() === "IDENTITY" ? "" : String(t.expression || t.lineage_description || "");
const edgeColor = (x) => Math.abs([...firstJob(x)].reduce((h, c) => (h * 31 + c.charCodeAt(0)) | 0, 0)) % EDGE_COLORS.length;
const distanceLabel = (v) => Number.isFinite(Number(v)) && Number(v) > 0 ? `расстояние ${Number(v)} ` : "результат job";
const criticalScore = (id) => (s.critical.get(id)?.score ?? 0).toFixed(3);
const criticalScoreValue = (id) => s.critical.get(id)?.score ?? 0;
const isEngineer = () => s.user?.role === "data_engineer";
const criticalThreshold = () => {
  const scores = [...s.critical.values()].map((x) => Number(x.score)).filter(Number.isFinite).sort((a, b) => a - b);
  return scores.length ? scores[Math.ceil(scores.length * 0.9) - 1] : Infinity;
};

e.loginForm.onsubmit = login;
e.logoutButton.onclick = logout;
e.refreshButton.onclick = loadGraph;
e.criticalToggleButton.onclick = () => (s.criticalHighlight = !s.criticalHighlight, render());
e.syncPanelButton.onclick = async () => isEngineer() && (s.syncOpen = !s.syncOpen, renderSchedules(), s.syncOpen && (resetSchedule(), await loadSchedules().catch((err) => showSyncError(err.message))));
e.syncCloseButton.onclick = () => (s.syncOpen = false, renderSchedules());
e.syncReloadButton.onclick = () => loadSchedules().catch((err) => showSyncError(err.message));
e.syncForm.onsubmit = saveSchedule;
e.syncList.onclick = (event) => {
  const button = event.target.closest("button[data-action]");
  if (button) scheduleAction(button.dataset.action, button.closest(".sync-card").dataset.id, button);
};
e.detailsTabs.onclick = (event) => {
  const button = event.target.closest("button[data-mode]");
  if (!button) return;
  s.mode = button.dataset.mode;
  s.mode === "versions" && !s.history.length && !s.historyLoading ? loadHistory() : renderDetails();
};
e.downstreamToggle.onclick = () => {
  if (s.edge) s.collapsed.edgeSql = !s.collapsed.edgeSql;
  else if (s.table) s.collapsed.downstream = !s.collapsed.downstream;
  renderDetails();
};
[e.tableCountCard, e.edgeCountCard].forEach((card, i) => {
  card.onclick = () => (s.list = i ? "edges" : "tables", render());
  card.onkeydown = (event) => (event.key === "Enter" || event.key === " ") && (event.preventDefault(), card.click());
});
e.graph.onpointerdown = startPan;
e.graph.onpointermove = movePan;
e.graph.onpointerup = e.graph.onpointercancel = endPan;
e.graph.addEventListener("wheel", zoom, { passive: false });
window.onresize = renderGraph;
initAuth();
