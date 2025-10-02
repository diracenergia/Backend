/* SCADA Lite – sin gráficos */

const API = ""; // mismo origen
const $ = (q, el=document) => el.querySelector(q);
const $$ = (q, el=document) => [...el.querySelectorAll(q)];

let operator = localStorage.getItem("operator") || "";
let currentTankId = null;
let currentPumpId = null;

/* ---------- UI base ---------- */
function toast(msg, ms=2200){
  const t = $("#toast");
  t.textContent = msg;
  t.classList.add("show");
  setTimeout(()=>t.classList.remove("show"), ms);
}
function setActiveRoute(route){
  $$(".nav a").forEach(a => a.classList.toggle("active", a.dataset.route === route));
  $$(".page").forEach(p => p.classList.remove("visible"));
  $(`#page-${route}`).classList.add("visible");
}

/* ---------- FETCH helpers ---------- */
async function apiGet(path, params){
  const url = new URL(path, window.location.origin);
  if (params) Object.entries(params).forEach(([k,v]) => {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, v);
  });
  const r = await fetch(url, {headers:{Accept:"application/json"}});
  if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
  return r.json();
}
async function apiPost(path, bodyObj){
  const r = await fetch(new URL(path, window.location.origin), {
    method: "POST",
    headers: {"Content-Type":"application/json"},
    body: JSON.stringify(bodyObj || {})
  });
  const txt = await r.text();
  let data; try { data = JSON.parse(txt); } catch { data = txt; }
  if (!r.ok) {
    const msg = (data && data.detail) ? JSON.stringify(data.detail) : txt;
    throw new Error(msg);
  }
  return data;
}

/* ---------- Dashboard ---------- */
async function loadDashboard(){
  try{
    const [tanks, pumps, alarms] = await Promise.all([
      apiGet("/tanks"),
      apiGet("/pumps"),
      apiGet("/alarms", {active: true})
    ]);
    $("#dashTanksCount").textContent = tanks.length;
    $("#dashPumpsCount").textContent = pumps.length;
    $("#dashActiveAlarms").textContent = alarms.length;
  }catch(e){
    toast("Dashboard: " + e.message);
  }
}

/* ---------- Tanques ---------- */
function renderTanksTable(rows){
  const wrap = $("#tanksTableWrap");
  if (!rows.length){
    wrap.innerHTML = `<div class="box">No hay tanques.</div>`;
    return;
  }
  wrap.innerHTML = `
  <table>
    <thead><tr>
      <th>ID</th><th>Nombre</th><th>Capacidad (L)</th><th></th>
    </tr></thead>
    <tbody>
      ${rows.map(r => `
        <tr>
          <td>${r.id}</td>
          <td>${escapeHtml(r.name || "")}</td>
          <td>${r.capacity_liters ?? "—"}</td>
          <td><button class="primary" data-tank-id="${r.id}">Ver detalle</button></td>
        </tr>
      `).join("")}
    </tbody>
  </table>`;
  $$("button[data-tank-id]", wrap).forEach(btn => {
    btn.addEventListener("click", () => showTankDetail(parseInt(btn.dataset.tankId)));
  });
}
function escapeHtml(s){ return (s ?? "").toString()
  .replaceAll("&","&amp;").replaceAll("<","&lt;").replaceAll(">","&gt;"); }
async function loadTanks(){
  try{
    const tanks = await apiGet("/tanks");
    renderTanksTable(tanks);
    if (!currentTankId && tanks.length){
      currentTankId = tanks[0].id;
      showTankDetail(currentTankId);
    }
  }catch(e){ toast("Tanques: " + e.message); }
}
async function showTankDetail(tankId){
  currentTankId = tankId;
  try{
    const latest = await apiGet(`/tanks/${tankId}/latest`);
    $("#tankLatestBox").innerHTML = `
      <div><b>Tanque #${tankId}</b></div>
      <div>Nivel: <span class="badge ${badgeForLevel(latest.level_percent)}">${(latest.level_percent ?? "—")}%</span></div>
      <div>Volumen: ${(latest.volume_l ?? "—")} L</div>
      <div>Temp: ${(latest.temperature_c ?? "—")} °C</div>
      <div>ts: ${latest.ts ?? "—"}</div>
    `;
  }catch(e){
    $("#tankLatestBox").innerHTML = `<div class="badge warn">Sin lecturas</div>`;
  }
  try{
    const cmds = await apiGet(`/tanks/${tankId}/commands`, {limit: 20});
    renderCmds("#tankCmdsWrap", cmds);
  }catch(e){
    $("#tankCmdsWrap").innerHTML = `<div class="box">No se pudo cargar comandos.</div>`;
  }
}
function badgeForLevel(pct){
  if (pct == null) return "warn";
  if (pct <= 10 || pct >= 90) return "crit";
  if (pct <= 25 || pct >= 75) return "warn";
  return "ok";
}
function renderCmds(sel, cmds){
  const el = $(sel);
  if (!cmds.length){ el.innerHTML = `<div class="box">Sin comandos.</div>`; return; }
  el.innerHTML = `
    <table>
      <thead><tr>
        <th>ID</th><th>cmd</th><th>status</th><th>ts_created</th><th>error</th>
      </tr></thead>
      <tbody>
        ${cmds.map(c => `
          <tr>
            <td>${c.id}</td>
            <td>${escapeHtml(c.cmd)}</td>
            <td>${c.status}</td>
            <td>${c.ts_created ?? ""}</td>
            <td>${escapeHtml(c.error ?? "")}</td>
          </tr>
        `).join("")}
      </tbody>
    </table>
  `;
}

/* Envío de comandos de TANQUE */
$("#page-tanks").addEventListener("click", async (ev) => {
  const btn = ev.target.closest("[data-tank-action]");
  if (!btn || !currentTankId) return;
  const action = btn.dataset.tankAction;
  try{
    const payload = buildTankPayloadFor(action);
    const body = { cmd: action, user: operatorOrDefault(), payload };
    await apiPost(`/tanks/${currentTankId}/command`, body);
    toast(`Tanque ${currentTankId}: ${action} enviado`);
    showTankDetail(currentTankId);
  }catch(e){
    toast(`Error cmd tanque: ${e.message}`);
  }
});
function buildTankPayloadFor(action){
  switch(action){
    case "SCENARIO": return { name: $("#tankScenario").value };
    case "SET_TANK_LEVEL": return { level_percent: clampNum($("#tankLevelPct").value, 0, 100) };
    case "SET_VALVE": return {
      in_pct: valOrNull($("#valveIn").value),
      out_pct: valOrNull($("#valveOut").value)
    };
    case "SET_LEAK": return { lpm: clampNum($("#leakLpm").value, 0, Infinity) };
    case "SET_NOISE": return { amp: clampNum($("#noiseAmp").value, 0, Infinity) };
    case "SET_PERIODS": return {
      period_seconds: clampNum($("#periodSeconds").value, 0.1, Infinity),
      poll_cmd_seconds: clampNum($("#pollCmdSeconds").value, 0.1, Infinity)
    };
    default: return {};
  }
}

/* ---------- Bombas ---------- */
function renderPumpsTable(rows){
  const wrap = $("#pumpsTableWrap");
  if (!rows.length){
    wrap.innerHTML = `<div class="box">No hay bombas.</div>`;
    return;
  }
  wrap.innerHTML = `
  <table>
    <thead><tr>
      <th>ID</th><th>Nombre</th><th>Modelo</th><th>Qmax (L/min)</th><th></th>
    </tr></thead>
    <tbody>
      ${rows.map(r => `
        <tr>
          <td>${r.id}</td>
          <td>${escapeHtml(r.name || "")}</td>
          <td>${escapeHtml(r.model || "")}</td>
          <td>${r.max_flow_lpm ?? "—"}</td>
          <td><button class="primary" data-pump-id="${r.id}">Ver detalle</button></td>
        </tr>
      `).join("")}
    </tbody>
  </table>`;
  $$("button[data-pump-id]", wrap).forEach(btn => {
    btn.addEventListener("click", () => showPumpDetail(parseInt(btn.dataset.pumpId)));
  });
}
async function loadPumps(){
  try{
    const pumps = await apiGet("/pumps");
    renderPumpsTable(pumps);
    if (!currentPumpId && pumps.length){
      currentPumpId = pumps[0].id;
      showPumpDetail(currentPumpId);
    }
  }catch(e){ toast("Bombas: " + e.message); }
}
async function showPumpDetail(pumpId){
  currentPumpId = pumpId;
  try{
    const latest = await apiGet(`/pumps/${pumpId}/latest`);
    $("#pumpLatestBox").innerHTML = `
      <div><b>Bomba #${pumpId}</b></div>
      <div>Estado: ${latest.is_on ? '<span class="badge ok">ON</span>' : '<span class="badge warn">OFF</span>'}</div>
      <div>Caudal: ${latest.flow_lpm ?? "—"} L/min – Presión: ${latest.pressure_bar ?? "—"} bar</div>
      <div>Modo: ${escapeHtml(latest.control_mode ?? "—")} – Lockout: ${latest.manual_lockout ? "sí":"no"}</div>
      <div>ts: ${latest.ts ?? "—"}</div>
    `;
  }catch(e){
    $("#pumpLatestBox").innerHTML = `<div class="badge warn">Sin lecturas</div>`;
  }
  try{
    const cmds = await apiGet(`/pumps/${pumpId}/commands`, {limit: 20});
    renderCmds("#pumpCmdsWrap", cmds);
  }catch(e){
    $("#pumpCmdsWrap").innerHTML = `<div class="box">No se pudo cargar comandos.</div>`;
  }
}

/* Envío de comandos de BOMBA */
$("#page-pumps").addEventListener("click", async (ev) => {
  const btn = ev.target.closest("[data-pump-action]");
  if (!btn || !currentPumpId) return;
  const action = btn.dataset.pumpAction;
  try{
    const body = (action === "SPEED")
      ? { cmd: action, user: operatorOrDefault(), speed_pct: parseInt($("#pumpSpeed").value,10) }
      : { cmd: action, user: operatorOrDefault() };
    await apiPost(`/pumps/${currentPumpId}/command`, body);
    toast(`Bomba ${currentPumpId}: ${action} enviado`);
    showPumpDetail(currentPumpId);
  }catch(e){
    toast(`Error cmd bomba: ${e.message}`);
  }
});
$("#pumpSpeed").addEventListener("input", () => {
  $("#pumpSpeedVal").textContent = `${$("#pumpSpeed").value}%`;
});

/* ---------- Alarmas ---------- */
async function loadAlarms(){
  try{
    const rows = await apiGet("/alarms", {active: true});
    const wrap = $("#alarmsTableWrap");
    if (!rows.length){
      wrap.innerHTML = `<div class="box">No hay alarmas activas.</div>`;
      return;
    }
    wrap.innerHTML = `
      <table>
        <thead><tr>
          <th>ID</th><th>Asset</th><th>Code</th><th>Sev</th><th>Mensaje</th><th>ts</th><th></th>
        </tr></thead>
        <tbody>
          ${rows.map(a => `
            <tr>
              <td>${a.id}</td>
              <td>${escapeHtml(a.asset_type)}/${a.asset_id}</td>
              <td>${escapeHtml(a.code)}</td>
              <td>${sevBadge(a.severity)}</td>
              <td>${escapeHtml(a.message ?? "")}</td>
              <td>${a.ts_raised ?? ""}</td>
              <td><button class="primary" data-ack-id="${a.id}">ACK</button></td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    `;
    $$("button[data-ack-id]", wrap).forEach(b => {
      b.addEventListener("click", () => ackAlarm(parseInt(b.dataset.ackId,10)));
    });
  }catch(e){
    toast("Alarmas: " + e.message);
  }
}
function sevBadge(sev){
  const m = (sev||"").toLowerCase();
  if (m === "critical") return `<span class="badge crit">critical</span>`;
  if (m === "warning")  return `<span class="badge warn">warning</span>`;
  return `<span class="badge ok">${escapeHtml(sev||"")}</span>`;
}
async function ackAlarm(id){
  try{
    const note = prompt("Nota (opcional) para ACK:");
    const body = { user: operatorOrDefault(), note: note || null };
    await apiPost(`/alarms/${id}/ack`, body);
    toast("ACK enviado");
    loadAlarms();
  }catch(e){
    toast("ACK error: " + e.message);
  }
}

/* ---------- Auditoría ---------- */
$("#auditFilter").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const fd = new FormData(ev.currentTarget);
  const params = Object.fromEntries([...fd.entries()].filter(([_,v]) => v !== ""));
  try{
    const rows = await apiGet("/audit", params);
    const wrap = $("#auditTableWrap");
    if (!rows.length){ wrap.innerHTML = `<div class="box">Sin resultados.</div>`; return; }
    wrap.innerHTML = `
      <table>
        <thead><tr>
          <th>ts</th><th>user</th><th>action</th><th>asset</th><th>domain</th>
          <th>asset_type</th><th>asset_id</th><th>code</th><th>severity</th><th>state</th>
        </tr></thead>
        <tbody>
          ${rows.map(r => `
            <tr>
              <td>${r.ts}</td><td>${escapeHtml(r.user || "")}</td><td>${escapeHtml(r.action || "")}</td>
              <td>${escapeHtml(r.asset || "")}</td><td>${escapeHtml(r.domain || "")}</td>
              <td>${escapeHtml(r.asset_type || "")}</td><td>${r.asset_id ?? ""}</td>
              <td>${escapeHtml(r.code || "")}</td><td>${escapeHtml(r.severity || "")}</td>
              <td>${escapeHtml(r.state || "")}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    `;
  }catch(e){
    toast("Auditoría: " + e.message);
  }
});

/* ---------- Utilidades ---------- */
function clampNum(v, min, max){
  if (v === "" || v === null || v === undefined) return undefined;
  const n = Number(v);
  if (Number.isNaN(n)) return undefined;
  return Math.max(min, Math.min(max, n));
}
function valOrNull(v){
  if (v === "" || v === null || v === undefined) return undefined;
  const n = Number(v);
  return Number.isNaN(n) ? undefined : n;
}
function operatorOrDefault(){
  const op = ($("#operatorName").value || operator || "").trim();
  return op || "ui";
}

/* ---------- Eventos globales ---------- */
$("#saveOperator").addEventListener("click", () => {
  const v = ($("#operatorName").value || "").trim();
  operator = v;
  localStorage.setItem("operator", v);
  toast("Operador guardado");
});
$(".nav").addEventListener("click", (ev) => {
  const a = ev.target.closest("a[data-route]");
  if (!a) return;
  ev.preventDefault();
  const route = a.dataset.route;
  setActiveRoute(route);
  if (route === "dashboard") loadDashboard();
  if (route === "tanks") loadTanks();
  if (route === "pumps") loadPumps();
  if (route === "alarms") loadAlarms();
});

/* ---------- Boot ---------- */
(function init(){
  $("#operatorName").value = operator;
  setActiveRoute("dashboard");
  loadDashboard();
  setInterval(loadDashboard, 10_000);
})();
