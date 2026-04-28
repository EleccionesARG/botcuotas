/**
 * pulso-agent v2 — Agente de control de cuotas + Meta Ads
 * Pulso Research / MVP DATA SRL
 *
 * FIXES v2:
 *  1. notifiedQuotas persiste en Firebase → no re-spam al reiniciar Railway
 *  2. completedSurveys: detecta encuestas terminadas y deja de monitorearlas
 *  3. Cola de acciones → evita race conditions entre listeners paralelos
 *  4. META_AUTO_CONFIRM=true → ejecuta sin pedir confirmacion
 *  5. forceSyncNow corregido → await real, reporta error correctamente
 *  6. updateAdsetAge preserva targeting existente (no sobreescribe geo/intereses)
 *  7. cloneAdsetWithGender copia bid_strategy y promoted_object
 *  8. pendingActions con expiracion (2h) → no acumula entradas infinitas
 *  9. Auth check en callbacks de Telegram
 * 10. Reporte y watchdog skippean encuestas completadas
 *
 * Comandos Telegram:
 *  /estado        - resumen de cuotas activas
 *  /estado todo   - incluye encuestas completadas
 *  /sync          - fuerza sincronizacion inmediata
 *  /salud         - estado del sistema
 *  /ayuda         - lista de comandos
 */

'use strict';

const axios = require('axios');
const { initializeApp }                          = require('firebase/app');
const { getDatabase, ref, onValue, get, set }    = require('firebase/database');

// -----------------------------------------
// CONFIG
// -----------------------------------------
const TG_TOKEN          = process.env.TG_TOKEN;
const TG_CHAT_ID        = process.env.TG_CHAT_ID;
const TG_ALLOWED_IDS    = (process.env.TG_ALLOWED_IDS || process.env.TG_CHAT_ID || '')
                            .split(',').map(s => s.trim()).filter(Boolean);
const META_TOKEN        = process.env.META_TOKEN;
const META_ACCOUNT      = process.env.META_ACCOUNT || '1580131239919455';
// FIX: nueva variable — si 'true', ejecuta acciones Meta automaticamente sin pedir confirmacion
const META_AUTO_CONFIRM = process.env.META_AUTO_CONFIRM === 'true';
const FB_API_KEY        = process.env.FB_API_KEY;
const FB_DB_URL         = process.env.FB_DB_URL || 'https://control-cuotas-pulso-default-rtdb.firebaseio.com';
const SYNC_SERVER_URL   = process.env.SYNC_SERVER_URL || 'https://controlcuotasv2-production.up.railway.app';

const AGE_GROUPS_YOUNG = '16-29';
const AGE_GROUPS_OLD   = '+30';
const SUBRANGES_OLD = [
  { label: '50-64', min: 50, max: 64 },
  { label: '30-49', min: 30, max: 49 },
];

const agentStartTime = Date.now();

// -----------------------------------------
// FIREBASE
// -----------------------------------------
let db;
try {
  const fbApp = initializeApp({ apiKey: FB_API_KEY, databaseURL: FB_DB_URL });
  db = getDatabase(fbApp);
  console.log('[firebase] Conectado a', FB_DB_URL);
} catch (e) {
  console.error('[firebase] Error:', e.message);
  process.exit(1);
}

// -----------------------------------------
// ESTADO INTERNO
// -----------------------------------------
let appConfig    = null;
let lastSyncData = {};

const pendingActions = new Map();  // cbKey → action
const pendingExpiry  = new Map();  // cbKey → timestamp de expiracion (ms)

// FIX: estas dos Sets se persisten en Firebase → sobreviven reinicios de Railway
const notifiedQuotas   = new Set(); // pulso/agentState/notifiedQuotas
const completedSurveys = new Set(); // pulso/agentState/completedSurveys

let tgOffset        = 0;
let watchdogAlerted = false;

// FIX: cola de acciones para evitar race conditions entre listeners paralelos
const actionQueue    = [];
let actionProcessing = false;

// -----------------------------------------
// UTILS
// -----------------------------------------
function norm(s) {
  return (s || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
}
function parseAgeBounds(groups) {
  return groups.map(g => {
    if (g.endsWith('+')) return [parseInt(g), 999];
    const p = g.split('-'); return [parseInt(p[0]), parseInt(p[1])];
  });
}
function getAgeGrp(edad, bounds, groups) {
  for (let i = 0; i < bounds.length; i++) {
    if (edad >= bounds[i][0] && edad <= bounds[i][1]) return groups[i];
  }
  return '?';
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function pct(c, t) { if (!t) return 0; return Math.round((c / t) * 100); }
function bar(p) {
  const f = Math.round(Math.min(p, 100) / 10);
  return '\u2588'.repeat(f) + '\u2591'.repeat(10 - f);
}
function relativeTime(isoStr) {
  if (!isoStr) return 'nunca';
  const diff = Math.floor((Date.now() - new Date(isoStr)) / 1000);
  if (diff < 60) return `hace ${diff}s`;
  if (diff < 3600) return `hace ${Math.floor(diff / 60)}min`;
  return `hace ${Math.floor(diff / 3600)}h`;
}

// Sanitiza una clave para usarla como nodo de Firebase
// Firebase no permite: . # $ / [ ] en claves
function sanitizeFBKey(k) {
  return k.replace(/\|/g, '_PIPE_').replace(/[.#$/\[\]]/g, '_');
}
function unsanitizeFBKey(k) {
  return k.replace(/_PIPE_/g, '|');
}

// -----------------------------------------
// ESTRATO LOOKUP (replica logica del dashboard)
// -----------------------------------------
const PROV_ALIASES = {
  'caba': 'caba', 'ciudad autonoma de buenos aires': 'caba', 'ciudad de buenos aires': 'caba',
  'capital federal': 'caba', 'buenos aires ciudad': 'caba',
  'buenos aires': 'buenos aires', 'provincia de buenos aires': 'buenos aires',
  'santiago del estero': 'stgo del estero', 'stgo del estero': 'stgo del estero',
  'tierra del fuego': 'tierra del fuego',
  'tierra del fuego, antartida e islas del atlantico sur': 'tierra del fuego',
  'entre rios': 'entre rios', 'entre r\u00edos': 'entre rios',
  'neuqu\u00e9n': 'neuquen', 'neuquen': 'neuquen',
  'c\u00f3rdoba': 'cordoba', 'cordoba': 'cordoba',
  'tucum\u00e1n': 'tucuman', 'tucuman': 'tucuman',
  'r\u00edo negro': 'rio negro', 'rio negro': 'rio negro',
};
function normProv(s) { const n = norm(s); return PROV_ALIASES[n] || n; }

function lookupEst(prov, depto, muestra) {
  if (!muestra || !muestra.refTable) return null;
  const pn = normProv(prov);
  const dn = norm(depto);
  if (muestra.fixedEstrato) {
    const fo = muestra.fixedEstrato;
    if (fo[pn] !== undefined) return String(fo[pn]);
    if (fo[norm(prov)] !== undefined) return String(fo[norm(prov)]);
  }
  let r;
  if (muestra.cobertura === 'provincial' || muestra.cobertura === 'municipal') {
    r = muestra.refTable.find(x => norm(x.nivel2) === dn);
    if (!r) r = muestra.refTable.find(x => norm(x.nivel2).includes(dn) || dn.includes(norm(x.nivel2)));
  } else {
    r = muestra.refTable.find(x => normProv(x.nivel1) === pn && norm(x.nivel2) === dn);
    if (!r) r = muestra.refTable.find(x => normProv(x.nivel1) === pn &&
      (norm(x.nivel2).includes(dn) || dn.includes(norm(x.nivel2))));
  }
  return r ? (r.estrato !== null && r.estrato !== undefined ? String(r.estrato) : null) : null;
}

// -----------------------------------------
// PERSISTENCIA EN FIREBASE (FIX #1)
// -----------------------------------------
async function loadAgentState() {
  try {
    const snap = await get(ref(db, 'pulso/agentState'));
    const val  = snap.val();
    if (!val) {
      console.log('[state] Sin estado previo en Firebase (primer arranque limpio)');
      return;
    }
    if (val.notifiedQuotas) {
      for (const k of Object.keys(val.notifiedQuotas)) {
        notifiedQuotas.add(unsanitizeFBKey(k));
      }
      console.log(`[state] ${notifiedQuotas.size} cuota(s) notificadas previas cargadas`);
    }
    if (val.completedSurveys) {
      for (const sid of Object.keys(val.completedSurveys)) {
        completedSurveys.add(sid);
      }
      console.log(`[state] ${completedSurveys.size} encuesta(s) completadas previas cargadas`);
    }
  } catch (e) {
    console.warn('[state] Error cargando estado desde Firebase:', e.message);
  }
}

async function saveNotifiedQuota(key) {
  notifiedQuotas.add(key);
  try {
    await set(ref(db, `pulso/agentState/notifiedQuotas/${sanitizeFBKey(key)}`), true);
  } catch (e) {
    console.warn('[state] Error persistiendo cuota notificada:', e.message);
  }
}

async function saveCompletedSurvey(surveyId) {
  completedSurveys.add(String(surveyId));
  try {
    await set(ref(db, `pulso/agentState/completedSurveys/${surveyId}`), new Date().toISOString());
  } catch (e) {
    console.warn('[state] Error persistiendo encuesta completada:', e.message);
  }
}

// -----------------------------------------
// COLA DE ACCIONES — FIX #3 (race condition)
// -----------------------------------------
async function processActionQueue() {
  if (actionProcessing) return;
  actionProcessing = true;
  try {
    while (actionQueue.length > 0) {
      const action = actionQueue.shift();
      // Re-chequear: puede haberse notificado mientras esperaba en la cola
      if (notifiedQuotas.has(action.notifKey)) continue;
      await notifyOrExecute(action);
      await sleep(600);
    }
  } finally {
    actionProcessing = false;
  }
}

function enqueueActions(actions) {
  let added = 0;
  for (const action of actions) {
    if (!notifiedQuotas.has(action.notifKey)) {
      actionQueue.push(action);
      added++;
    }
  }
  if (added > 0) processActionQueue().catch(e => console.error('[queue]', e.message));
}

// Limpia pendingActions expiradas (FIX #8)
function cleanExpiredPendingActions() {
  const now = Date.now();
  let cleaned = 0;
  for (const [key, expiry] of pendingExpiry.entries()) {
    if (now > expiry) {
      pendingActions.delete(key);
      pendingExpiry.delete(key);
      cleaned++;
    }
  }
  if (cleaned > 0) console.log(`[agent] ${Math.floor(cleaned / 2)} accion(es) pendiente(s) expiradas eliminadas`);
}

// -----------------------------------------
// TELEGRAM API
// -----------------------------------------
async function tgRequest(method, body = {}) {
  try {
    const res = await axios.post(
      `https://api.telegram.org/bot${TG_TOKEN}/${method}`,
      body, { timeout: 35000 }
    );
    return res.data;
  } catch (e) {
    if (!e.message.includes('timeout')) console.error(`[telegram] ${method} error:`, e.message);
    return null;
  }
}
async function tgSend(text, replyMarkup = null) {
  const body = { chat_id: TG_CHAT_ID, text, parse_mode: 'HTML' };
  if (replyMarkup) body.reply_markup = replyMarkup;
  return tgRequest('sendMessage', body);
}
async function tgSendTo(chatId, text, replyMarkup = null) {
  const body = { chat_id: chatId, text, parse_mode: 'HTML' };
  if (replyMarkup) body.reply_markup = replyMarkup;
  return tgRequest('sendMessage', body);
}
async function tgAnswer(id, text = '') {
  return tgRequest('answerCallbackQuery', { callback_query_id: id, text });
}
async function tgEdit(messageId, text, replyMarkup = null) {
  const body = { chat_id: TG_CHAT_ID, message_id: messageId, text, parse_mode: 'HTML' };
  body.reply_markup = replyMarkup || { inline_keyboard: [] };
  return tgRequest('editMessageText', body);
}

// -----------------------------------------
// COMANDO /estado — FIX #2 (encuestas completadas)
// -----------------------------------------
function buildEstadoMessage(includeCompleted = false) {
  if (!appConfig) return '\u26a0\ufe0f Sin configuracion cargada aun.';
  const surveys = appConfig.activeSurveys || [];
  if (!surveys.length) return '\u26a0\ufe0f No hay encuestas activas.';

  let msg = '\ud83d\udcca <b>Estado de cuotas</b>\n';

  for (const sv of surveys) {
    const isDone = completedSurveys.has(String(sv.smSurveyId));

    if (isDone && !includeCompleted) {
      msg += `\n\u2705 <b>${sv.smTitle || sv.smSurveyId}</b> \u2014 <i>completada</i>\n`;
      continue;
    }

    const payload  = lastSyncData[String(sv.smSurveyId)];
    const muestra  = (appConfig.muestras || []).find(m => m.id === sv.muestraId);
    const quotas   = sv.quotas || {};
    const rawCases = payload?.rawCases || [];

    const totalTarget = Object.values(quotas).reduce((a, b) => a + b, 0);
    const totalPct    = pct(rawCases.length, totalTarget);

    msg += '\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n';
    msg += `\ud83d\udccb <b>${sv.smTitle || sv.smSurveyId}</b>${isDone ? ' \u2705 <i>COMPLETADA</i>' : ''}\n`;
    msg += `${bar(totalPct)} ${totalPct}%  (${rawCases.length} / ${totalTarget})\n`;
    msg += `\ud83d\udd50 Ultimo sync: ${relativeTime(payload?.lastSync || sv.lastSync)}\n`;

    if (isDone) { msg += '<i>Todas las cuotas completadas.</i>\n'; continue; }
    if (!muestra || !rawCases.length) { msg += '<i>Sin datos de casos aun</i>\n'; continue; }

    const groups  = muestra.ageGroups || [];
    const counts  = computeCounts(rawCases, muestra);
    const estratos = [...new Set(Object.keys(quotas).map(k => k.split('||')[2]))].filter(Boolean).sort();

    for (const estrato of estratos) {
      msg += `\n<b>Estrato ${estrato}</b>\n`;
      for (const gen of ['Masculino', 'Femenino']) {
        const gLabel = gen === 'Masculino' ? '\u2642' : '\u2640';
        for (const grp of groups) {
          const key = `${gen}||${grp}||${estrato}`;
          const t = quotas[key] || 0;
          if (!t) continue;
          const c = counts[key] || 0;
          const p = pct(c, t);
          const icon = p >= 100 ? '\u2705' : p >= 80 ? '\u26a0\ufe0f' : '\ud83d\udd35';
          msg += `${icon} ${gLabel} ${grp}: ${c}/${t} (${p}%)\n`;
        }
      }
    }
  }
  return msg;
}

// -----------------------------------------
// COMANDO /salud
// -----------------------------------------
async function buildSaludMessage() {
  const lines = ['\ud83d\udd0d <b>Salud del sistema</b>\n'];

  lines.push(db ? '\ud83d\udfe2 Firebase: conectado' : '\ud83d\udd34 Firebase: desconectado');

  try {
    const res = await axios.get(`${SYNC_SERVER_URL}/`, { timeout: 5000 });
    const d   = res.data;
    const active = d.totalActive ?? d.activeSurveys?.length ?? '?';
    lines.push(`\ud83d\udfe2 Sync server: online (${active} encuesta(s) activa(s))`);
  } catch (e) {
    lines.push('\ud83d\udd34 Sync server: no responde (' + e.message + ')');
  }

  const allSyncs = Object.values(lastSyncData);
  if (allSyncs.length) {
    const latest = allSyncs.reduce((a, b) =>
      new Date(a.lastSync || 0) > new Date(b.lastSync || 0) ? a : b
    );
    if (latest.lastSync) {
      const ageMin = Math.floor((Date.now() - new Date(latest.lastSync)) / 60000);
      const icon = ageMin < 30 ? '\ud83d\udfe2' : ageMin < 90 ? '\ud83d\udfe1' : '\ud83d\udd34';
      lines.push(`${icon} Ultimo dato recibido: hace ${ageMin} min`);
    }
  } else {
    lines.push('\ud83d\udfe1 Sin datos recibidos aun');
  }

  const upMs = Date.now() - agentStartTime;
  lines.push(`\u23f1 Activo hace ${Math.floor(upMs/3600000)}h ${Math.floor((upMs%3600000)/60000)}min`);

  const totalSurveys  = (appConfig?.activeSurveys || []).length;
  const activeSurveys = totalSurveys - completedSurveys.size;
  lines.push(`\ud83d\udcca Encuestas: ${activeSurveys} activa(s), ${completedSurveys.size} completada(s) de ${totalSurveys}`);
  lines.push(`\ud83d\udce6 Cuotas notificadas (hist\u00f3rico): ${notifiedQuotas.size}`);
  lines.push(`\u23f3 Acciones pendientes de confirmaci\u00f3n: ${pendingActions.size / 2}`);
  lines.push(META_AUTO_CONFIRM
    ? '\u26a1 Meta Ads: modo AUTOM\u00c1TICO (sin confirmaci\u00f3n)'
    : '\ud83d\udcac Meta Ads: modo confirmaci\u00f3n manual');
  lines.push(watchdogAlerted
    ? '\ud83d\udd34 Watchdog: ALERTA ACTIVA (sync retrasado)'
    : '\ud83d\udfe2 Watchdog: OK');

  return lines.join('\n');
}

// -----------------------------------------
// COMANDO /sync — FIX #5 (await real)
// -----------------------------------------
async function forceSyncNow() {
  try {
    const res = await axios.post(`${SYNC_SERVER_URL}/sync/now`, {}, { timeout: 10000 });
    return `\u2705 ${res.data?.message || 'Sync iniciado — datos disponibles en unos segundos'}`;
  } catch (e) {
    if (e.response?.data?.error) return `\u274c ${e.response.data.error}`;
    return `\u274c Sync server no responde: ${e.message}`;
  }
}

// -----------------------------------------
// META ADS API
// -----------------------------------------
async function metaGet(path, params = {}) {
  const res = await axios.get(`https://graph.facebook.com/v19.0${path}`, {
    params: { access_token: META_TOKEN, ...params }, timeout: 15000,
  });
  return res.data;
}
async function metaPost(path, data = {}) {
  const res = await axios.post(
    `https://graph.facebook.com/v19.0${path}`,
    { access_token: META_TOKEN, ...data }, { timeout: 15000 }
  );
  return res.data;
}
async function findAdsetsByName(namePart) {
  try {
    const data = await metaGet(`/act_${META_ACCOUNT}/adsets`, {
      fields: 'id,name,status,targeting,daily_budget,bid_amount,optimization_goal,billing_event,campaign_id',
      limit: 200,
    });
    const needle = norm(namePart);
    return (data.data || []).filter(a => norm(a.name).includes(needle));
  } catch (e) { console.error('[meta] findAdsetsByName error:', e.message); return []; }
}
function adsetName(estrato, range) {
  return `estrato ${estrato} ${range === AGE_GROUPS_YOUNG ? 'jovenes' : '+30'}`;
}
async function pauseAdset(id) { return metaPost(`/${id}`, { status: 'PAUSED' }); }

// FIX #6: primero obtiene targeting actual y lo mergea, no lo sobreescribe
async function updateAdsetAge(id, ageMin, ageMax) {
  const adset = await metaGet(`/${id}`, { fields: 'targeting' });
  const targeting = typeof adset.targeting === 'string'
    ? JSON.parse(adset.targeting)
    : (adset.targeting || {});
  targeting.age_min = ageMin;
  if (ageMax && ageMax < 65) targeting.age_max = ageMax;
  else delete targeting.age_max;
  return metaPost(`/${id}`, { targeting: JSON.stringify(targeting) });
}

// FIX #7: copia bid_strategy y promoted_object ademas de bid_amount
async function cloneAdsetWithGender(original, genderCode, newName) {
  const orig = await metaGet(`/${original.id}`, {
    fields: 'name,campaign_id,daily_budget,targeting,optimization_goal,billing_event,bid_amount,bid_strategy,promoted_object',
  });
  const targeting = typeof orig.targeting === 'string'
    ? JSON.parse(orig.targeting)
    : (orig.targeting || {});
  targeting.genders = [genderCode];
  const body = {
    name: newName,
    campaign_id: orig.campaign_id,
    daily_budget: orig.daily_budget,
    targeting: JSON.stringify(targeting),
    optimization_goal: orig.optimization_goal,
    billing_event: orig.billing_event,
    status: 'ACTIVE',
  };
  if (orig.bid_amount)      body.bid_amount      = orig.bid_amount;
  if (orig.bid_strategy)    body.bid_strategy    = orig.bid_strategy;
  if (orig.promoted_object) body.promoted_object = JSON.stringify(orig.promoted_object);
  const res = await metaPost(`/act_${META_ACCOUNT}/adsets`, body);
  return res.id;
}

// -----------------------------------------
// LOGICA DE CUOTAS
// -----------------------------------------
function getMuestraForSurvey(surveyId) {
  if (!appConfig) return null;
  const sv = (appConfig.activeSurveys || []).find(s => String(s.smSurveyId) === String(surveyId));
  if (!sv) return null;
  const muestra = (appConfig.muestras || []).find(m => m.id === sv.muestraId);
  return { survey: sv, muestra };
}
function computeCounts(rawCases, muestra) {
  if (!muestra || !rawCases) return {};
  const groups = muestra.ageGroups || [];
  const bounds = parseAgeBounds(groups);
  const counts = {};
  for (const c of rawCases) {
    const ageGrp  = getAgeGrp(parseInt(c.edad), bounds, groups);
    const estrato = (c.estrato && c.estrato !== '?')
      ? String(c.estrato)
      : (lookupEst(c.prov, c.depto, muestra) || '?');
    const key = `${c.gen}||${ageGrp}||${estrato}`;
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

// FIX #2: detecta si TODAS las cuotas (> 0) estan completas
function isSurveyComplete(rawCases, survey, muestra) {
  const quotas  = survey.quotas || {};
  const nonZero = Object.entries(quotas).filter(([, t]) => t > 0);
  if (!nonZero.length) return false;
  const counts  = computeCounts(rawCases, muestra);
  return nonZero.every(([key, target]) => (counts[key] || 0) >= target);
}

function evaluateActions(surveyId, rawCases, survey, muestra) {
  const quotas    = survey.quotas || {};
  const counts    = computeCounts(rawCases, muestra);
  const actions   = [];
  const ageGroups = muestra.ageGroups || [];
  const estratos  = [...new Set(Object.keys(quotas).map(k => k.split('||')[2]))];

  for (const estrato of estratos) {
    // --- Adset jovenes (16-29 / 18-29) ---
    const youngGroup = ageGroups.find(g => {
      const n = norm(g); return n.startsWith('16') || n.startsWith('18');
    });
    if (youngGroup) {
      const keyF = `Femenino||${youngGroup}||${estrato}`;
      const keyM = `Masculino||${youngGroup}||${estrato}`;
      const tF = quotas[keyF]||0, tM = quotas[keyM]||0;
      const cF = counts[keyF]||0, cM = counts[keyM]||0;
      const doneF = tF > 0 && cF >= tF;
      const doneM = tM > 0 && cM >= tM;
      const kBoth = `${surveyId}||${estrato}||${youngGroup}||both`;
      const kF    = `${surveyId}||${estrato}||${youngGroup}||F`;
      const kM    = `${surveyId}||${estrato}||${youngGroup}||M`;

      if (doneF && doneM && !notifiedQuotas.has(kBoth)) {
        actions.push({
          notifKey: kBoth, type: 'pause',
          adsetNamePart: adsetName(estrato, AGE_GROUPS_YOUNG),
          description: `\u2705 Cuota <b>${youngGroup} - Estrato ${estrato}</b> completa en ambos g\u00e9neros (\u2642${cM}/${tM} \u2640${cF}/${tF})\n\n\u2192 Pausar adset <b>${adsetName(estrato, AGE_GROUPS_YOUNG)}</b>`,
        });
      } else if (doneF && !doneM && !notifiedQuotas.has(kF)) {
        actions.push({
          notifKey: kF, type: 'split_pause_female',
          adsetNamePart: adsetName(estrato, AGE_GROUPS_YOUNG),
          description: `\u26a0\ufe0f Cuota <b>${youngGroup} - Estrato ${estrato} - Femenino</b> completa (\u2640${cF}/${tF})\nMasculino a\u00fan abierto (\u2642${cM}/${tM})\n\n\u2192 Separar adset en M/F y pausar Femenino`,
        });
      } else if (doneM && !doneF && !notifiedQuotas.has(kM)) {
        actions.push({
          notifKey: kM, type: 'split_pause_male',
          adsetNamePart: adsetName(estrato, AGE_GROUPS_YOUNG),
          description: `\u26a0\ufe0f Cuota <b>${youngGroup} - Estrato ${estrato} - Masculino</b> completa (\u2642${cM}/${tM})\nFemenino a\u00fan abierto (\u2640${cF}/${tF})\n\n\u2192 Separar adset en M/F y pausar Masculino`,
        });
      }
    }

    // --- Adset +30 subrangos ---
    for (const subrange of SUBRANGES_OLD) {
      const matchedGroup = ageGroups.find(g => {
        const b = parseAgeBounds([g])[0];
        return b[0] >= subrange.min && b[1] <= subrange.max;
      });
      if (!matchedGroup) continue;
      const keyF = `Femenino||${matchedGroup}||${estrato}`;
      const keyM = `Masculino||${matchedGroup}||${estrato}`;
      const tF = quotas[keyF]||0, tM = quotas[keyM]||0;
      if (!tF && !tM) continue;
      const cF = counts[keyF]||0, cM = counts[keyM]||0;
      const kBoth = `${surveyId}||${estrato}||${matchedGroup}||both`;
      if ((tF > 0 && cF >= tF) && (tM > 0 && cM >= tM) && !notifiedQuotas.has(kBoth)) {
        const idx  = SUBRANGES_OLD.indexOf(subrange);
        const next = SUBRANGES_OLD[idx + 1];
        if (!next) {
          actions.push({
            notifKey: kBoth, type: 'pause',
            adsetNamePart: adsetName(estrato, AGE_GROUPS_OLD),
            description: `\u2705 Cuota <b>${matchedGroup} - Estrato ${estrato}</b> completa en ambos g\u00e9neros. Todos los rangos +30 completos.\n\n\u2192 Pausar adset <b>${adsetName(estrato, AGE_GROUPS_OLD)}</b>`,
          });
        } else {
          actions.push({
            notifKey: kBoth, type: 'update_age',
            adsetNamePart: adsetName(estrato, AGE_GROUPS_OLD),
            ageMin: next.min, ageMax: next.max,
            description: `\u2705 Cuota <b>${matchedGroup} - Estrato ${estrato}</b> completa en ambos g\u00e9neros (\u2642${cM}/${tM} \u2640${cF}/${tF})\n\n\u2192 Acotar adset <b>${adsetName(estrato, AGE_GROUPS_OLD)}</b> a <b>${next.min}-${next.max} a\u00f1os</b>`,
          });
        }
      }
    }
  }
  return actions;
}

// -----------------------------------------
// EJECUTAR ACCION EN META
// -----------------------------------------
async function executeAction(action) {
  const adsets = await findAdsetsByName(action.adsetNamePart);
  if (!adsets.length) throw new Error(`No se encontr\u00f3 adset con nombre que contenga "${action.adsetNamePart}"`);
  const adset = adsets[0];
  switch (action.type) {
    case 'pause':
      await pauseAdset(adset.id);
      return `Adset <b>${adset.name}</b> pausado \u2713`;
    case 'update_age':
      await updateAdsetAge(adset.id, action.ageMin, action.ageMax);
      return `Adset <b>${adset.name}</b> acotado a ${action.ageMin}-${action.ageMax} a\u00f1os \u2713`;
    case 'split_pause_female': {
      const nameM = `${adset.name} masculino`, nameF = `${adset.name} femenino`;
      const newFId = await cloneAdsetWithGender(adset, 2, nameF);
      await cloneAdsetWithGender(adset, 1, nameM);
      await pauseAdset(newFId);
      await pauseAdset(adset.id);
      return `Dividido:\n\u2022 <b>${nameM}</b> activo\n\u2022 <b>${nameF}</b> pausado\n\u2022 Original pausado`;
    }
    case 'split_pause_male': {
      const nameM = `${adset.name} masculino`, nameF = `${adset.name} femenino`;
      const newMId = await cloneAdsetWithGender(adset, 1, nameM);
      await cloneAdsetWithGender(adset, 2, nameF);
      await pauseAdset(newMId);
      await pauseAdset(adset.id);
      return `Dividido:\n\u2022 <b>${nameF}</b> activo\n\u2022 <b>${nameM}</b> pausado\n\u2022 Original pausado`;
    }
    default:
      throw new Error(`Tipo desconocido: ${action.type}`);
  }
}

// -----------------------------------------
// NOTIFICAR O EJECUTAR — FIX #4 (META_AUTO_CONFIRM)
// -----------------------------------------
async function notifyOrExecute(action) {
  // Persistir ANTES de actuar para que incluso si falla la notif, no se re-intente en el proximo ciclo
  await saveNotifiedQuota(action.notifKey);

  if (META_AUTO_CONFIRM) {
    // Modo automatico: ejecutar directamente
    const sent = await tgSend(
      `\u26a1 <b>Pulso \u2014 Ejecutando autom\u00e1ticamente</b>\n\n${action.description}\n\n\u23f3 En proceso...`
    );
    try {
      const result = await executeAction(action);
      const text = `\u2705 <b>Ejecutado</b>\n\n${action.description}\n\n<i>${result}</i>`;
      if (sent?.result?.message_id) await tgEdit(sent.result.message_id, text);
      else await tgSend(text);
      console.log(`[agent] Auto-ejecutado: ${action.notifKey}`);
    } catch (e) {
      const text = `\u274c <b>Error al ejecutar</b>\n\n${action.description}\n\n<i>${e.message}</i>`;
      if (sent?.result?.message_id) await tgEdit(sent.result.message_id, text);
      else await tgSend(text);
      console.error(`[agent] Error auto-ejecutando ${action.notifKey}:`, e.message);
    }
  } else {
    // Modo confirmacion: enviar botones
    const ts = Date.now();
    const cbConfirm = `confirm_${ts}`;
    const cbCancel  = `cancel_${ts}`;
    pendingActions.set(cbConfirm, action);
    pendingActions.set(cbCancel, { ...action, cancel: true });
    const expiry = ts + 2 * 60 * 60 * 1000; // expira en 2 horas
    pendingExpiry.set(cbConfirm, expiry);
    pendingExpiry.set(cbCancel, expiry);

    await tgSend(
      `\ud83d\udd14 <b>Pulso \u2014 Acci\u00f3n requerida</b>\n\n${action.description}\n\n\u00bfEjecutar?`,
      { inline_keyboard: [[
        { text: '\u2705 Confirmar', callback_data: cbConfirm },
        { text: '\u274c Cancelar',  callback_data: cbCancel  },
      ]]}
    );
    console.log(`[agent] Notificaci\u00f3n enviada: ${action.notifKey}`);
  }
}

// -----------------------------------------
// PROCESAR UPDATES TELEGRAM
// -----------------------------------------
async function processTelegramUpdate(update) {
  // Callback (botones)
  if (update.callback_query) {
    const cq     = update.callback_query;
    const fromId = String(cq.from?.id || '');
    await tgAnswer(cq.id);

    // FIX #9: auth check en callbacks (evita que alguien externo ejecute acciones)
    if (!TG_ALLOWED_IDS.includes(fromId)) {
      console.warn(`[telegram] Callback rechazado — usuario no autorizado: ${fromId}`);
      return;
    }

    const data = cq.data, messageId = cq.message?.message_id;
    if (!pendingActions.has(data)) {
      await tgEdit(messageId, '\u26a0\ufe0f Esta acci\u00f3n ya fue procesada o expir\u00f3 (2h).');
      return;
    }
    const action = pendingActions.get(data);
    // Eliminar ambas opciones (confirm y cancel)
    const other = data.startsWith('confirm_')
      ? data.replace('confirm_', 'cancel_')
      : data.replace('cancel_', 'confirm_');
    pendingActions.delete(data); pendingActions.delete(other);
    pendingExpiry.delete(data);  pendingExpiry.delete(other);

    if (action.cancel) {
      await tgEdit(messageId, `\u274c Cancelado.\n\n${action.description}`);
      return;
    }
    await tgEdit(messageId, `\u23f3 Ejecutando...\n\n${action.description}`);
    try {
      const result = await executeAction(action);
      await tgEdit(messageId, `\u2705 <b>Ejecutado</b>\n\n${action.description}\n\n<i>${result}</i>`);
    } catch (e) {
      await tgEdit(messageId, `\u274c <b>Error</b>\n\n${action.description}\n\n<i>${e.message}</i>`);
    }
    return;
  }

  // Mensaje de texto (comandos)
  const msg = update.message;
  if (!msg?.text) return;
  const chatId = String(msg.chat.id);
  if (!TG_ALLOWED_IDS.includes(chatId)) return;
  const text = msg.text.trim().toLowerCase();

  if (text.startsWith('/estado') || text.startsWith('/status')) {
    const includeDone = text.includes('todo') || text.includes('all');
    await tgSendTo(chatId, '\u23f3 Calculando...');
    await tgSendTo(chatId, buildEstadoMessage(includeDone));
  } else if (text.startsWith('/salud') || text.startsWith('/health')) {
    await tgSendTo(chatId, '\u23f3 Verificando...');
    await tgSendTo(chatId, await buildSaludMessage());
  } else if (text.startsWith('/sync')) {
    await tgSendTo(chatId, '\u23f3 Forzando sincronizaci\u00f3n...');
    await tgSendTo(chatId, await forceSyncNow());
  } else if (text.startsWith('/ayuda') || text.startsWith('/help') || text.startsWith('/start')) {
    const modeStr = META_AUTO_CONFIRM ? '\u26a1 Meta Ads: modo autom\u00e1tico' : '\ud83d\udcac Meta Ads: confirmaci\u00f3n manual';
    await tgSendTo(chatId,
      '\ud83e\udd16 <b>Pulso Agent v2 \u2014 Comandos</b>\n\n' +
      '/estado \u2014 Cuotas de encuestas activas\n' +
      '/estado todo \u2014 Incluye encuestas ya completadas\n' +
      '/sync \u2014 Fuerza sincronizaci\u00f3n con SurveyMonkey\n' +
      '/salud \u2014 Estado del sistema\n' +
      '/ayuda \u2014 Esta ayuda\n\n' +
      modeStr
    );
  }
}

// -----------------------------------------
// TELEGRAM LONG POLLING
// -----------------------------------------
async function pollTelegram() {
  while (true) {
    try {
      const res = await tgRequest('getUpdates', {
        offset: tgOffset, timeout: 30,
        allowed_updates: ['callback_query', 'message'],
      });
      if (res?.result?.length) {
        for (const update of res.result) {
          tgOffset = update.update_id + 1;
          await processTelegramUpdate(update).catch(e => console.error('[update]', e.message));
        }
      }
    } catch (e) {
      if (!e.message?.includes('timeout')) console.error('[polling]', e.message);
      await sleep(3000);
    }
  }
}

// -----------------------------------------
// FIREBASE LISTENERS
// -----------------------------------------
function startFirebaseListeners() {
  onValue(ref(db, 'pulso/v4config'), (snap) => {
    try {
      const raw = snap.val(); if (!raw) return;
      appConfig = typeof raw === 'string' ? JSON.parse(raw) : raw;
      console.log(`[firebase] Config \u2014 ${(appConfig.activeSurveys||[]).length} encuesta(s)`);
    } catch (e) { console.error('[firebase] v4config error:', e.message); }
  });

  onValue(ref(db, 'pulso/v4sync'), async (snap) => {
    try {
      const root = snap.val();
      if (!root || !appConfig) return;
      const entries = typeof root === 'string' ? { legacy: JSON.parse(root) } : root;

      // FIX #3: recolectar todas las acciones y encolarlas juntas (no disparar en paralelo)
      const newActions = [];

      for (const [sid, raw] of Object.entries(entries)) {
        const payload  = typeof raw === 'string' ? JSON.parse(raw) : raw;
        const surveyId = String(payload.surveyId || sid);
        lastSyncData[surveyId] = payload;

        // Resetear watchdog SOLO si los datos son realmente frescos (< 30 min)
        // Evita que datos viejos/huerfanos de Firebase liberen el watchdog en loop
        const dataAgeMin = payload.lastSync
          ? Math.floor((Date.now() - new Date(payload.lastSync)) / 60000)
          : 999;
        if (watchdogAlerted && dataAgeMin < 30) {
          watchdogAlerted = false;
          await tgSend('\ud83d\udfe2 <b>Sync recuperado</b> \u2014 Datos actualizados correctamente.');
        }

        // FIX #2: ignorar encuestas ya completadas
        if (completedSurveys.has(surveyId)) continue;

        const info = getMuestraForSurvey(surveyId);
        if (!info?.muestra) continue;

        const rawCases = payload.rawCases || [];

        // FIX #2: chequear si la encuesta se acaba de completar
        if (isSurveyComplete(rawCases, info.survey, info.muestra)) {
          await saveCompletedSurvey(surveyId);
          await tgSend(
            `\ud83c\udfc1 <b>Encuesta completada</b>\n\n` +
            `\ud83d\udccb <b>${info.survey.smTitle || surveyId}</b>\n\n` +
            `Todas las cuotas fueron alcanzadas. El monitoreo de esta encuesta se detiene.`
          );
          console.log(`[agent] Encuesta ${surveyId} completada`);
          continue;
        }

        const actions = evaluateActions(surveyId, rawCases, info.survey, info.muestra);
        newActions.push(...actions);
      }

      if (newActions.length > 0) enqueueActions(newActions);
    } catch (e) { console.error('[firebase] v4sync error:', e.message); }
  });
}

// -----------------------------------------
// HEALTH CHECK HTTP
// -----------------------------------------
const http = require('http');
http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'ok', agent: 'pulso-agent-v2',
    pending: pendingActions.size / 2,
    surveys: Object.keys(lastSyncData).length,
    completed: completedSurveys.size,
    notified: notifiedQuotas.size,
    autoConfirm: META_AUTO_CONFIRM,
    watchdogAlerted,
    config: !!appConfig,
  }));
}).listen(process.env.PORT || 3001, () => console.log(`[http] Puerto ${process.env.PORT || 3001}`));

// -----------------------------------------
// INICIO
// -----------------------------------------
(async () => {
  console.log('\n  Pulso Agent v2 arrancando...');
  console.log(`  Telegram:     ${TG_TOKEN ? '\u2713' : '\u2717 FALTA TG_TOKEN'}`);
  console.log(`  Meta:         ${META_TOKEN ? '\u2713' : '\u2717 FALTA META_TOKEN'}`);
  console.log(`  Firebase:     \u2713`);
  console.log(`  Cuenta Meta:  act_${META_ACCOUNT}`);
  console.log(`  Sync server:  ${SYNC_SERVER_URL}`);
  console.log(`  Usuarios TG:  ${TG_ALLOWED_IDS.join(', ')}`);
  console.log(`  Auto-confirm: ${META_AUTO_CONFIRM ? 'SI (meta ejecuta automatico)' : 'NO (pide confirmacion)'}\n`);

  // FIX #1: cargar estado previo ANTES de activar listeners
  // Esto evita que al reiniciar Railway se re-notifiquen todas las cuotas ya procesadas
  await loadAgentState();

  if (TG_TOKEN && TG_CHAT_ID) {
    const modeStr = META_AUTO_CONFIRM
      ? '\u26a1 Meta Ads: modo AUTOM\u00c1TICO'
      : '\ud83d\udcac Meta Ads: confirmaci\u00f3n manual';
    await tgSend(
      `\ud83d\udfe2 <b>Pulso Agent v2 iniciado</b>\n${modeStr}\n` +
      `\ud83d\udce6 ${notifiedQuotas.size} cuota(s) previas cargadas\n` +
      `\u2705 ${completedSurveys.size} encuesta(s) completadas\n\n` +
      'Comandos: /estado | /sync | /salud | /ayuda'
    );
  }

  startFirebaseListeners();
  pollTelegram(); // no await — corre en background

  // Reporte automatico cada 6 horas (FIX #10: solo encuestas activas)
  setInterval(async () => {
    try {
      const activeSurveys = (appConfig?.activeSurveys || [])
        .filter(sv => !completedSurveys.has(String(sv.smSurveyId)));
      if (!activeSurveys.length) {
        console.log('[agent] Reporte omitido — sin encuestas activas');
        return;
      }
      // No reportar si el watchdog está activo (sync caído, datos viejos)
      if (watchdogAlerted) {
        console.log('[agent] Reporte omitido — sync caído (watchdog activo)');
        return;
      }
      await tgSend('\u23f0 <b>Reporte autom\u00e1tico</b>\n\n' + buildEstadoMessage());
      console.log('[agent] Reporte autom\u00e1tico enviado');
    } catch (e) { console.error('[agent] Error reporte:', e.message); }
  }, 6 * 60 * 60 * 1000);

  // Watchdog: alerta si no llegan datos en 90 min (FIX #10: solo encuestas activas)
  setInterval(async () => {
    try {
      const activeSurveyIds = (appConfig?.activeSurveys || [])
        .filter(sv => !completedSurveys.has(String(sv.smSurveyId)))
        .map(sv => String(sv.smSurveyId));
      if (!activeSurveyIds.length) return;

      const activeSyncs = Object.entries(lastSyncData)
        .filter(([sid]) => activeSurveyIds.includes(sid))
        .map(([, v]) => v);
      if (!activeSyncs.length) return;

      const latest = activeSyncs.reduce((a, b) =>
        new Date(a.lastSync||0) > new Date(b.lastSync||0) ? a : b
      );
      if (!latest.lastSync) return;
      const ageMin = Math.floor((Date.now() - new Date(latest.lastSync)) / 60000);
      if (ageMin > 90 && !watchdogAlerted) {
        watchdogAlerted = true;
        await tgSend(
          '\u26a0\ufe0f <b>Pulso Agent \u2014 Sin datos nuevos</b>\n\n' +
          `Hace <b>${ageMin} minutos</b> que no llegan datos de SurveyMonkey.\n` +
          'El sync puede estar ca\u00eddo o pausado.\n\n' +
          'Usa /sync para forzar una actualizaci\u00f3n o /salud para diagnosticar.'
        );
        console.log(`[watchdog] Alerta: sin datos hace ${ageMin}min`);
      }
    } catch (e) { console.error('[watchdog]', e.message); }
  }, 15 * 60 * 1000);

  // Limpiar pendingActions expiradas cada hora (FIX #8)
  setInterval(cleanExpiredPendingActions, 60 * 60 * 1000);

  console.log('[agent] Iniciado: reporte 6hs | watchdog 15min | limpieza pendientes 1h');
})();
