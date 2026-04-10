/**
 * pulso-agent — Agente de control de cuotas + Meta Ads
 * Pulso Research / MVP DATA SRL
 *
 * Comandos Telegram:
 *  /estado  - resumen de cuotas
 *  /sync    - fuerza sincronizacion inmediata
 *  /salud   - estado del sistema (Firebase, sync server, ultimo dato)
 *  /ayuda   - lista de comandos
 *
 * Automatico:
 *  - Detecta cuotas al 100% y notifica con botones Confirmar/Cancelar
 *  - Reporte cada 6 horas
 *  - Watchdog: alerta si no llegan datos en 90 minutos
 */

'use strict';

const axios = require('axios');
const { initializeApp } = require('firebase/app');
const { getDatabase, ref, onValue } = require('firebase/database');

// -----------------------------------------
// CONFIG
// -----------------------------------------
const TG_TOKEN        = process.env.TG_TOKEN;
const TG_CHAT_ID      = process.env.TG_CHAT_ID;
const TG_ALLOWED_IDS  = (process.env.TG_ALLOWED_IDS || process.env.TG_CHAT_ID || '').split(',').map(s => s.trim());
const META_TOKEN      = process.env.META_TOKEN;
const META_ACCOUNT    = process.env.META_ACCOUNT || '1580131239919455';
const FB_API_KEY      = process.env.FB_API_KEY;
const FB_DB_URL       = process.env.FB_DB_URL || 'https://control-cuotas-pulso-default-rtdb.firebaseio.com';
const SYNC_SERVER_URL = process.env.SYNC_SERVER_URL || 'https://controlcuotasv2-production.up.railway.app';

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
const pendingActions = new Map();
const notifiedQuotas = new Set();
let tgOffset = 0;
let watchdogAlerted = false;

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

// -----------------------------------------
// ESTRATO LOOKUP (replica logica del dashboard)
// -----------------------------------------
const PROV_ALIASES = {
  'caba':'caba','ciudad autonoma de buenos aires':'caba','ciudad de buenos aires':'caba',
  'capital federal':'caba','buenos aires ciudad':'caba',
  'buenos aires':'buenos aires','provincia de buenos aires':'buenos aires',
  'santiago del estero':'stgo del estero','stgo del estero':'stgo del estero',
  'tierra del fuego':'tierra del fuego',
  'tierra del fuego, antartida e islas del atlantico sur':'tierra del fuego',
  'entre rios':'entre rios','entre r\u00edos':'entre rios',
  'neuqu\u00e9n':'neuquen','neuquen':'neuquen',
  'c\u00f3rdoba':'cordoba','cordoba':'cordoba',
  'tucum\u00e1n':'tucuman','tucuman':'tucuman',
  'r\u00edo negro':'rio negro','rio negro':'rio negro',
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
// COMANDO /estado
// -----------------------------------------
function buildEstadoMessage() {
  if (!appConfig) return '\u26a0\ufe0f Sin configuracion cargada aun.';
  const surveys = appConfig.activeSurveys || [];
  if (!surveys.length) return '\u26a0\ufe0f No hay encuestas activas.';

  let msg = '\ud83d\udcca <b>Estado de cuotas</b>\n';

  for (const sv of surveys) {
    const payload  = lastSyncData[String(sv.smSurveyId)];
    const muestra  = (appConfig.muestras || []).find(m => m.id === sv.muestraId);
    const quotas   = sv.quotas || {};
    const rawCases = payload?.rawCases || [];

    const totalTarget = Object.values(quotas).reduce((a, b) => a + b, 0);
    const totalPct    = pct(rawCases.length, totalTarget);

    msg += '\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n';
    msg += `\ud83d\udccb <b>${sv.smTitle || sv.smSurveyId}</b>\n`;
    msg += `${bar(totalPct)} ${totalPct}%  (${rawCases.length} / ${totalTarget})\n`;
    msg += `\ud83d\udd50 Ultimo sync: ${relativeTime(payload?.lastSync || sv.lastSync)}\n`;

    if (!muestra || !rawCases.length) { msg += '<i>Sin datos de casos aun</i>\n'; continue; }

    const groups = muestra.ageGroups || [];
    const bounds = parseAgeBounds(groups);
    const counts = computeCounts(rawCases, muestra);
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

  // Firebase
  lines.push(db ? '\ud83d\udfe2 Firebase: conectado' : '\ud83d\udd34 Firebase: desconectado');

  // Sync server
  try {
    const res = await axios.get(`${SYNC_SERVER_URL}/`, { timeout: 5000 });
    const d = res.data;
    const syncStatus = d.syncActive ? 'sync activo' : 'sync detenido';
    lines.push(`\ud83d\udfe2 Sync server: online (${syncStatus})`);
    if (d.lastSync) lines.push(`   Ultimo sync server: ${relativeTime(d.lastSync)}`);
  } catch (e) {
    lines.push('\ud83d\udd34 Sync server: no responde (' + e.message + ')');
  }

  // Ultimo dato recibido en Firebase
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

  // Uptime
  const upMs = Date.now() - agentStartTime;
  const upH  = Math.floor(upMs / 3600000);
  const upM  = Math.floor((upMs % 3600000) / 60000);
  lines.push(`\u23f1 Bot activo hace ${upH}h ${upM}min`);

  // Encuestas
  const nSurveys = (appConfig?.activeSurveys || []).length;
  lines.push(`\ud83d\udcca Encuestas monitoreadas: ${nSurveys}`);

  // Watchdog
  lines.push(watchdogAlerted ? '\ud83d\udd34 Watchdog: alerta activa (sync retrasado)' : '\ud83d\udfe2 Watchdog: OK');

  return lines.join('\n');
}

// -----------------------------------------
// COMANDO /sync
// -----------------------------------------
async function forceSyncNow() {
  try {
    axios.post(`${SYNC_SERVER_URL}/sync/now`, {}, { timeout: 5000 }).catch(() => {});
    return '\u2705 Sync iniciado \u2014 los datos se actualizaran en unos segundos';
  } catch (e) {
    return `\u274c Error: ${e.message}`;
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
// Convencion: "estrato N jovenes" / "estrato N +30"
function adsetName(estrato, range) {
  const rangeLabel = (range === AGE_GROUPS_YOUNG) ? 'jovenes' : '+30';
  return `estrato ${estrato} ${rangeLabel}`;
}
async function pauseAdset(id) { return metaPost(`/${id}`, { status: 'PAUSED' }); }
async function updateAdsetAge(id, ageMin, ageMax) {
  const targeting = { age_min: ageMin };
  if (ageMax && ageMax < 65) targeting.age_max = ageMax;
  return metaPost(`/${id}`, { targeting: JSON.stringify(targeting) });
}
async function cloneAdsetWithGender(original, genderCode, newName) {
  const orig = await metaGet(`/${original.id}`, {
    fields: 'name,campaign_id,daily_budget,targeting,optimization_goal,billing_event,bid_amount',
  });
  const targeting = typeof orig.targeting === 'string' ? JSON.parse(orig.targeting) : (orig.targeting || {});
  targeting.genders = [genderCode];
  const body = {
    name: newName, campaign_id: orig.campaign_id, daily_budget: orig.daily_budget,
    targeting: JSON.stringify(targeting), optimization_goal: orig.optimization_goal,
    billing_event: orig.billing_event, status: 'ACTIVE',
  };
  if (orig.bid_amount) body.bid_amount = orig.bid_amount;
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
function evaluateActions(surveyId, rawCases, survey, muestra) {
  const quotas    = survey.quotas || {};
  const counts    = computeCounts(rawCases, muestra);
  const actions   = [];
  const ageGroups = muestra.ageGroups || [];
  const estratos  = [...new Set(Object.keys(quotas).map(k => k.split('||')[2]))];

  for (const estrato of estratos) {
    // Adset jovenes (16-29 / 18-29)
    const youngGroup = ageGroups.find(g => { const n = norm(g); return n.startsWith('16') || n.startsWith('18'); });
    if (youngGroup) {
      const keyF = `Femenino||${youngGroup}||${estrato}`;
      const keyM = `Masculino||${youngGroup}||${estrato}`;
      const tF = quotas[keyF]||0, tM = quotas[keyM]||0;
      const cF = counts[keyF]||0, cM = counts[keyM]||0;
      const doneF = tF > 0 && cF >= tF, doneM = tM > 0 && cM >= tM;
      const kBoth = `${surveyId}||${estrato}||${youngGroup}||both`;
      const kF    = `${surveyId}||${estrato}||${youngGroup}||F`;
      const kM    = `${surveyId}||${estrato}||${youngGroup}||M`;

      if (doneF && doneM && !notifiedQuotas.has(kBoth)) {
        actions.push({ notifKey: kBoth, type: 'pause', adsetNamePart: adsetName(estrato, AGE_GROUPS_YOUNG),
          description: `\u2705 Cuota <b>${youngGroup} - Estrato ${estrato}</b> completa en ambos generos (\u2642${cM}/${tM} \u2640${cF}/${tF})\n\n\u2192 Pausar adset <b>${adsetName(estrato, AGE_GROUPS_YOUNG)}</b>` });
      } else if (doneF && !doneM && !notifiedQuotas.has(kF)) {
        actions.push({ notifKey: kF, type: 'split_pause_female', adsetNamePart: adsetName(estrato, AGE_GROUPS_YOUNG),
          description: `\u26a0\ufe0f Cuota <b>${youngGroup} - Estrato ${estrato} - Femenino</b> completa (\u2640${cF}/${tF})\nMasculino aun abierto (\u2642${cM}/${tM})\n\n\u2192 Separar adset en M/F y pausar Femenino` });
      } else if (doneM && !doneF && !notifiedQuotas.has(kM)) {
        actions.push({ notifKey: kM, type: 'split_pause_male', adsetNamePart: adsetName(estrato, AGE_GROUPS_YOUNG),
          description: `\u26a0\ufe0f Cuota <b>${youngGroup} - Estrato ${estrato} - Masculino</b> completa (\u2642${cM}/${tM})\nFemenino aun abierto (\u2640${cF}/${tF})\n\n\u2192 Separar adset en M/F y pausar Masculino` });
      }
    }

    // Adset +30 subrangos
    for (const subrange of SUBRANGES_OLD) {
      const matchedGroup = ageGroups.find(g => { const b = parseAgeBounds([g])[0]; return b[0] >= subrange.min && b[1] <= subrange.max; });
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
          actions.push({ notifKey: kBoth, type: 'pause', adsetNamePart: adsetName(estrato, AGE_GROUPS_OLD),
            description: `\u2705 Cuota <b>${matchedGroup} - Estrato ${estrato}</b> completa en ambos generos. Todos los rangos +30 completos.\n\n\u2192 Pausar adset <b>${adsetName(estrato, AGE_GROUPS_OLD)}</b>` });
        } else {
          actions.push({ notifKey: kBoth, type: 'update_age', adsetNamePart: adsetName(estrato, AGE_GROUPS_OLD),
            ageMin: next.min, ageMax: next.max,
            description: `\u2705 Cuota <b>${matchedGroup} - Estrato ${estrato}</b> completa en ambos generos (\u2642${cM}/${tM} \u2640${cF}/${tF})\n\n\u2192 Acotar adset <b>${adsetName(estrato, AGE_GROUPS_OLD)}</b> a <b>${next.min}-${next.max} anos</b>` });
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
  if (!adsets.length) throw new Error(`No se encontro adset con nombre que contenga "${action.adsetNamePart}"`);
  const adset = adsets[0];
  switch (action.type) {
    case 'pause':
      await pauseAdset(adset.id);
      return `Adset <b>${adset.name}</b> pausado \u2713`;
    case 'update_age':
      await updateAdsetAge(adset.id, action.ageMin, action.ageMax);
      return `Adset <b>${adset.name}</b> acotado a ${action.ageMin}-${action.ageMax} anos \u2713`;
    case 'split_pause_female': {
      const nameM = `${adset.name} masculino`, nameF = `${adset.name} femenino`;
      const newFId = await cloneAdsetWithGender(adset, 2, nameF);
      await cloneAdsetWithGender(adset, 1, nameM);
      await pauseAdset(newFId); await pauseAdset(adset.id);
      return `Dividido:\n\u2022 <b>${nameM}</b> activo\n\u2022 <b>${nameF}</b> pausado\n\u2022 Original pausado`;
    }
    case 'split_pause_male': {
      const nameM = `${adset.name} masculino`, nameF = `${adset.name} femenino`;
      const newMId = await cloneAdsetWithGender(adset, 1, nameM);
      await cloneAdsetWithGender(adset, 2, nameF);
      await pauseAdset(newMId); await pauseAdset(adset.id);
      return `Dividido:\n\u2022 <b>${nameF}</b> activo\n\u2022 <b>${nameM}</b> pausado\n\u2022 Original pausado`;
    }
    default: throw new Error(`Tipo desconocido: ${action.type}`);
  }
}

// -----------------------------------------
// NOTIFICAR
// -----------------------------------------
async function notifyAndAwaitConfirmation(action) {
  const cbConfirm = `confirm_${Date.now()}`;
  const cbCancel  = `cancel_${Date.now()}`;
  pendingActions.set(cbConfirm, action);
  pendingActions.set(cbCancel, { ...action, cancel: true });
  notifiedQuotas.add(action.notifKey);
  await tgSend(
    `\ud83d\udd14 <b>Pulso \u2014 Accion requerida</b>\n\n${action.description}\n\n\u00bfEjecutar?`,
    { inline_keyboard: [[
      { text: '\u2705 Confirmar', callback_data: cbConfirm },
      { text: '\u274c Cancelar',  callback_data: cbCancel  },
    ]]}
  );
  console.log(`[agent] Notificacion: ${action.notifKey}`);
}

// -----------------------------------------
// PROCESAR UPDATES TELEGRAM
// -----------------------------------------
async function processTelegramUpdate(update) {
  // Callback (botones)
  if (update.callback_query) {
    const cq = update.callback_query;
    await tgAnswer(cq.id);
    const data = cq.data, messageId = cq.message?.message_id;
    if (!pendingActions.has(data)) { await tgEdit(messageId, '\u26a0\ufe0f Esta accion ya fue procesada o expiro.'); return; }
    const action = pendingActions.get(data);
    const other  = data.startsWith('confirm_') ? data.replace('confirm_','cancel_') : data.replace('cancel_','confirm_');
    pendingActions.delete(data); pendingActions.delete(other);
    if (action.cancel) { await tgEdit(messageId, `\u274c Cancelado.\n\n${action.description}`); return; }
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
  if (!TG_ALLOWED_IDS.includes(chatId)) return; // solo usuarios autorizados
  const text = msg.text.trim().toLowerCase();

  if (text.startsWith('/estado') || text.startsWith('/status')) {
    await tgSendTo(chatId, '\u23f3 Calculando...');
    await tgSendTo(chatId, buildEstadoMessage());
  } else if (text.startsWith('/salud') || text.startsWith('/health')) {
    await tgSendTo(chatId, '\u23f3 Verificando...');
    await tgSendTo(chatId, await buildSaludMessage());
  } else if (text.startsWith('/sync')) {
    await tgSendTo(chatId, '\u23f3 Forzando sincronizacion...');
    await tgSendTo(chatId, await forceSyncNow());
  } else if (text.startsWith('/ayuda') || text.startsWith('/help') || text.startsWith('/start')) {
    await tgSendTo(chatId,
      '\ud83e\udd16 <b>Pulso Agent \u2014 Comandos</b>\n\n' +
      '/estado \u2014 Resumen de cuotas de todas las encuestas activas\n' +
      '/sync \u2014 Fuerza sincronizacion inmediata con SurveyMonkey\n' +
      '/salud \u2014 Estado del sistema (Firebase, sync server, ultimo dato)\n' +
      '/ayuda \u2014 Muestra esta ayuda\n\n' +
      'Te aviso automaticamente cuando una cuota se completa y necesita accion en Meta Ads.'
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
      const root = snap.val(); if (!root || !appConfig) return;
      const entries = typeof root === 'string' ? { legacy: JSON.parse(root) } : root;
      for (const [sid, raw] of Object.entries(entries)) {
        const payload   = typeof raw === 'string' ? JSON.parse(raw) : raw;
        const surveyId  = payload.surveyId || sid;
        lastSyncData[String(surveyId)] = payload;
        // Resetear watchdog cuando llegan datos nuevos
        if (watchdogAlerted) {
          watchdogAlerted = false;
          await tgSend('\ud83d\udfe2 <b>Sync recuperado</b> \u2014 Datos actualizados correctamente.');
        }
        const info = getMuestraForSurvey(surveyId);
        if (!info?.muestra) continue;
        const actions = evaluateActions(surveyId, payload.rawCases || [], info.survey, info.muestra);
        for (const action of actions) { await notifyAndAwaitConfirmation(action); await sleep(500); }
      }
    } catch (e) { console.error('[firebase] v4sync error:', e.message); }
  });
}

// -----------------------------------------
// HEALTH CHECK HTTP
// -----------------------------------------
const http = require('http');
http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ status: 'ok', agent: 'pulso-agent',
    pending: pendingActions.size / 2, surveys: Object.keys(lastSyncData).length,
    watchdogAlerted, config: !!appConfig }));
}).listen(process.env.PORT || 3001, () => console.log(`[http] Puerto ${process.env.PORT || 3001}`));

// -----------------------------------------
// INICIO
// -----------------------------------------
(async () => {
  console.log('\n  Pulso Agent arrancando...');
  console.log(`  Telegram:    ${TG_TOKEN ? '\u2713' : '\u2717 FALTA TG_TOKEN'}`);
  console.log(`  Meta:        ${META_TOKEN ? '\u2713' : '\u2717 FALTA META_TOKEN'}`);
  console.log(`  Firebase:    \u2713`);
  console.log(`  Cuenta Meta: act_${META_ACCOUNT}`);
  console.log(`  Sync server: ${SYNC_SERVER_URL}`);
  console.log(`  Usuarios:    ${TG_ALLOWED_IDS.join(', ')}\n`);

  if (TG_TOKEN && TG_CHAT_ID) {
    await tgSend(
      '\ud83d\udfe2 <b>Pulso Agent iniciado</b>\nMonitoreando cuotas en tiempo real.\n\n' +
      'Comandos:\n/estado \u2014 ver cuotas\n/sync \u2014 forzar sync\n/salud \u2014 estado del sistema\n/ayuda \u2014 mas info'
    );
  }

  startFirebaseListeners();
  pollTelegram();

  // Reporte automatico cada 6 horas
  setInterval(async () => {
    try {
      await tgSend('\u23f0 <b>Reporte automatico</b>\n\n' + buildEstadoMessage());
      console.log('[agent] Reporte automatico enviado');
    } catch (e) { console.error('[agent] Error reporte:', e.message); }
  }, 6 * 60 * 60 * 1000);

  // Watchdog: alerta si no llegan datos en 90 minutos
  setInterval(async () => {
    try {
      const allSyncs = Object.values(lastSyncData);
      if (!allSyncs.length) return;
      const latest = allSyncs.reduce((a, b) =>
        new Date(a.lastSync||0) > new Date(b.lastSync||0) ? a : b
      );
      if (!latest.lastSync) return;
      const ageMin = Math.floor((Date.now() - new Date(latest.lastSync)) / 60000);
      if (ageMin > 90 && !watchdogAlerted) {
        watchdogAlerted = true;
        await tgSend(
          '\u26a0\ufe0f <b>Pulso Agent \u2014 Sin datos nuevos</b>\n\n' +
          `Hace <b>${ageMin} minutos</b> que no llegan datos de SurveyMonkey.\n` +
          'El sync puede estar caido o pausado.\n\n' +
          'Usa /sync para forzar una actualizacion o /salud para diagnosticar.'
        );
        console.log(`[watchdog] Alerta: sin datos hace ${ageMin}min`);
      }
    } catch (e) { console.error('[watchdog]', e.message); }
  }, 15 * 60 * 1000);

  console.log('[agent] Reporte cada 6hs + watchdog cada 15min activados');
})();
