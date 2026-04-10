/**
 * pulso-agent — Agente de control de cuotas + Meta Ads
 * Pulso Research / MVP DATA SRL
 *
 * Flujo:
 *  1. Escucha Firebase (pulso/v4sync + pulso/v4config)
 *  2. Detecta cuotas completadas
 *  3. Manda alerta Telegram con botones Confirmar/Cancelar
 *  4. Al confirmar → ejecuta acción en Meta Ads
 *
 * Acciones posibles:
 *  - Cuota 50-64 completa en AMBOS géneros → achica adset +30 a 30-49
 *  - Cuota 30-49 completa en AMBOS géneros → pausa adset +30
 *  - Cuota 16-29 completa en UN género    → separa adset en M/F, pausa el completado
 *  - Cuota 16-29 completa en AMBOS géneros → pausa adset 16-29 completo
 */

'use strict';

const axios = require('axios');
const { initializeApp } = require('firebase/app');
const { getDatabase, ref, onValue, get } = require('firebase/database');

// ─────────────────────────────────────────
// CONFIG — variables de entorno en Railway
// ─────────────────────────────────────────
const TG_TOKEN      = process.env.TG_TOKEN;
const TG_CHAT_ID    = process.env.TG_CHAT_ID;
const META_TOKEN    = process.env.META_TOKEN;
const META_ACCOUNT  = process.env.META_ACCOUNT || '1580131239919455';
const FB_API_KEY    = process.env.FB_API_KEY;
const FB_DB_URL     = process.env.FB_DB_URL || 'https://control-cuotas-pulso-default-rtdb.firebaseio.com';

// Grupos de edad del sistema
const AGE_GROUPS_YOUNG = '16-29';
const AGE_GROUPS_OLD   = '+30';
// Sub-rangos dentro de +30 (de mayor a menor — orden de completado esperado)
const SUBRANGES_OLD = [
  { label: '50-64', min: 50, max: 64 },
  { label: '30-49', min: 30, max: 49 },
];

// ─────────────────────────────────────────
// FIREBASE
// ─────────────────────────────────────────
let db;
try {
  const fbApp = initializeApp({ apiKey: FB_API_KEY, databaseURL: FB_DB_URL });
  db = getDatabase(fbApp);
  console.log('[firebase] Conectado a', FB_DB_URL);
} catch (e) {
  console.error('[firebase] Error:', e.message);
  process.exit(1);
}

// ─────────────────────────────────────────
// ESTADO INTERNO
// ─────────────────────────────────────────
let appConfig    = null;   // último estado de pulso/v4config
let syncData     = {};     // { surveyId → payload }
// Acciones pendientes de confirmación Telegram
// key = callbackData string → { description, action }
const pendingActions = new Map();
// Cuotas ya notificadas (para no repetir)
const notifiedQuotas = new Set();
// Offset para Telegram long polling
let tgOffset = 0;

// ─────────────────────────────────────────
// UTILS
// ─────────────────────────────────────────
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
// Mapea grupo de edad del dashboard → rango adset
function adsetRangeForAgeGrp(ageGrp) {
  const n = norm(ageGrp);
  if (n.includes('16') || n.includes('18') || (n.includes('29') && !n.includes('30'))) return AGE_GROUPS_YOUNG;
  return AGE_GROUPS_OLD;
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─────────────────────────────────────────
// TELEGRAM API
// ─────────────────────────────────────────
async function tgRequest(method, body = {}) {
  try {
    const res = await axios.post(
      `https://api.telegram.org/bot${TG_TOKEN}/${method}`,
      body, { timeout: 10000 }
    );
    return res.data;
  } catch (e) {
    console.error(`[telegram] ${method} error:`, e.message);
    return null;
  }
}

async function tgSend(text, replyMarkup = null) {
  const body = { chat_id: TG_CHAT_ID, text, parse_mode: 'HTML' };
  if (replyMarkup) body.reply_markup = replyMarkup;
  return tgRequest('sendMessage', body);
}

async function tgAnswer(callbackQueryId, text = '') {
  return tgRequest('answerCallbackQuery', { callback_query_id: callbackQueryId, text });
}

async function tgEdit(messageId, text, replyMarkup = null) {
  const body = { chat_id: TG_CHAT_ID, message_id: messageId, text, parse_mode: 'HTML' };
  if (replyMarkup) body.reply_markup = replyMarkup;
  else body.reply_markup = { inline_keyboard: [] };
  return tgRequest('editMessageText', body);
}

// ─────────────────────────────────────────
// META ADS API
// ─────────────────────────────────────────
async function metaGet(path, params = {}) {
  const res = await axios.get(`https://graph.facebook.com/v19.0${path}`, {
    params: { access_token: META_TOKEN, ...params },
    timeout: 15000,
  });
  return res.data;
}
async function metaPost(path, data = {}) {
  const res = await axios.post(
    `https://graph.facebook.com/v19.0${path}`,
    { access_token: META_TOKEN, ...data },
    { timeout: 15000 }
  );
  return res.data;
}

// Busca adsets por nombre parcial dentro de la cuenta
async function findAdsetsByName(namePart) {
  try {
    const data = await metaGet(`/act_${META_ACCOUNT}/adsets`, {
      fields: 'id,name,status,targeting,daily_budget,bid_amount,optimization_goal,billing_event,campaign_id',
      limit: 200,
    });
    const needle = norm(namePart);
    return (data.data || []).filter(a => norm(a.name).includes(needle));
  } catch (e) {
    console.error('[meta] findAdsetsByName error:', e.message);
    return [];
  }
}

// Construye el nombre de adset según convención "Estrato X 16-29" o "Estrato X +30"
function adsetName(estrato, range) {
  return `Estrato ${estrato} ${range}`;
}

// Pausa un adset
async function pauseAdset(adsetId) {
  return metaPost(`/${adsetId}`, { status: 'PAUSED' });
}

// Ajusta rango etario de un adset
async function updateAdsetAge(adsetId, ageMin, ageMax) {
  const targeting = { age_min: ageMin };
  if (ageMax && ageMax < 65) targeting.age_max = ageMax;
  return metaPost(`/${adsetId}`, { targeting: JSON.stringify(targeting) });
}

// Duplica un adset con género específico (1=M, 2=F) y devuelve el nuevo ID
async function cloneAdsetWithGender(original, genderCode, newName) {
  try {
    // Obtener config completa del adset original
    const orig = await metaGet(`/${original.id}`, {
      fields: 'name,campaign_id,daily_budget,targeting,optimization_goal,billing_event,bid_amount,status',
    });
    const targeting = typeof orig.targeting === 'string'
      ? JSON.parse(orig.targeting)
      : (orig.targeting || {});

    targeting.genders = [genderCode];

    const body = {
      name:              newName,
      campaign_id:       orig.campaign_id,
      daily_budget:      orig.daily_budget,
      targeting:         JSON.stringify(targeting),
      optimization_goal: orig.optimization_goal,
      billing_event:     orig.billing_event,
      status:            'ACTIVE',
    };
    if (orig.bid_amount) body.bid_amount = orig.bid_amount;

    const res = await metaPost(`/act_${META_ACCOUNT}/adsets`, body);
    return res.id;
  } catch (e) {
    console.error('[meta] cloneAdsetWithGender error:', e.message);
    throw e;
  }
}

// ─────────────────────────────────────────
// LÓGICA DE CUOTAS
// ─────────────────────────────────────────

// Obtiene la muestra activa para una encuesta
function getMuestraForSurvey(surveyId) {
  if (!appConfig) return null;
  const surveys = appConfig.activeSurveys || [];
  const sv = surveys.find(s => String(s.smSurveyId) === String(surveyId));
  if (!sv) return null;
  const muestra = (appConfig.muestras || []).find(m => m.id === sv.muestraId);
  return { survey: sv, muestra };
}

// Calcula conteos actuales de casos por clave gen||ageGrp||estrato
function computeCounts(rawCases, muestra) {
  if (!muestra || !rawCases) return {};
  const groups = muestra.ageGroups || [];
  const bounds = parseAgeBounds(groups);
  const counts = {};
  for (const c of rawCases) {
    const ageGrp = getAgeGrp(parseInt(c.edad), bounds, groups);
    const key = `${c.gen}||${ageGrp}||${c.estrato}`;
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

// Evalúa qué acciones son necesarias y devuelve lista de acciones pendientes
function evaluateActions(surveyId, rawCases, survey, muestra) {
  const quotas  = survey.quotas || {};   // key → target
  const counts  = computeCounts(rawCases, muestra);
  const actions = [];
  const ageGroups = muestra.ageGroups || [];

  // Agrupar por estrato
  const estratos = [...new Set(Object.keys(quotas).map(k => k.split('||')[2]))];

  for (const estrato of estratos) {
    // ── ADSET 16-29 ──────────────────────────────────────────
    // Encontrar el grupo de edad joven (el que contiene 16-29 o 18-29)
    const youngGroup = ageGroups.find(g => {
      const n = norm(g); return n.startsWith('16') || n.startsWith('18');
    });
    if (youngGroup) {
      const keyF = `Femenino||${youngGroup}||${estrato}`;
      const keyM = `Masculino||${youngGroup}||${estrato}`;
      const tF = quotas[keyF] || 0;
      const tM = quotas[keyM] || 0;
      const cF = counts[keyF] || 0;
      const cM = counts[keyM] || 0;

      const doneF = tF > 0 && cF >= tF;
      const doneM = tM > 0 && cM >= tM;
      const notifKey16F = `${surveyId}||${estrato}||${youngGroup}||F`;
      const notifKey16M = `${surveyId}||${estrato}||${youngGroup}||M`;
      const notifKey16Both = `${surveyId}||${estrato}||${youngGroup}||both`;

      if (doneF && doneM && !notifiedQuotas.has(notifKey16Both)) {
        actions.push({
          notifKey: notifKey16Both,
          type: 'pause',
          adsetNamePart: adsetName(estrato, AGE_GROUPS_YOUNG),
          description: `✅ Cuota <b>${youngGroup} - Estrato ${estrato}</b> completa en ambos géneros (M:${cM}/${tM} F:${cF}/${tF}).\n\n→ Pausar adset <b>${adsetName(estrato, AGE_GROUPS_YOUNG)}</b>`,
        });
      } else if (doneF && !doneM && !notifiedQuotas.has(notifKey16F)) {
        actions.push({
          notifKey: notifKey16F,
          type: 'split_pause_female',
          adsetNamePart: adsetName(estrato, AGE_GROUPS_YOUNG),
          estrato, ageGrp: youngGroup,
          description: `⚠️ Cuota <b>${youngGroup} - Estrato ${estrato} - Femenino</b> completa (${cF}/${tF}).\nMasculino aún abierto (${cM}/${tM}).\n\n→ Separar adset en M/F y pausar Femenino`,
        });
      } else if (doneM && !doneF && !notifiedQuotas.has(notifKey16M)) {
        actions.push({
          notifKey: notifKey16M,
          type: 'split_pause_male',
          adsetNamePart: adsetName(estrato, AGE_GROUPS_YOUNG),
          estrato, ageGrp: youngGroup,
          description: `⚠️ Cuota <b>${youngGroup} - Estrato ${estrato} - Masculino</b> completa (${cM}/${tM}).\nFemenino aún abierto (${cF}/${tF}).\n\n→ Separar adset en M/F y pausar Masculino`,
        });
      }
    }

    // ── ADSET +30 — sub-rangos ────────────────────────────────
    for (const subrange of SUBRANGES_OLD) {
      const matchedGroup = ageGroups.find(g => {
        const bounds = parseAgeBounds([g])[0];
        return bounds[0] >= subrange.min && bounds[1] <= subrange.max;
      });
      if (!matchedGroup) continue;

      const keyF = `Femenino||${matchedGroup}||${estrato}`;
      const keyM = `Masculino||${matchedGroup}||${estrato}`;
      const tF = quotas[keyF] || 0;
      const tM = quotas[keyM] || 0;
      if (!tF && !tM) continue;

      const cF = counts[keyF] || 0;
      const cM = counts[keyM] || 0;
      const doneF = tF > 0 && cF >= tF;
      const doneM = tM > 0 && cM >= tM;
      const notifKeyBoth = `${surveyId}||${estrato}||${matchedGroup}||both`;

      if (doneF && doneM && !notifiedQuotas.has(notifKeyBoth)) {
        // Determinar acción: ¿quedan subrangos abajo?
        const currentIdx = SUBRANGES_OLD.indexOf(subrange);
        const nextSubrange = SUBRANGES_OLD[currentIdx + 1]; // el que queda por debajo

        if (!nextSubrange) {
          // Era el último subrango (+30 completo) → pausar adset
          actions.push({
            notifKey: notifKeyBoth,
            type: 'pause',
            adsetNamePart: adsetName(estrato, AGE_GROUPS_OLD),
            description: `✅ Cuota <b>${matchedGroup} - Estrato ${estrato}</b> completa en ambos géneros.\nTodos los rangos de +30 completos.\n\n→ Pausar adset <b>${adsetName(estrato, AGE_GROUPS_OLD)}</b>`,
          });
        } else {
          // Acotar adset al subrango siguiente
          actions.push({
            notifKey: notifKeyBoth,
            type: 'update_age',
            adsetNamePart: adsetName(estrato, AGE_GROUPS_OLD),
            ageMin: nextSubrange.min,
            ageMax: nextSubrange.max,
            description: `✅ Cuota <b>${matchedGroup} - Estrato ${estrato}</b> completa en ambos géneros (M:${cM}/${tM} F:${cF}/${tF}).\n\n→ Acotar adset <b>${adsetName(estrato, AGE_GROUPS_OLD)}</b> a <b>${nextSubrange.min}-${nextSubrange.max} años</b>`,
          });
        }
      }
    }
  }

  return actions;
}

// ─────────────────────────────────────────
// EJECUTAR ACCIÓN EN META
// ─────────────────────────────────────────
async function executeAction(action) {
  const adsets = await findAdsetsByName(action.adsetNamePart);
  if (!adsets.length) {
    throw new Error(`No se encontró ningún adset con nombre que contenga "${action.adsetNamePart}"`);
  }
  const adset = adsets[0];

  switch (action.type) {
    case 'pause':
      await pauseAdset(adset.id);
      return `Adset <b>${adset.name}</b> pausado ✓`;

    case 'update_age':
      await updateAdsetAge(adset.id, action.ageMin, action.ageMax);
      return `Adset <b>${adset.name}</b> acotado a ${action.ageMin}-${action.ageMax} años ✓`;

    case 'split_pause_female': {
      const nameM = `${adset.name} - Masculino`;
      const nameF = `${adset.name} - Femenino`;
      const newMId = await cloneAdsetWithGender(adset, 1, nameM); // 1 = M
      const newFId = await cloneAdsetWithGender(adset, 2, nameF); // 2 = F
      await pauseAdset(newFId);
      await pauseAdset(adset.id); // pausar original
      return `Adset dividido:\n• <b>${nameM}</b> activo ✓\n• <b>${nameF}</b> pausado ✓\n• Original pausado ✓`;
    }

    case 'split_pause_male': {
      const nameM = `${adset.name} - Masculino`;
      const nameF = `${adset.name} - Femenino`;
      const newMId = await cloneAdsetWithGender(adset, 1, nameM);
      const newFId = await cloneAdsetWithGender(adset, 2, nameF);
      await pauseAdset(newMId);
      await pauseAdset(adset.id); // pausar original
      return `Adset dividido:\n• <b>${nameF}</b> activo ✓\n• <b>${nameM}</b> pausado ✓\n• Original pausado ✓`;
    }

    default:
      throw new Error(`Tipo de acción desconocido: ${action.type}`);
  }
}

// ─────────────────────────────────────────
// NOTIFICAR VÍA TELEGRAM
// ─────────────────────────────────────────
async function notifyAndAwaitConfirmation(action) {
  const cbConfirm = `confirm_${Date.now()}`;
  const cbCancel  = `cancel_${Date.now()}`;

  pendingActions.set(cbConfirm, action);
  pendingActions.set(cbCancel, { ...action, cancel: true });
  notifiedQuotas.add(action.notifKey); // marcar como notificada

  const keyboard = {
    inline_keyboard: [[
      { text: '✅ Confirmar', callback_data: cbConfirm },
      { text: '❌ Cancelar', callback_data: cbCancel },
    ]],
  };

  await tgSend(
    `🔔 <b>Pulso — Acción requerida</b>\n\n${action.description}\n\n¿Ejecutar?`,
    keyboard
  );
  console.log(`[agent] Notificación Telegram enviada para: ${action.notifKey}`);
}

// ─────────────────────────────────────────
// PROCESAR CALLBACKS DE TELEGRAM
// ─────────────────────────────────────────
async function processTelegramUpdate(update) {
  const cq = update.callback_query;
  if (!cq) return;

  const data      = cq.data;
  const messageId = cq.message?.message_id;
  await tgAnswer(cq.id);

  if (!pendingActions.has(data)) {
    await tgEdit(messageId, '⚠️ Esta acción ya fue procesada o expiró.');
    return;
  }

  const action = pendingActions.get(data);
  // Limpiar ambos botones
  const cbOther = data.startsWith('confirm_')
    ? data.replace('confirm_', 'cancel_')
    : data.replace('cancel_', 'confirm_');
  pendingActions.delete(data);
  pendingActions.delete(cbOther);

  if (action.cancel) {
    await tgEdit(messageId, `❌ Acción cancelada.\n\n${action.description}`);
    console.log(`[agent] Acción cancelada por usuario: ${action.notifKey}`);
    return;
  }

  // Ejecutar
  await tgEdit(messageId, `⏳ Ejecutando...\n\n${action.description}`);
  try {
    const result = await executeAction(action);
    await tgEdit(messageId, `✅ <b>Ejecutado</b>\n\n${action.description}\n\n<i>${result}</i>`);
    console.log(`[agent] Acción ejecutada: ${action.notifKey}`);
  } catch (e) {
    await tgEdit(messageId, `❌ <b>Error al ejecutar</b>\n\n${action.description}\n\n<i>${e.message}</i>`);
    console.error(`[agent] Error ejecutando acción:`, e.message);
  }
}

// ─────────────────────────────────────────
// TELEGRAM LONG POLLING
// ─────────────────────────────────────────
async function pollTelegram() {
  while (true) {
    try {
      const res = await tgRequest('getUpdates', {
        offset: tgOffset,
        timeout: 30,
        allowed_updates: ['callback_query'],
      });
      if (res?.result?.length) {
        for (const update of res.result) {
          tgOffset = update.update_id + 1;
          await processTelegramUpdate(update);
        }
      }
    } catch (e) {
      console.error('[telegram polling]', e.message);
      await sleep(5000);
    }
  }
}

// ─────────────────────────────────────────
// FIREBASE LISTENERS
// ─────────────────────────────────────────
function startFirebaseListeners() {
  // Escuchar config (muestras, quotas por survey)
  onValue(ref(db, 'pulso/v4config'), (snap) => {
    try {
      const raw = snap.val();
      if (!raw) return;
      appConfig = typeof raw === 'string' ? JSON.parse(raw) : raw;
      console.log(`[firebase] Config actualizada — ${(appConfig.activeSurveys || []).length} encuesta(s) activa(s)`);
    } catch (e) {
      console.error('[firebase] Error parseando v4config:', e.message);
    }
  });

  // Escuchar sync data (rawCases por survey)
  onValue(ref(db, 'pulso/v4sync'), async (snap) => {
    try {
      const root = snap.val();
      if (!root || !appConfig) return;

      const entries = typeof root === 'string'
        ? { legacy: JSON.parse(root) }
        : root;

      for (const [sid, raw] of Object.entries(entries)) {
        const payload = typeof raw === 'string' ? JSON.parse(raw) : raw;
        const surveyId = payload.surveyId || sid;

        const muestraInfo = getMuestraForSurvey(surveyId);
        if (!muestraInfo || !muestraInfo.muestra) continue;

        const { survey, muestra } = muestraInfo;
        const rawCases = payload.rawCases || [];

        const actions = evaluateActions(surveyId, rawCases, survey, muestra);
        for (const action of actions) {
          await notifyAndAwaitConfirmation(action);
          await sleep(500); // no spamear Telegram
        }
      }
    } catch (e) {
      console.error('[firebase] Error procesando v4sync:', e.message);
    }
  });
}

// ─────────────────────────────────────────
// HEALTH CHECK HTTP (para Railway)
// ─────────────────────────────────────────
const http = require('http');
http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status:   'ok',
    agent:    'pulso-agent',
    pending:  pendingActions.size / 2,
    notified: notifiedQuotas.size,
    config:   !!appConfig,
  }));
}).listen(process.env.PORT || 3001, () => {
  console.log(`[http] Health check en puerto ${process.env.PORT || 3001}`);
});

// ─────────────────────────────────────────
// INICIO
// ─────────────────────────────────────────
(async () => {
  console.log('\n  Pulso Agent arrancando...');
  console.log(`  Telegram:  ${TG_TOKEN ? '✓' : '✗ FALTA TG_TOKEN'}`);
  console.log(`  Meta:      ${META_TOKEN ? '✓' : '✗ FALTA META_TOKEN'}`);
  console.log(`  Firebase:  ✓`);
  console.log(`  Cuenta Meta: act_${META_ACCOUNT}\n`);

  // Mensaje de inicio a Telegram
  if (TG_TOKEN && TG_CHAT_ID) {
    await tgSend('🟢 <b>Pulso Agent iniciado</b>\nMonitoreando cuotas en tiempo real. Te aviso cuando haya acciones para ejecutar.');
  }

  startFirebaseListeners();
  pollTelegram(); // no await — corre en paralelo
})();
