// src/utils/marketMonitor.js
import { resolveUserLang } from '../cache.js';
import { usersCollection, client } from '../db.js';
import { MARKET_BATCH_SIZE, MARKET_BATCH_PAUSE_MS } from '../constants.js';
import { buildPorNetflowsBlock } from '../porNetflows.js';

const SNAPSHOT_CACHE_MS = Number(process.env.SNAPSHOT_CACHE_MS ?? 60_000);
const _snapCache = new Map();

const esc = (s) => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const B = (s) => `<b>${esc(s)}</b>`;
const BU = (s) => `<b><u>${esc(s)}</u></b>`;
const nearZero = (v) => Number.isFinite(v) && Math.abs(v) < 1e-8;
const isNum = (v) => Number.isFinite(Number(v));

function humanFmt(n) {
  if (!Number.isFinite(n)) return '—';
  try {
    if (Math.abs(n) >= 1000) return Intl.NumberFormat('ru-RU',{maximumFractionDigits:0}).format(Math.round(n));
    if (Math.abs(n) >= 1)   return Intl.NumberFormat('ru-RU',{maximumFractionDigits:2}).format(Number(n.toFixed(2)));
    return Number(n).toPrecision(6).replace(/(?:\.0+$|(?<=\.[0-9]*?)0+)$/,'');
  } catch { return String(n); }
}

function humanFmtEN(n) {
  if (!Number.isFinite(n)) return '—';
  try {
    if (Math.abs(n) >= 1000) return Intl.NumberFormat('en-US',{maximumFractionDigits:0}).format(Math.round(n));
    if (Math.abs(n) >= 1)   return Intl.NumberFormat('en-US',{maximumFractionDigits:2}).format(Number(n.toFixed(2)));
    return Number(n).toPrecision(6).replace(/(?:\.0+$|(?<=\.[0-9]*?)0+)$/,'');
  } catch { return String(n); }
}

function abbrevWithUnit(n, isEn=false) {
  if(!Number.isFinite(n)) return '';
  const v = Math.abs(n);
  if (v >= 1_000_000_000_000) return `${(v/1_000_000_000_000).toFixed(2)} ${isEn?'T':'трлн'}`;
  if (v >= 1_000_000_000)     return `${(v/1_000_000_000).toFixed(2)} ${isEn?'B':'млрд'}`;
  if (v >= 1_000_000)         return `${(v/1_000_000).toFixed(2)} ${isEn?'M':'млн'}`;
  if (v >= 1_000)             return `${(v/1_000).toFixed(2)} ${isEn?'K':'тыс.'}`;
  return `${v.toFixed(2)}`;
}

function fmtFunding(v) {
  if(!Number.isFinite(v)) return '—';
  return Number(v).toFixed(8).replace(/\.0+$|0+$/,'');
}

function circleByDelta(x) {
  if(!Number.isFinite(x) || x===0) return '⚪';
  return x>0?'🟢':'🔴';
}

function verdictLabelFromEmoji(emoji, isEn){
  switch (emoji) {
    case '🟢': return isEn ? 'longs inflow' : 'приток лонгов';
    case '🟡': return isEn ? 'short-cover'  : 'short-cover';
    case '🟠': return isEn ? 'absorption'   : 'впитывание';
    default:   return isEn ? 'cooling'      : 'охлаждение';
  }
}

function riskBar(score){
  const n=Math.max(0,Math.min(10,Math.round((score||0)*10)));
  return '🟥'.repeat(n)+'⬜'.repeat(10-n);
}

function priceChangeRisk(pct24h){
  if(!Number.isFinite(pct24h)) return 0;
  const mag = Math.min(1, Math.abs(pct24h)/8);
  return mag;
}

function fundingRiskFromNow(f){
  if(!Number.isFinite(f)) return 0;
  return Math.min(1, Math.abs(f)*10000/30);
}

function sentimentRiskFromLS(longPct){
  if(!Number.isFinite(longPct)) return 0;
  if(longPct>=60) return Math.min(1, (longPct-60)/15);
  if(longPct<=40) return Math.min(1, (40-longPct)/15);
  return 0;
}

function riskFromOiCvd(verdictEmoji){
  switch (verdictEmoji) {
    case '🟢': return 0.45;
    case '🟡': return 0.55;
    case '🟠': return 0.65;
    default:   return 0.35;
  }
}

function riskFromFgi(v){
  if(!Number.isFinite(v)) return 0.35;
  if (v <= 24) return 0.55;
  if (v <= 44) return 0.45;
  if (v <= 54) return 0.35;
  if (v <= 74) return 0.50;
  return 0.60;
}

function riskFromBreadth(tot){
  if (!tot || !Number.isFinite(tot.d1) || !Number.isFinite(tot.d2) || !Number.isFinite(tot.d3)) return 0.35;
  const mean = (tot.d1 + tot.d2 + tot.d3)/3;
  if (mean >= 2) return 0.30;
  if (mean >= 0.5) return 0.35;
  if (mean >= -0.5) return 0.40;
  if (mean >= -2) return 0.50;
  return 0.60;
}

function riskFromSpx(pct){
  if(!Number.isFinite(pct)) return 0.35;
  if (pct >= 1.0) return 0.30;
  if (pct >= 0.2) return 0.33;
  if (pct >= -0.2) return 0.38;
  if (pct >= -1.0) return 0.48;
  return 0.58;
}

function computeRiskV2(symSnap, extras, symbol){
  const priceRisk = priceChangeRisk(symSnap?.pct24);
  const fundingRisk = fundingRiskFromNow(symSnap?.fundingNow);
  const sentimentRisk = sentimentRiskFromLS(symSnap?.longShort?.longPct);
  const oi = symbol==='BTC' ? extras?.oiCvdBTC : extras?.oiCvdETH;
  const oiRisk = oi ? riskFromOiCvd(oi.verdictEmoji) : 0.35;
  const fgi = Number.isFinite(extras?.fgiNow) ? extras.fgiNow : (Number.isFinite(extras?.snapshots?.BTC?.fgiValue)?extras.snapshots.BTC.fgiValue:null);
  const fgiRisk = riskFromFgi(fgi);
  const breadthRisk = riskFromBreadth(extras?.totals || null);
  const spxRisk = riskFromSpx(extras?.spx?.pct);
  const s =
    0.30*priceRisk +
    0.15*fundingRisk +
    0.20*sentimentRisk +
    0.15*oiRisk +
    0.10*fgiRisk +
    0.10*breadthRisk +
    0.00*spxRisk;
  return Math.max(0, Math.min(1, s));
}

function fearGreedBarColorized(v){
  const val = Number(v);
  if (!Number.isFinite(val) || val < 0 || val > 100) return '⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜';
  const filled = Math.max(0, Math.min(10, Math.floor(val/10)));
  let color = '🟨';
  if (val <= 44) color = '🟥';
  else if (val <= 54) color = '🟨';
  else if (val <= 74) color = '🟩';
  else color = '🟩';
  return color.repeat(filled) + '⬜'.repeat(10 - filled);
}

function fgiClassFromValue(v, isEn){
  const val = Number(v);
  let key = null;
  if (!Number.isFinite(val)) key = null;
  else if (val <= 24) key = 'Extreme Fear';
  else if (val <= 44) key = 'Fear';
  else if (val <= 54) key = 'Neutral';
  else if (val <= 74) key = 'Greed';
  else key = 'Extreme Greed';
  const dict = {
    'Extreme Fear': { ru: 'Экстремальный страх', en: 'Extreme Fear' },
    'Fear':         { ru: 'Страх',               en: 'Fear' },
    'Neutral':      { ru: 'Нейтрально',          en: 'Neutral' },
    'Greed':        { ru: 'Жадность',            en: 'Greed' },
    'Extreme Greed':{ ru: 'Экстремальная жадность', en: 'Extreme Greed' }
  };
  if (!key) return null;
  return isEn ? dict[key].en : dict[key].ru;
}

function renderLsBlock(ls, isEn, label){
  const lbl = label || (isEn ? 'Asset' : 'Актив');
  if (!ls || !Number.isFinite(ls.longPct) || !Number.isFinite(ls.shortPct)) return `${esc(lbl)}: —`;
  const greens = Math.max(0, Math.min(10, Math.round(ls.longPct/10)));
  const reds   = 10 - greens;
  const bar = '🟩'.repeat(greens) + '🟥'.repeat(reds);
  const L = B(isEn ? 'Longs' : 'Лонги');
  const S = B(isEn ? 'Shorts' : 'Шорты');
  return `${esc(lbl)}:\n• ${L} ${B(`${ls.longPct}%`)} | ${S} ${B(`${ls.shortPct}%`)}\n${bar}`;
}

function formatKyiv(tsEpoch, tsIso) {
  try {
    const d = Number.isFinite(Number(tsEpoch)) && Number(tsEpoch) > 0
      ? new Date(Number(tsEpoch))
      : (tsIso ? new Date(tsIso) : new Date());
    const ru = new Intl.DateTimeFormat('ru-RU',{ timeZone:'Europe/Kyiv', year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit' }).format(d);
    const en = new Intl.DateTimeFormat('en-GB',{ timeZone:'Europe/Kyiv', year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit' }).format(d);
    return { ru, en };
  } catch {
    const now = new Date();
    const ru = new Intl.DateTimeFormat('ru-RU',{ timeZone:'Europe/Kyiv', year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit' }).format(now);
    const en = new Intl.DateTimeFormat('en-GB',{ timeZone:'Europe/Kyiv', year:'numeric', month:'2-digit', day:'2-digit', hour:'2-digit', minute:'2-digit' }).format(now);
    return { ru, en };
  }
}

function conciseRsiAdvice(v,isEn){
  if (!Number.isFinite(v)) return isEn?'No RSI — rely on price/volume.':'Без RSI — опора на цену/объём.';
  if (v >= 70) return isEn?'Overbought risk — tighten risk, watch for divergences.':'Риск перекупленности — ужать риск, искать дивергенции.';
  if (v <= 30) return isEn?'Oversold — wait for reversal; avoid naked shorts.':'Перепроданность — ждать разворота, не шортить без подтверждения.';
  return isEn?'Momentum neutral — trade trend with stops.':'Импульс нейтрален — работать по тренду и стопам.';
}

function conciseFlowsAdvice(usd,isEn){
  if (!Number.isFinite(usd)) return isEn?'Don’t rely on flows alone; decide by confluence.':'Не полагайся на потоки отдельно; решения по совокупности сигналов.';
  if (usd > 0) return isEn?'Inflow — potential sell pressure; avoid all-in on pumps.':'Приток — возможные продажи; не входить all-in на росте.';
  if (usd < 0) return isEn?'Outflow — supportive; longs only on confirmation.':'Отток — поддержка; лонги только по подтверждению.';
  return isEn?'Flat — stick to plan.':'Ровно — держать план.';
}

function conciseFundingAdvice(f,isEn){
  if (!Number.isFinite(f)) return isEn?'Evaluate without funding; don’t overrate it.':'Оценивай без funding; не переоценивать метрику.';
  if (Math.abs(f) > 0.0003) return isEn?'Elevated funding — cut leverage, be ready for squeezes.':'Повышенный funding — резать плечо, готовность к сквизам.';
  return isEn?'Moderate funding — don’t add leverage without confirmation.':'Умеренный funding — плечо не увеличивать без подтверждений.';
}

function flowsHeaderLine(sym, isEn){
  const now = Number(sym?.netFlowsUSDNow);
  const prev = Number(sym?.netFlowsUSDPrev);
  const diff = Number(sym?.netFlowsUSDDiff);
  if (!Number.isFinite(now) && !Number.isFinite(prev)) return '—';
  const abbrVal = Number.isFinite(now) ? `${now>=0?'+':'−'}${abbrevWithUnit(Math.abs(now), isEn)}` : '';
  const abbr = abbrVal ? B(abbrVal) : '—';
  let deltaPart = '';
  if (Number.isFinite(prev) && Math.abs(prev) > 0 && Number.isFinite(diff)) {
    const diffPct = (diff/Math.abs(prev))*100;
    if (Number.isFinite(diffPct)) {
      const circ = circleByDelta(diffPct);
      deltaPart = ` ${circ}(${B(`${diffPct>0?'+':''}${diffPct.toFixed(2)}%`)} ${isEn?'vs prev 24h':'к пред. 24ч'})`;
    }
  }
  return `${abbr}${deltaPart}`;
}

function pickSubsetBySymbols(snapshots, symbols){
  const out={};
  for(const s of symbols){
    if (snapshots?.[s]) {
      out[s]=snapshots[s];
    }
  }
  return out;
}

async function findClosestWith(db, collection, target, hasValue, windowMs=48*3600*1000){
  const minTs = target - windowMs;
  const maxTs = target + windowMs;
  const q = { at: { $gte: minTs, $lte: maxTs } };
  const proj = { at:1, snapshots:1, btcDominancePct:1, spx:1, totals:1, oiCvd:1 };
  const cur = db.collection(collection).find(q, { projection: proj }).sort({ at: 1 }).limit(1000);
  let best=null, bestDist=Infinity;
  while (await cur.hasNext()) {
    const d = await cur.next();
    if (hasValue(d)) {
      const dist = Math.abs(Number(d.at) - target);
      if (dist < bestDist) {
        best = d;
        bestDist = dist;
      }
    }
  }
  if (best) return best;
  const cur2 = db.collection(collection).find({}, { projection: proj }).sort({ at: -1 }).limit(500);
  best=null;
  bestDist=Infinity;
  while (await cur2.hasNext()) {
    const d = await cur2.next();
    if (hasValue(d)) {
      const dist = Math.abs(Number(d.at) - target);
      if (dist < bestDist) {
        best = d;
        bestDist = dist;
      }
    }
  }
  return best;
}

async function findLatestDocWith(db, collection, hasValue){
  const cur = db.collection(collection).find({}, { projection: { at:1, snapshots:1, btcDominancePct:1, spx:1, totals:1, oiCvd:1 } }).sort({ at: -1 }).limit(500);
  while (await cur.hasNext()) {
    const d = await cur.next();
    if (hasValue(d)) return d;
  }
  return null;
}

export async function getMarketSnapshot(symbols=['BTC','ETH','PAXG']){
  const dbName = process.env.DB_NAME || 'crypto_alert_dev';
  const collection = process.env.COLLECTION || 'marketSnapshots';
  const db = client.db(dbName);

  const cacheKey = symbols.slice().sort().join(',');
  const now = Date.now();
  const hit = _snapCache.get(cacheKey);
  if (hit && now - hit.ts < SNAPSHOT_CACHE_MS) return hit.data;

  const freshest = await db.collection(collection).find(
    {},
    {
      projection: {
        snapshots: 1,
        at: 1,
        atIsoKyiv: 1,
        btcDominancePct: 1,
        spx: 1,
        totals: 1,
        oiCvd: 1,
        capTop: 1,
        cryptoquant: 1,
        gemini: 1,
        macro: 1,
      }
    }
  ).sort({ at: -1 }).limit(1).next();

  if (process.env.DEBUG_OICVD === '1') {
    const atLabel = freshest?.atIsoKyiv || new Date(freshest?.at || Date.now()).toISOString();
    console.log('[OI/CVD DEBUG] at:', atLabel);
  }
  if (!freshest || !freshest.snapshots) {
    const miss = { ok:false, reason:'no_snapshot' };
    _snapCache.set(cacheKey, { ts: now, data: miss });
    return miss;
  }

  let geminiFinal = freshest.gemini || null;

  if (!geminiFinal) {
    const coll = db.collection(collection);

    const prevDocs = await coll.find(
      { at: { $lt: freshest.at }, gemini: { $exists: true, $ne: null } },
      { projection: { at: 1, gemini: 1, atIsoKyiv: 1 } }
    )
      .sort({ at: -1 })
      .limit(5)
      .toArray();

    if (prevDocs && prevDocs.length > 0) {
      const fallback = prevDocs[0];
      geminiFinal = fallback.gemini;

      const adminId = Number(process.env.ADMIN_CHAT_ID);
      if (adminId) {
        try {
          await bot.telegram.sendMessage(
            adminId,
            `❗ Нет Gemini отчёта за ${freshest.atIsoKyiv || freshest.at}\n` +
            `Взял отчёт за ${fallback.atIsoKyiv || fallback.at}`
          );
        } catch (e) {
          console.error('[ADMIN_NOTIFY_FAIL]', e);
        }
      }
    } else {
      const adminId = Number(process.env.ADMIN_CHAT_ID);
      if (adminId) {
        try {
          await bot.telegram.sendMessage(
            adminId,
            `⚠️ Нет вообще ни одного Gemini отчёта в базе. Последний snapshot: ${freshest.atIsoKyiv || freshest.at}`
          );
        } catch {}
      }
    }
  }

  const subset = pickSubsetBySymbols(freshest.snapshots, symbols);

  const domNowDoc = isNum(freshest.btcDominancePct) ? freshest : (await findLatestDocWith(db, collection, d => isNum(d?.btcDominancePct)));
  const domNowVal = isNum(domNowDoc?.btcDominancePct) ? Number(domNowDoc.btcDominancePct) : null;
  let domDeltaVal = null;
  if (isNum(domNowDoc?.at) && isNum(domNowVal)) {
    const target = Number(domNowDoc.at) - 24*3600*1000;
    const ref = await findClosestWith(db, collection, target, d => isNum(d?.btcDominancePct));
    const refVal = isNum(ref?.btcDominancePct) ? Number(ref.btcDominancePct) : null;
    if (isNum(refVal) && refVal !== 0) domDeltaVal = ((domNowVal - refVal) / refVal) * 100;
  }

  let spxNowDoc = isNum(freshest?.spx?.price) ? freshest : (await findLatestDocWith(db, collection, d => isNum(d?.spx?.price)));
  const spxNowPrice = isNum(spxNowDoc?.spx?.price) ? Number(spxNowDoc.spx.price) : null;
  let spxNowPct = isNum(spxNowDoc?.spx?.pct) ? Number(spxNowDoc.spx.pct) : null;
  if (isNum(spxNowDoc?.at) && isNum(spxNowPrice)) {
    const target = Number(spxNowDoc.at) - 24*3600*1000;
    const ref = await findClosestWith(db, collection, target, d => isNum(d?.spx?.price));
    const refPrice = isNum(ref?.spx?.price) ? Number(ref.spx.price) : null;
    if (isNum(refPrice) && refPrice !== 0) spxNowPct = ((spxNowPrice - refPrice)/refPrice)*100;
  }
  const spx = { price: isNum(spxNowPrice) ? spxNowPrice : null, pct: isNum(spxNowPct) ? spxNowPct : null, src: spxNowDoc?.spx?.src || null };

  const totals = freshest?.totals ?? null;

  const fgiNow = isNum(freshest?.snapshots?.BTC?.fgiValue) ? Number(freshest.snapshots.BTC.fgiValue) : null;
  let fgiDelta = null;
  if (isNum(freshest?.at) && isNum(fgiNow)) {
    const target = Number(freshest.at) - 24*3600*1000;
    const ref = await findClosestWith(db, collection, target, d => isNum(d?.snapshots?.BTC?.fgiValue));
    const refVal = isNum(ref?.snapshots?.BTC?.fgiValue) ? Number(ref.snapshots.BTC.fgiValue) : null;
    if (isNum(refVal)) fgiDelta = fgiNow - refVal;
  }

  const leadersTop = freshest?.capTop && Array.isArray(freshest.capTop.absTop10)
    ? { windowLabel: freshest.capTop.windowLabel || '', items: freshest.capTop.absTop10 }
    : null;

  const result = {
    ok:true,
    snapshots: subset,
    fetchedAt: freshest.at,
    atIsoKyiv: freshest.atIsoKyiv || '',
    btcDominancePct: isNum(domNowVal) ? domNowVal : null,
    btcDominanceDelta: isNum(domDeltaVal) ? domDeltaVal : null,
    spx,
    totals,
    fgiNow: isNum(fgiNow) ? fgiNow : null,
    fgiDelta: isNum(fgiDelta) ? fgiDelta : null,
    oiCvdBTC: freshest?.oiCvd?.BTC || null,
    oiCvdETH: freshest?.oiCvd?.ETH || null,
    leadersTop,
    cryptoquant: freshest.cryptoquant || null,
    gemini: geminiFinal,
    macro: freshest.macro || null
  };

  _snapCache.set(cacheKey, { ts: now, data: result });
  return result;
}

function oiCvdLine(symbol, snap, isEn, priceNow){
  if (!snap || (!Number.isFinite(snap.oiChangePct) && !Number.isFinite(snap.cvd) && !Number.isFinite(snap.cvdUSD))) return '—';
  const circ = snap.verdictEmoji || '⚪️';
  const oiTxt = Number.isFinite(snap.oiChangePct) ? `${snap.oiChangePct>0?'+':''}${snap.oiChangePct.toFixed(2)}%` : '—';
  const oiLabel = isEn ? `OI Δ (${snap.windowLabel})` : `OI Δ (${snap.windowLabel})`;
  let cvdUsdTxt = '—';
  if (Number.isFinite(snap.cvdUSD)) {
    const abs = Math.abs(snap.cvdUSD);
    cvdUsdTxt = `${snap.cvdUSD>=0?'+':'−'}$${abbrevWithUnit(abs, true)}`;
  } else if (Number.isFinite(snap.cvd) && Number.isFinite(priceNow)) {
    const usd = Math.abs(snap.cvd * priceNow);
    const sign = snap.cvd >= 0 ? '+' : '−';
    cvdUsdTxt = `${sign}$${abbrevWithUnit(usd, true)}`;
  }
  const cvdLabel = isEn ? `CVD (${snap.windowLabel})` : `CVD (${snap.windowLabel})`;
  const verdictTxt = verdictLabelFromEmoji(snap.verdictEmoji, isEn);
  return `${symbol}: ${oiLabel}: ${B(oiTxt)} | ${cvdLabel}: ${B(cvdUsdTxt)} — ${circ} ${verdictTxt}`;
}

export async function buildMorningReportParts(
  snapshots,
  lang = 'ru',
  tsIsoKyiv = '',
  tsEpoch = null,
  extras = {}
) {
  const isEn = String(lang).toLowerCase().startsWith('en');

  const T = isEn
    ? {
      report: 'REPORT',
      asof: 'As of',
      price: 'Price *¹',
      dom: 'BTC Dominance *²',
      fgi: 'Fear & Greed *³',
      ls: 'Longs vs Shorts *⁴',
      macro: 'Macro Data *⁵',
      volumes: '24h Volume *⁶',
      rsi: 'RSI (14) *⁷',
      oicvd: 'OI (open interest) and CVD (cumulative delta volume) *⁸',
      leaders: 'Interest leaders *⁹',
      flows: 'Net flows *¹⁰',
      funding: 'Funding (avg) *¹¹',
      risks: 'Risk *¹²',
      plan: 'Action plan',
      over24h: 'over 24h',
      updatesNote: 'updates every 15 min'
    }
    : {
      report: 'ОТЧЕТ',
      asof: 'Данные на',
      price: 'Цена *¹',
      dom: 'Доминация BTC *²',
      fgi: 'Индекс страха и жадности *³',
      ls: 'Лонги vs Шорты *⁴',
      macro: 'Макроданные *⁵',
      volumes: 'Объём 24 ч *⁶',
      rsi: 'RSI (14) *⁷',
      oicvd: 'OI (открытый интерес) и CVD (кумулятивная дельта) *⁸',
      leaders: 'Лидеры интереса *⁹',
      flows: 'Притоки/оттоки *¹⁰',
      funding: 'Фандинг (ср.) *¹¹',
      risks: 'Риск *¹²',
      plan: 'План действий',
      over24h: 'за 24 часа',
      updatesNote: 'обновляются каждые 15 мин'
    };

  const when = formatKyiv(tsEpoch, tsIsoKyiv);
  const asOf = isEn ? when.en : when.ru;
  const tzSuffix = ' (Europe/Kyiv)';

  const scoreBTC = computeRiskV2(snapshots.BTC || {}, { ...extras, snapshots }, 'BTC');
  const scoreETH = computeRiskV2(snapshots.ETH || {}, { ...extras, snapshots }, 'ETH');

  const oiBTC = extras?.oiCvdBTC || null;
  const oiETH = extras?.oiCvdETH || null;

  const dxy = extras?.macro?.dxy || null;
  const m2  = extras?.macro?.m2  || null;

  const priceLine = (sym) => {
    const pct = Number(sym?.pct24);
    const circ = circleByDelta(pct);
    const pctTxt = Number.isFinite(pct)
      ? `${circ} (${B(`${pct > 0 ? '+' : ''}${pct.toFixed(2)}%`)} ${T.over24h})`
      : '(—)';
    const p = Number.isFinite(sym?.price)
      ? `$${isEn ? humanFmtEN(sym.price) : humanFmt(sym.price)}`
      : '—';
    return `${B(p)} ${pctTxt}`;
  };

  const fgiLine = (sym) => {
    const v = Number(sym?.fgiValue);
    if (!Number.isFinite(v)) return '—';
    const cls = fgiClassFromValue(v, isEn);
    const bar = fearGreedBarColorized(v);
    return `${B(String(v))}${cls ? ` (${B(cls)})` : ''}\n${bar}`;
  };

  const volumeLine = (sym) => {
    const vol = Number(sym?.vol24);
    const deltaPct = Number(sym?.volDeltaPct);
    const circ = circleByDelta(deltaPct);
    const abbrVal = Number.isFinite(vol) ? abbrevWithUnit(vol, isEn) : '';
    const abbr = abbrVal ? B(abbrVal) : '—';
    const pctTxt = Number.isFinite(deltaPct)
      ? `${circ}(${B(`${deltaPct > 0 ? '+' : ''}${deltaPct.toFixed(2)}%`)} ${T.over24h})`
      : '';
    return [abbr, pctTxt].filter(Boolean).join(' ');
  };

  const rsiLine = (sym) => {
    const now = Number(sym?.rsi14), prev = Number(sym?.rsi14Prev);
    if(!Number.isFinite(now)) return '—';
    const base = B(isEn?humanFmtEN(now):humanFmt(now));
    if(Number.isFinite(prev)){
      const d = now - prev;
      const circ = circleByDelta(d);
      const dTxt = `${circ}(${B(`${d > 0 ? '+' : ''}${d.toFixed(2)}`)} ${T.over24h})`;
      return `${base} ${dTxt}`;
    }
    return base;
  };

  const fundingLine = (sym) => {
    const now = Number(sym?.fundingNow);
    const prev = Number(sym?.fundingPrev);
    if(!Number.isFinite(now) || nearZero(now)) return '—';
    const base = B(fmtFunding(now));
    if(Number.isFinite(prev) && !nearZero(prev)){
      const d = now - prev;
      const circ = circleByDelta(d);
      const bps = d * 10000;
      const dTxt = `${circ}(${B(`${bps > 0 ? '+' : ''}${bps.toFixed(2)} ${isEn ? 'bps' : 'б.п.'}`)})`;
      return `${base} ${dTxt}`;
    }
    return base;
  };

  const head=[];
  head.push(`📊 ${BU(T.report)}`);
  head.push('');

  head.push(BU(T.price));
  if (snapshots.BTC) head.push(`• BTC: ${priceLine((snapshots.BTC)||{})}`);
  if (snapshots.ETH) head.push(`• ETH: ${priceLine((snapshots.ETH)||{})}`);
  if (snapshots.PAXG) head.push(`• PAXG: ${priceLine((snapshots.PAXG)||{})}`);
  head.push('');

  head.push(BU(T.dom));
  {
    const domPct = typeof extras?.btcDominancePct === 'number' ? extras.btcDominancePct : null;
    const domDelta = typeof extras?.btcDominanceDelta === 'number' ? extras.btcDominanceDelta : null;
    const domParts = [];
    if (Number.isFinite(domPct)) domParts.push(B(`${domPct.toFixed(2)}%`));
    if (Number.isFinite(domDelta)) {
      const circ = circleByDelta(domDelta);
      domParts.push(`${circ} (${B(`${domDelta > 0 ? '+' : ''}${domDelta.toFixed(2)}%`)} ${T.over24h})`);
    }
    head.push(`${domParts.length ? domParts.join(' ') : '—'}`);
  }
  head.push('');

  head.push(BU(T.fgi));
  head.push(`${fgiLine((snapshots.BTC)||{})}`);
  head.push('');

  head.push(BU(T.ls));
  if (snapshots.BTC) head.push(renderLsBlock(((snapshots.BTC)||{}).longShort, isEn, 'BTC'));
  if (snapshots.ETH) head.push(renderLsBlock(((snapshots.ETH)||{}).longShort, isEn, 'ETH'));
  head.push('');

  head.push(BU(T.macro));

  {
    const p = extras?.spx?.price ?? null;
    const pct = extras?.spx?.pct ?? null;
    const parts = [];
    if (Number.isFinite(p)) parts.push(B(isEn ? humanFmtEN(p) : humanFmt(p)));
    if (Number.isFinite(pct)) {
      const circ = circleByDelta(pct);
      parts.push(`${circ} (${B(`${pct > 0 ? '+' : ''}${pct.toFixed(2)}%`)} ${T.over24h})`);
    }
    head.push(`• S&P 500: ${parts.length ? parts.join(' ') : '—'}`);
  }

  {
    const price = dxy?.price ?? null;
    const pct = dxy?.pct ?? null;
    const parts = [];
    if (Number.isFinite(price)) {
      const pTxt = isEn ? humanFmtEN(price) : humanFmt(price);
      parts.push(B(pTxt));
    }
    if (Number.isFinite(pct)) {
      const circ = circleByDelta(pct);
      const pctTxt = `${pct > 0 ? '+' : ''}${pct.toFixed(2)}%`;
      parts.push(`${circ} (${B(pctTxt)} ${T.over24h})`);
    }
    head.push(`• DXY: ${parts.length ? parts.join(' ') : '—'}`);
  }

  {
    const now = m2?.now ?? null;
    const m2T = now / 1000;
    const pct = m2?.pct ?? null;
    const parts = [];
    if (Number.isFinite(now)) parts.push(B(`${m2T.toFixed(2)} ${isEn ? 'T' : 'трлн'}`));
    if (Number.isFinite(pct)) {
      const circ = circleByDelta(pct);
      parts.push(`${circ} (${B(`${pct > 0 ? '+' : ''}${pct.toFixed(2)}%`)} ${T.over24h})`);
    }
    head.push(`• M2: ${parts.length ? parts.join(' ') : '—'}`);
  }

  {
    const tot = extras?.totals || null;
    if (tot && Number.isFinite(tot.total)) {
      const row = (label, val, d) => {
        const base = B(abbrevWithUnit(val, isEn) || '—');
        const delta = Number.isFinite(d)
          ? ` ${circleByDelta(d)}(${B(`${d > 0 ? '+' : ''}${d.toFixed(2)}%`)} ${T.over24h})`
          : '';
        return `• ${label}: ${base}${delta}`;
      };
      head.push(row('TOTAL', tot.total, tot.d1));
      head.push(row('TOTAL2', tot.total2, tot.d2));
      head.push(row('TOTAL3', tot.total3, tot.d3));
    } else {
      head.push('• TOTAL: —');
    }
  }

  head.push('');

  head.push(BU(T.volumes));
  if (snapshots.BTC) head.push(`• BTC: ${volumeLine((snapshots.BTC)||{})}`);
  if (snapshots.ETH) head.push(`• ETH: ${volumeLine((snapshots.ETH)||{})}`);
  head.push('');

  head.push(BU(T.rsi));
  if (snapshots.BTC) head.push(`• BTC: ${rsiLine((snapshots.BTC)||{})}`);
  if (snapshots.ETH) head.push(`• ETH: ${rsiLine((snapshots.ETH)||{})}`);
  head.push('');

  head.push(BU(T.oicvd));
  if (oiBTC) head.push(oiCvdLine('• BTC', oiBTC, isEn, (snapshots?.BTC||{}).price));
  if (oiETH) head.push(oiCvdLine('• ETH', oiETH, isEn, (snapshots?.ETH||{}).price));
  head.push('');

  if (extras?.leadersTop && Array.isArray(extras.leadersTop.items) && extras.leadersTop.items.length){
    const expl = isEn ? '(asset: OI Δ (30m) | CVD (30m))' : '(монета: OI Δ (30м) | CVD (30м))';
    head.push(`${BU(T.leaders)} ${expl}`);

    const deriveEmoji = (oiPct, cvdUsd) => {
      const oi = Number(oiPct), usd = Number(cvdUsd);
      if (Number.isFinite(oi) && Number.isFinite(usd)) {
        if (oi > 0 && usd > 0) return '🟢';
        if (oi < 0 && usd > 0) return '🟡';
        if (oi > 0 && usd < 0) return '🟠';
      }
      return '⚪️';
    };

    for (const it of extras.leadersTop.items.slice(0,5)) {
      let emoji = String(it?.verdictEmoji || '');
      if (!emoji) emoji = deriveEmoji(it?.oiPct, it?.cvdUsd);

      const symTxt = String(it?.sym ?? '');
      const sOi = Number.isFinite(it?.oiPct) ? `${it.oiPct > 0 ? '+' : ''}${Number(it.oiPct).toFixed(2)}%` : '—';

      let sCvd = '—';
      if (Number.isFinite(it?.cvdUsd)) {
        const abs = Math.abs(Number(it.cvdUsd));
        sCvd = `${Number(it.cvdUsd) >= 0 ? '+' : '−'}$${abbrevWithUnit(abs, true)}`;
      }

      const comboBold = B(`${sOi} | ${sCvd}`);
      const label = esc(verdictLabelFromEmoji(emoji, isEn));

      head.push(`• ${symTxt}: ${comboBold} — ${emoji} ${label}`);
    }
    head.push('');
  }

  try {
    const porBlock = await buildPorNetflowsBlock(lang, {
      btcPrice: snapshots.BTC?.price ?? null,
      ethPrice: snapshots.ETH?.price ?? null,
      cryptoquant: extras?.cryptoquant || null
    });

    head.push(BU(T.flows));

    if (porBlock && typeof porBlock === 'string' && porBlock.trim()) {
      head.push(porBlock.trim());
    } else {
      head.push(isEn
        ? '• BTC / ETH: no flow data yet'
        : '• BTC / ETH: данных по потокам пока нет'
      );
    }
    head.push('');
  } catch (err) {
    head.push(BU(T.flows));
    head.push(isEn
      ? '• BTC / ETH: error loading flows'
      : '• BTC / ETH: ошибка загрузки потоков'
    );
    head.push('');
  }

  head.push(BU(T.funding));
  if (snapshots.BTC) head.push(`• BTC: ${fundingLine((snapshots.BTC)||{})}`);
  if (snapshots.ETH) head.push(`• ETH: ${fundingLine((snapshots.ETH)||{})}`);
  head.push('');
  const rBbar = `${riskBar(scoreBTC)} ${B(`${Math.round(scoreBTC*100)}%`)}`;
  const rEbar = `${riskBar(scoreETH)} ${B(`${Math.round(scoreETH*100)}%`)}`;
  head.push(BU(T.risks));
  if (snapshots.BTC) head.push(`• BTC:\n${rBbar}`);
  if (snapshots.ETH) head.push(`• ETH:\n${rEbar}`);
  head.push('');

  const help=[];
  help.push(BU(isEn?'Guide':'📘 Справка'));
  help.push('');

  help.push(
    isEn
      ? `${B('¹ Price (spot)')}\n— Current price and 24h change.\n• BTC/ETH: wait for confirmation of signals; avoid emotional risk increases.\n• PAXG: defensive instrument; use according to plan.`
      : `${B('¹ Цена (spot)')}\n— Текущая цена и изменение за 24 часа.\n• BTC/ETH: ждать подтверждений сигналов, не повышать риск на эмоциях.\n• PAXG: защитный инструмент; используется по плану.`
  );
  help.push('');

  help.push(
    isEn
      ? `${B('² BTC Dominance')}\n— BTC share of total crypto market capitalization.\nRise → rotation into BTC, pressure on alts.\nFall → interest in alts, expansion of demand.`
      : `${B('² Доминация BTC')}\n— Доля BTC в общей капитализации крипторынка.\nРост → ротация в BTC, давление на альты.\nПадение → интерес к альтам, расширение спроса.`
  );
  help.push('');

  help.push(
    isEn
      ? `${B('³ Fear & Greed (FGI)')}\n— Composite BTC sentiment indicator.\nExtreme fear → panic, potential for rebound.\nExtreme greed → crowd is overheated, higher reversal risk.`
      : `${B('³ Индекс страха и жадности (FGI)')}\n— Сводный индикатор настроений по BTC.\nЭкстр. страх → паника, потенциал для восстановления.\nЭкстр. жадность → толпа перегрета, повышен риск разворота.`
  );
  help.push('');

  help.push(
    isEn
      ? `${B('⁴ Longs vs Shorts (L/S)')}\n— Shows positioning imbalance.\nStrong imbalance (>60/40) → elevated squeeze risk.\nDo not enter the overloaded side without confirmations.`
      : `${B('⁴ Лонги vs Шорты (L/S)')}\n— Показывает перекос в позициях.\nСильный перекос (>60/40) → повышенный риск шорт-/лонг-сквиза.\nНе входить в сторону перегруженной стороны без подтверждений.`
  );
  help.push('');

  help.push(
    isEn
      ? `${B('⁵ Macro Data')}\n— Broad risk context that affects crypto assets.\n\n• S&P 500 — risk-on / risk-off environment. Rises support crypto.\n• DXY — dollar index: when the dollar rises, investors move out of risk assets, which puts pressure on crypto.\n• M2 — money supply: growth = more liquidity.\n• TOTAL — market breadth; growth = capital inflow into crypto.\n• TOTAL2 (ex-BTC) — alt breadth.\n• TOTAL3 (ex-BTC & ETH) — high-beta, high-risk alts.`
      : `${B('⁵ Макроданные')}\n— Общий фон риска, влияющий на криптоактивы.\n\n• S&P 500 — риск-он/риск-офф среда. Рост поддерживает крипту.\n• DXY — индекс доллара: когда доллар растёт, инвесторы уходят из рискованных активов, поэтому на крипту появляется давление.\n• M2 — денежная масса: рост = больше ликвидности.\n• TOTAL — широта рынка; рост = приток капитала в крипту.\n• TOTAL2 (без BTC) — ширина альтов.\n• TOTAL3 (без BTC и ETH) — высокорискованные альты (высокая бета).`
  );
  help.push('');

  help.push(
    isEn
      ? `${B('⁶ 24h Volume')}\n— Confirms strength of move.\nPrice rise without volume — weak move.\nDecline on low volume — weak selling pressure.`
      : `${B('⁶ Объём 24ч')}\n— Подтверждение силы движения.\nРост цены без объёма — слабое движение.\nПадение на низких объёмах — слабое давление продавцов.`
  );
  help.push('');

  help.push(
    isEn
      ? `${B('⁷ RSI (14)')}\n— Momentum indicator.\n≈70 — overbought, elevated pullback risk.\n≈30 — oversold, possible reversal.\nAlways interpret in the trend context.`
      : `${B('⁷ RSI (14)')}\n— Индикатор импульса.\n≈70 — перегрев, повышен риск отката.\n≈30 — перепроданность, возможен разворот.\nТрактовать всегда только в контексте тренда.`
  );
  help.push('');

  help.push(
    isEn
      ? `${B('⁸ OI & CVD')}\n— OI: change in open interest (position size).\n— CVD: who is aggressive — market buyers or market sellers.\n\nLegend:\n• 🟢 Longs inflow — trend-long on pullbacks; don’t chase.\n• 🟡 Short-cover — risky to short in chase; longs on confirmation.\n• 🟠 Absorption — breakout-longs are risky; fade at resistances.\n• ⚪️ Cooling — base size, wait for signals.`
      : `${B('⁸ OI и CVD')}\n— OI: изменение открытого интереса (размер позиций).\n— CVD: кто агрессор — маркет-покупатели или маркет-продавцы.\n\nОбозначения:\n• 🟢 Приток лонгов — тренд-лонг по откату; не гнаться.\n• 🟡 Short-cover — опасно шортить в догонку; лонг по подтверждению.\n• 🟠 Впитывание — пробойные лонги рискованны; работать от сопротивлений.\n• ⚪️ Охлаждение — базовый размер, ждать сигнала.`
  );
  help.push('');

  help.push(
    isEn
      ? `${B('⁹ Leaders of interest')}\n— Composite ranking by OIΔ% and CVD$; highlights tickers where position build-up aligns with aggressive flow. Sign and relative magnitude matter more than absolute sizes.`
      : `${B('⁹ Лидеры интереса')}\n— Композитный рейтинг по OIΔ% и CVD$:\nпоказывает монеты, где одновременно есть набор позиций и активный агрессивный поток.\nВажны знак и относительная величина, а не абсолютные цифры.`
  );
  help.push('');

  help.push(
    isEn
      ? `${B('¹⁰ Net flows (CEX flows)')}\n— Aggregated BTC/ETH inflows/outflows across exchanges over 24h.\nInflow → potential sell pressure.\nOutflow → coins moving to custody, supports price.`
      : `${B('¹⁰ Притоки / Оттоки (CEX flows)')}\n— Совокупные притоки/оттоки BTC/ETH на биржи за 24ч.\nПриток → потенциальное давление продаж.\nОтток → монеты уходят на хранение, поддерживает цену.`
  );
  help.push('');

  help.push(
    isEn
      ? `${B('¹¹ Funding (avg)')}\n— Average perp funding.\nHigh positive → long crowd overheated.\nHigh negative → shorts overheated.\nIf |funding| > 0.03% — trim risk.`
      : `${B('¹¹ Фандинг (ср.)')}\n— Средний funding на перпетуальных фьючерсах.\nВысокий положительный → рынок перегрет long-ами.\nВысокий отрицательный → перегруз по шортам.\nЕсли |funding| высок >0.03% — снижать риск.`
  );
  help.push('');

  help.push(
    isEn
      ? `${B('¹² Risk (aggregator)')}\n— Combined indicator based on price change, funding, L/S, OI/CVD, FGI and market breadth.\nHigh risk → reduce size, avoid adding leverage, take partial profits.\nLow risk → trade setups, entries by signals, cautiously increase.`
      : `${B('¹² Риск (агрегатор)')}\n— Сводный показатель на основе цены, funding, L/S, OI/CVD, FGI и широты рынка.\nВысокий риск → уменьшать размер, не поднимать плечо, фиксировать частично.\nНизкий → работать по сетапам, входы по сигналам, аккуратно увеличивать.`
  );

  const plan=[];
  plan.push(BU(T.plan));
  plan.push('');
  const planLines = (label, score, oi, snap) => {
    const pct = Math.round(score*100);
    const regime =
      pct >= 60 ? (isEn?'Reduce exposure':'Снижать экспозицию') :
        pct >= 30 ? (isEn?'Cut leverage':'Резать плечо') :
          pct >= 10 ? (isEn?'Confirmations only':'Только по подтверждению') :
            (isEn?'Hold / gentle DCA':'Держать / мягкий DCA');
    const oiTxt = oi ? `${oi.verdictEmoji||'⚪️'} ${verdictLabelFromEmoji(oi.verdictEmoji, isEn)}` : (isEn?'—':'—');
    const fundNow = Number(snap?.fundingNow);
    const fundNote = Number.isFinite(fundNow)
      ? (Math.abs(fundNow)>0.0003 ? (isEn?'elevated funding — trim risk':'повышенный funding — риск поджать')
        : (isEn?'moderate funding':'умеренный funding'))
      : (isEn?'no funding':'нет funding');
    return [
      `${label} — ${B(regime)}; ${B(`${pct}%`)}`,
      `${isEn?'OI/CVD':'OI/CVD'}: ${oiTxt}; ${isEn?'funding':'фандинг'}: ${fundNote}.`,
      `${isEn?'Entries on pullbacks / signals; partial TP by rules':'Входы на откатах/по сетапам; частичная фиксация по правилам'}.`
    ];
  };
  if (snapshots.BTC) plan.push(...planLines('BTC', scoreBTC, oiBTC, snapshots.BTC));
  if (snapshots.ETH) plan.push(...planLines('ETH', scoreETH, oiETH, snapshots.ETH));

  const footerHtml = `\n📊 ${T.asof}: ${B(`${asOf}${tzSuffix}`)} — ${T.updatesNote}`;

  const headHtml = head.join('\n');
  const helpHtml = help.join('\n');
  const planHtml = plan.join('\n');
  const fullHtml = headHtml + '\n' + planHtml + '\n' + helpHtml + '\n' + footerHtml;
  return { headHtml, helpHtml, fullHtml, footerHtml };
}



function buildShortReportParts(snapshots, lang='ru', tsIsoKyiv='', tsEpoch=null, extras={}){
  const isEn = String(lang).toLowerCase().startsWith('en');
  const T = isEn ? {
    short:'SHORT REPORT',
    market:'Market',
    btc:'BTC',
    eth:'ETH',
    gold:'Gold',
    total:'Total',
    rsi:'RSI (BTC)',
    dom:'BTC.D',
    ratio:'BTC/ETH',
    asof:'As of',
    updatesNote:'updates every 15 min'
  } : {
    short:'КРАТКИЙ ОТЧЕТ',
    market:'Рынок',
    btc:'BTC',
    eth:'ETH',
    gold:'Золото',
    total:'Total',
    rsi:'RSI (BTC)',
    dom:'BTC.D',
    ratio:'BTC/ETH',
    asof:'Данные на',
    updatesNote:'обновляются каждые 15 мин'
  };

  const btc = snapshots.BTC || {};
  const eth = snapshots.ETH || {};
  const paxg = snapshots.PAXG || {};

  const ratioNow = (Number.isFinite(btc.price) && Number.isFinite(eth.price) && eth.price!==0) ? (btc.price/eth.price) : null;
  const ratioDelta = (Number.isFinite(btc.pct24) && Number.isFinite(eth.pct24)) ? (((1+btc.pct24/100)/(1+eth.pct24/100))-1)*100 : null;

  const domPct = Number.isFinite(extras?.btcDominancePct) ? Number(extras.btcDominancePct) : null;
  const domDelta = Number.isFinite(extras?.btcDominanceDelta) ? Number(extras.btcDominanceDelta) : null;

  const fgiNow = Number.isFinite(extras?.fgiNow) ? Number(extras.fgiNow) : (Number.isFinite(btc.fgiValue) ? Number(btc.fgiValue) : null);
  const fgiDeltaAbs = Number.isFinite(extras?.fgiDelta) ? Number(extras.fgiDelta) : null;
  const fgiPrev = (Number.isFinite(fgiNow) && Number.isFinite(fgiDeltaAbs)) ? (fgiNow - fgiDeltaAbs) : null;
  const fgiDeltaPct = (Number.isFinite(fgiDeltaAbs) && Number.isFinite(fgiPrev) && fgiPrev !== 0)
    ? (fgiDeltaAbs / Math.abs(fgiPrev)) * 100
    : null;

  const goldPct = Number.isFinite(paxg.pct24) ? Number(paxg.pct24) : null;
  const goldPrice = Number.isFinite(paxg.price) ? (isEn?`$${humanFmtEN(paxg.price)}`:`$${humanFmt(paxg.price)}`) : null;

  const rsiNow = Number.isFinite(btc.rsi14) ? Number(btc.rsi14) : null;
  const rsiPrev = Number.isFinite(btc.rsi14Prev) ? Number(btc.rsi14Prev) : null;
  const rsiDelta = (Number.isFinite(rsiNow) && Number.isFinite(rsiPrev)) ? (rsiNow - rsiPrev) : null;

  const mcap = extras?.totals || null;
  const mcapNow = Number.isFinite(mcap?.total) ? Number(mcap.total) : null;
  const mcapPct = Number.isFinite(mcap?.d1) ? Number(mcap.d1) : null;

  const arrow = (v) => Number.isFinite(v) ? (v>0 ? '↗' : (v<0 ? '↘' : '→')) : '→';
  const pctFmt = (v) => Number.isFinite(v) ? `${v>0?'+':''}${v.toFixed(2)}%` : '—';
  const priceFmt = (v) => Number.isFinite(v) ? (isEn?`$${humanFmtEN(v)}`:`$${humanFmt(v)}`) : '—';
  const capFmtTight = (v) => {
    if (!Number.isFinite(v)) return '—';
    return (isEn ? abbrevWithUnit(v, true) : abbrevWithUnit(v, false)).replace(/ (?=[A-Za-zА-Яа-яЁё.]+$)/, '');
  };
  const ratioFmt = (v) => Number.isFinite(v) ? v.toFixed(4) : '—';
  const circ = (v) => circleByDelta(Number(v));

  const when = formatKyiv(tsEpoch, tsIsoKyiv);
  const asOf = isEn ? when.en : when.ru;
  const tzSuffix = ' (Europe/Kyiv)';
  const footerHtml = `\n📊 ${T.asof}: ${B(`${asOf}${tzSuffix}`)} — ${T.updatesNote}`;

  const lines = [];
  lines.push(`📌 ${BU(T.short)}`);
  lines.push('');

  const fgiLabelTxt = Number.isFinite(fgiNow)
    ? (isEn ? `${fgiNow} - ${fgiClassFromValue(fgiNow,true)}` : `${fgiNow} - ${fgiClassFromValue(fgiNow,false)}`)
    : '—';
  lines.push(`${circ(fgiDeltaPct)} ${T.market}: ${arrow(fgiDeltaPct)} ${pctFmt(fgiDeltaPct)} (${B(fgiLabelTxt)})`);

  lines.push(`${circ(btc.pct24)} ${T.btc}: ${arrow(btc.pct24)} ${pctFmt(btc.pct24)} (${B(priceFmt(btc.price))})`);
  lines.push(`${circ(eth.pct24)} ${T.eth}: ${arrow(eth.pct24)} ${pctFmt(eth.pct24)} (${B(priceFmt(eth.price))})`);
  lines.push(`${circ(goldPct)} ${T.gold}: ${arrow(goldPct)} ${pctFmt(goldPct)} (${B(goldPrice||'—')})`);

  lines.push(`${circ(mcapPct)} ${T.total}: ${arrow(mcapPct)} ${pctFmt(mcapPct)} (${B(capFmtTight(mcapNow))})`);

  const rsiDeltaTxt = Number.isFinite(rsiDelta) ? (rsiDelta>0?`+${rsiDelta.toFixed(2)}`:rsiDelta.toFixed(2)) : '—';
  const rsiValTxt = Number.isFinite(rsiNow) ? B(rsiNow.toFixed(2)) : '—';
  lines.push(`${circ(rsiDelta)} ${T.rsi}: ${arrow(rsiDelta)} ${rsiDeltaTxt} (${rsiValTxt})`);

  const domPctTxt = Number.isFinite(domPct) ? `${domPct.toFixed(2)}%` : '—';
  lines.push(`${circ(domDelta)} ${T.dom}: ${arrow(domDelta)} ${pctFmt(domDelta)} (${B(domPctTxt)})`);

  lines.push(`${circ(ratioDelta)} ${T.ratio}: ${arrow(ratioDelta)} ${pctFmt(ratioDelta)} (${B(ratioFmt(ratioNow))})`);

  return { shortHtml: lines.join('\n'), footerHtml };
}

export async function buildMorningReportHtml(snapshots, lang='ru', tsIsoKyiv='', tsEpoch=null, extras={}){
  const { fullHtml } = await buildMorningReportParts(snapshots, lang, tsIsoKyiv, tsEpoch, extras);
  return fullHtml;
}

export async function broadcastMarketSnapshot(bot, { batchSize=MARKET_BATCH_SIZE || 25, pauseMs=MARKET_BATCH_PAUSE_MS || 400 } = {}){
  if (!usersCollection) return { ok:false, reason:'mongo_not_connected' };

  const recipients = await usersCollection.find(
    { botBlocked: { $ne: true }, sendMarketReport: { $ne: false } },
    { projection: { userId: 1, lang: 1 } }
  ).toArray();

  if (!recipients.length) return { ok:true, delivered:0, users:0, batchSize, pauseMs };

  const snap = await getMarketSnapshot(['BTC','ETH','PAXG']).catch(()=>null);
  if (!snap?.ok) return { ok:false, reason:'snapshot_failed', delivered:0, users:recipients.length };

  const { snapshots, atIsoKyiv, fetchedAt, btcDominancePct, btcDominanceDelta, spx, totals, fgiNow, fgiDelta, oiCvdBTC, oiCvdETH, leadersTop, cryptoquant } = snap;

  let delivered = 0;
  for (let i = 0; i < recipients.length; i += batchSize) {
    const chunk = recipients.slice(i, i + batchSize);
    await Promise.all(chunk.map(async (u) => {
      try {
        const lang = await resolveUserLang(u.userId).catch(() => u.lang || 'ru');
        const parts = await buildMorningReportParts(
          snapshots,
          lang,
          atIsoKyiv,
          fetchedAt,
          {
            btcDominancePct,
            btcDominanceDelta,
            spx,
            totals,
            fgiNow,
            fgiDelta,
            oiCvdBTC,
            oiCvdETH,
            leadersTop,
            cryptoquant,
            macro: snap.macro || null,
          }
        );
        const isEn = String(lang).toLowerCase().startsWith('en');
        const kb = { inline_keyboard: [[
            { text: isEn ? 'AI recommendations' : 'Рекомендации ИИ', callback_data: 'market_ai' },
            { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
          ]] };
        await bot.telegram.sendMessage(u.userId, parts.headHtml + '\n' + parts.footerHtml, { parse_mode:'HTML', reply_markup: kb });
        delivered++;
      } catch (err) {
        const code = err?.response?.error_code;
        const description = err?.response?.description || String(err?.message || err);
        if (code === 403 || /bot was blocked/i.test(description)) {
          try {
            await usersCollection.updateOne(
              { userId: u.userId },
              { $set: { botBlocked: true, botBlockedAt: new Date() } },
              { upsert: true }
            );
          } catch {}
        }
      }
    }));
    if (i + batchSize < recipients.length) {
      await new Promise(r => setTimeout(r, pauseMs));
    }
  }

  return { ok:true, delivered, users: recipients.length, batchSize, pauseMs };
}

export async function sendMarketReportToUser(bot, userId){
  const snap=await getMarketSnapshot(['BTC','ETH','PAXG']);
  if(!snap?.ok) return { ok:false };
  const lang=await resolveUserLang(userId).catch(()=> 'ru');
  const parts = await buildMorningReportParts(
    snap.snapshots,
    lang,
    snap.atIsoKyiv || '',
    snap.fetchedAt ?? null,
    {
      btcDominancePct: snap.btcDominancePct,
      btcDominanceDelta: snap.btcDominanceDelta,
      spx: snap.spx,
      totals: snap.totals,
      fgiNow: snap.fgiNow,
      fgiDelta: snap.fgiDelta,
      oiCvdBTC: snap.oiCvdBTC,
      oiCvdETH: snap.oiCvdETH,
      leadersTop: snap.leadersTop,
      cryptoquant: snap.cryptoquant,
      macro: snap.macro || null,
    }
  );
  const isEn = String(lang).toLowerCase().startsWith('en');
  const kb = { inline_keyboard: [[
      { text: isEn ? 'AI recommendations' : 'Рекомендации ИИ', callback_data: 'market_ai' },
      { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
    ]] };
  await bot.telegram.sendMessage(userId, parts.headHtml + '\n' + parts.footerHtml, { parse_mode:'HTML', reply_markup: kb });
  return { ok:true };
}

export async function sendShortReportToUser(bot, userId){
  const snap=await getMarketSnapshot(['BTC','ETH','PAXG']);
  if(!snap?.ok) return { ok:false };
  const lang=await resolveUserLang(userId).catch(()=> 'ru');
  const { shortHtml, footerHtml } = buildShortReportParts(
    snap.snapshots,
    lang,
    snap.atIsoKyiv || '',
    snap.fetchedAt ?? null,
    { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, totals: snap.totals, fgiNow: snap.fgiNow, fgiDelta: snap.fgiDelta }
  );
  const isEn = String(lang).toLowerCase().startsWith('en');
  const kb = { inline_keyboard: [[
      { text: isEn ? 'AI recommendations' : 'Рекомендации ИИ', callback_data: 'market_ai' },
      { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
    ]] };
  await bot.telegram.sendMessage(userId, shortHtml + '\n' + footerHtml, { parse_mode:'HTML', reply_markup: kb });
  return { ok:true };
}

export async function editReportMessageWithHelp(ctx){
  try {
    const userId = ctx.from?.id;
    const lang = await resolveUserLang(userId).catch(()=> 'ru');
    const isEn = String(lang).toLowerCase().startsWith('en');
    const okText = isEn ? 'Done.' : 'Готово.';
    const snap=await getMarketSnapshot(['BTC','ETH','PAXG']);
    if(!snap?.ok) {
      await ctx.answerCbQuery(isEn?'Error':'Ошибка');
      return;
    }
    const parts = await buildMorningReportParts(
      snap.snapshots,
      lang,
      snap.atIsoKyiv || '',
      snap.fetchedAt ?? null,
      {
        btcDominancePct: snap.btcDominancePct,
        btcDominanceDelta: snap.btcDominanceDelta,
        spx: snap.spx,
        totals: snap.totals,
        fgiNow: snap.fgiNow,
        fgiDelta: snap.fgiDelta,
        oiCvdBTC: snap.oiCvdBTC,
        oiCvdETH: snap.oiCvdETH,
        leadersTop: snap.leadersTop,
        cryptoquant: snap.cryptoquant,
        macro: snap.macro || null,
      }
    );
    const kb = { inline_keyboard: [[
        { text: isEn ? 'AI recommendations' : 'Рекомендации ИИ', callback_data: 'market_ai' },
        { text: isEn ? 'Full report'  : 'Полный отчёт',   callback_data: 'market_full'  }
      ]] };
    await ctx.reply(parts.helpHtml + '\n' + parts.footerHtml, { parse_mode:'HTML', reply_markup: kb });
    await ctx.answerCbQuery(okText);
  } catch {
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
}

async function findPreviousSnapshotWithGemini(db, excludeTs) {
  return await db.collection('market_snapshots')
    .find({ ts: { $lt: excludeTs }, gemini: { $exists: true, $ne: null } })
    .sort({ ts: -1 })
    .limit(20)
    .toArray();
}
