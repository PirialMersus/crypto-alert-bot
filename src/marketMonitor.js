// src/marketMonitor.js
import { resolveUserLang } from './cache.js';
import { usersCollection, client } from './db.js';
import { MARKET_BATCH_SIZE, MARKET_BATCH_PAUSE_MS } from './constants.js';

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
function fmtFunding(v) { if(!Number.isFinite(v)) return '—'; return Number(v).toFixed(8).replace(/\.0+$|0+$/,''); }
function circleByDelta(x) { if(!Number.isFinite(x) || x===0) return '⚪'; return x>0?'🟢':'🔴'; }

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
  for(const s of symbols){ if (snapshots?.[s]) out[s]=snapshots[s]; }
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
      if (dist < bestDist) { best = d; bestDist = dist; }
    }
  }
  if (best) return best;
  const cur2 = db.collection(collection).find({}, { projection: proj }).sort({ at: -1 }).limit(500);
  best=null; bestDist=Infinity;
  while (await cur2.hasNext()) {
    const d = await cur2.next();
    if (hasValue(d)) {
      const dist = Math.abs(Number(d.at) - target);
      if (dist < bestDist) { best = d; bestDist = dist; }
    }
  }
  return best;
}

async function findLatestDocWith(db, collection, hasValue){
  const cur = db.collection(collection).find({}, { projection: { at:1, snapshots:1, btcDominancePct:1, spx:1, totals:1, oiCvd:1 } }).sort({ at: -1 }).limit(500);
  while (await cur.hasNext()) { const d = await cur.next(); if (hasValue(d)) return d; }
  return null;
}

export async function getMarketSnapshot(symbols=['BTC','ETH','PAXG']){
  const dbName = process.env.DB_NAME || 'crypto_alert_dev';
  const collection = process.env.COLLLECTION || process.env.COLLECTION || 'marketSnapshots';
  const db = client.db(dbName);

  const cacheKey = symbols.slice().sort().join(',');
  const hit = _snapCache.get(cacheKey);
  const now = Date.now();
  if (hit && now - hit.ts < SNAPSHOT_CACHE_MS) return hit.data;

  const freshest = await db.collection(collection).find({}, { projection: { snapshots:1, at:1, atIsoKyiv:1, btcDominancePct:1, spx:1, totals:1, oiCvd:1, capTop:1 } }).sort({ at: -1 }).limit(1).next();
  if (process.env.DEBUG_OICVD === '1') {
    const atLabel = freshest?.atIsoKyiv || new Date(freshest?.at || Date.now()).toISOString();
    console.log('[OI/CVD DEBUG] at:', atLabel);
  }
  if (!freshest || !freshest.snapshots) {
    const miss = { ok:false, reason:'no_snapshot' };
    _snapCache.set(cacheKey, { ts: now, data: miss });
    return miss;
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
    leadersTop
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

function buildMorningReportParts(snapshots, lang='ru', tsIsoKyiv='', tsEpoch=null, extras={}){
  const isEn=String(lang).toLowerCase().startsWith('en');
  const T=isEn?{
    report:'REPORT',
    asof:'As of',
    price:'Цена *¹',
    dom:'BTC Dominance *²',
    fgi:'Fear & Greed *³',
    ls:'Longs vs Shorts *⁴',
    spx:'S&P 500 *⁵',
    totals:'Market capitalization *⁶',
    volumes:'24h Volume *⁷',
    rsi:'RSI (14) *⁸',
    oicvd:'OI (open interest) and CVD (cumulative delta volume) *⁹',
    leaders:'Interest leaders *¹⁰',
    flows:'Net flows *¹¹',
    funding:'Funding (avg) *¹²',
    risks:'Risk *¹³',
    plan:'Action plan',
    over24h:'over 24h',
    updatesNote:'updates every 30 min'
  }:{
    report:'ОТЧЕТ',
    asof:'Данные на',
    price:'Цена *¹',
    dom:'Доминация BTC *²',
    fgi:'Индекс страха и жадности *³',
    ls:'Лонги vs Шорты *⁴',
    spx:'S&P 500 *⁵',
    totals:'Рыночная капитализация *⁶',
    volumes:'Объем 24 ч *⁷',
    rsi:'RSI (14) *⁸',
    oicvd:'OI (открытый интерес) и CVD (кумулятивная дельта объёма) *⁹',
    leaders:'Лидеры интереса *¹⁰',
    flows:'Притоки/оттоки *¹¹',
    funding:'Фандинг (ср.) *¹²',
    risks:'Риск *¹³',
    plan:'План действий',
    over24h:'за 24 часа',
    updatesNote:'обновляются каждые 30 мин'
  };

  const when = formatKyiv(tsEpoch, tsIsoKyiv);
  const asOf = isEn ? when.en : when.ru;
  const tzSuffix = ' (Europe/Kyiv)';

  const scoreBTC = computeRiskV2((snapshots.BTC||{}), { ...extras, snapshots }, 'BTC');
  const scoreETH = computeRiskV2((snapshots.ETH||{}), { ...extras, snapshots }, 'ETH');
  const oiBTC = extras?.oiCvdBTC || null;
  const oiETH = extras?.oiCvdETH || null;

  const priceLine = (sym) => {
    const pct = Number(sym?.pct24);
    const circ = circleByDelta(pct);
    const pctTxt = Number.isFinite(pct) ? `${circ} (${B(`${pct>0?'+':''}${pct.toFixed(2)}%`)} ${T.over24h})` : '(—)';
    const p = Number.isFinite(sym?.price) ? `$${isEn?humanFmtEN(sym.price):humanFmt(sym.price)}` : '—';
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
    const pctTxt = Number.isFinite(deltaPct) ? `${circ}(${B(`${deltaPct>0?'+':''}${deltaPct.toFixed(2)}%`)} ${T.over24h})` : '';
    return [abbr, pctTxt].filter(Boolean).join(' ');
  };
  const rsiLine = (sym) => {
    const now = Number(sym?.rsi14), prev = Number(sym?.rsi14Prev);
    if(!Number.isFinite(now)) return '—';
    const base = B(isEn?humanFmtEN(now):humanFmt(now));
    if(Number.isFinite(prev)){
      const d = now - prev;
      const circ = circleByDelta(d);
      const dTxt = `${circ}(${B(`${d>0?'+':''}${d.toFixed(2)}`)} ${T.over24h})`;
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
      const dTxt = `${circ}(${B(`${(bps>0?'+':'')}${(bps).toFixed(2)} ${isEn?'bps':'б.п.'}`)})`;
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
      domParts.push(`${circ} (${B(`${domDelta>0?'+':''}${domDelta.toFixed(2)}%`)} ${T.over24h})`);
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
  head.push(BU(T.spx));
  {
    const spxPrice = (extras?.spx && typeof extras.spx.price === 'number') ? extras.spx.price : null;
    const spxPct   = (extras?.spx && typeof extras.spx.pct   === 'number') ? extras.spx.pct   : null;
    const spxParts = [];
    if (Number.isFinite(spxPrice)) spxParts.push(B(isEn?humanFmtEN(spxPrice):humanFmt(spxPrice)));
    if (Number.isFinite(spxPct)) {
      const spxCirc = circleByDelta(spxPct);
      spxParts.push(`${spxCirc} (${B(`${spxPct>0?'+':''}${spxPct.toFixed(2)}%`)} ${T.over24h})`);
    }
    head.push(`${spxParts.length ? spxParts.join(' ') : '—'}`);
  }
  head.push('');
  head.push(BU(T.totals));
  {
    const tot = extras?.totals || null;
    if (tot && Number.isFinite(tot.total)) {
      const t1 = `${B((isEn?abbrevWithUnit(tot.total,true):abbrevWithUnit(tot.total,false)) || '—')}${Number.isFinite(tot.d1) ? ` ${circleByDelta(tot.d1)}(${B(`${tot.d1>0?'+':''}${tot.d1.toFixed(2)}%`)} ${T.over24h})` : ''}`;
      const t2 = `${B((isEn?abbrevWithUnit(tot.total2,true):abbrevWithUnit(tot.total2,false)) || '—')}${Number.isFinite(tot.d2) ? ` ${circleByDelta(tot.d2)}(${B(`${tot.d2>0?'+':''}${tot.d2.toFixed(2)}%`)} ${T.over24h})` : ''}`;
      const t3 = `${B((isEn?abbrevWithUnit(tot.total3,true):abbrevWithUnit(tot.total3,false)) || '—')}${Number.isFinite(tot.d3) ? ` ${circleByDelta(tot.d3)}(${B(`${tot.d3>0?'+':''}${tot.d3.toFixed(2)}%`)} ${T.over24h})` : ''}`;
      head.push(`• TOTAL: ${t1}`);
      head.push(`• TOTAL2: ${t2}`);
      head.push(`• TOTAL3: ${t3}`);
    } else {
      head.push('• —');
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

  head.push(BU(T.flows));
  if (snapshots.BTC) head.push(`• BTC: ${flowsHeaderLine((snapshots.BTC)||{}, isEn)}`);
  if (snapshots.ETH) head.push(`• ETH: ${flowsHeaderLine((snapshots.ETH)||{}, isEn)}`);
  head.push('');
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
  help.push(BU(isEn?'Guide':'Справка'));
  help.push('');

  help.push(`${B(isEn?'¹ Price: spot.':'¹ Цена: спот.')} ${isEn?'— snapshot of current price and 24h change. PAXG ≈ tokenized gold.':'— фиксация текущей цены и изменения за 24ч. PAXG — токенизированное золото.'}`);
  if (snapshots.BTC) help.push(`• ${B('BTC')}: ${isEn?'Wait for confirmations; do not raise risk.':'Ждать подтверждений; риск не повышать.'}`);
  if (snapshots.ETH) help.push(`• ${B('ETH')}: ${isEn?'Wait for confirmations; do not raise risk.':'Ждать подтверждений; риск не повышать.'}`);
  if (snapshots.PAXG) help.push(`• ${B('PAXG')}: ${isEn?'Safe-haven proxy; position by plan.':'Уклон в защиту; позиционирование по плану.'}`);

  help.push('');

  help.push(`${B(isEn?'² BTC Dominance':'² Доминация BTC')} ${isEn?'— BTC share of total crypto market cap. Rising → rotation to BTC; falling → alts interest.':'— доля BTC в общей капитализации. Рост → ротация в BTC; падение → интерес к альтам.'}`);

  help.push('');

  help.push(`${B(isEn?'³ Fear & Greed':'³ Индекс страха и жадности')} ${isEn?'— composite BTC sentiment.':'— сводный индикатор настроений по BTC.'}`);

  help.push('');

  help.push(`${B(isEn?'⁴ Longs vs Shorts':'⁴ Лонги vs Шорты')} ${isEn?'— positioning split.':'— распределение позиций.'}`);

  help.push('');

  help.push(`${B('⁵ S&P 500')} ${isEn?'— broad risk barometer.':'— индикатор общего риска.'}`);

  help.push('');

  help.push(`${B(isEn?'⁶ Market cap':'⁶ Рыночная капитализация')} ${isEn?'— breadth of the crypto market.':'— ширина/масштаб крипторынка.'}`);
  help.push(`• ${B('TOTAL')}: ${isEn?'Total crypto market cap.':'Вся капитализация крипторынка.'}`);
  help.push(`• ${B(isEn?'TOTAL2 (ex-BTC)':'TOTAL2 (без BTC)')}: ${isEn?'Alt breadth without BTC.':'Широта альтов без BTC.'}`);
  help.push(`• ${B(isEn?'TOTAL3 (ex-BTC & ETH)':'TOTAL3 (без BTC и ETH)')}: ${isEn?'High beta alts.':'Высокобета альты.'}`);

  help.push('');

  help.push(`${B(isEn?'⁷ 24h Volume':'⁷ Объем 24 ч')} ${isEn?'— volume confirms or contradicts price.':'— объём подтверждает или опровергает ход цены.'}`);

  help.push('');

  help.push(`${B('⁸ RSI(14)')} ${isEn?'— momentum: ~70 overbought, ~30 oversold.':'— импульс: ≈70 перегрев, ≈30 перепроданность.'}`);
  if (snapshots.BTC) help.push(`• ${B('BTC')}: ${conciseRsiAdvice((snapshots.BTC||{}).rsi14,isEn)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH')}: ${conciseRsiAdvice((snapshots.ETH||{}).rsi14,isEn)}`);

  help.push('');

  help.push(`${B(isEn?'⁹ OI and CVD':'⁹ OI и CVD')} — ${isEn
    ? 'OI shows change in open interest (position size), CVD shows who is aggressive (buyers vs sellers).'
    : 'OI показывает изменение открытого интереса (размер позиций), CVD — кто агрессор (покупатели или продавцы).'
  }`);
  if (isEn) {
    help.push(`• 🟢 ${B('Longs inflow')}: trend-long on pullbacks; don’t chase.`);
    help.push(`• 🟡 ${B('Short-cover')}: avoid chasing shorts; longs only after pullback/confirmation.`);
    help.push(`• 🟠 ${B('Absorption')}: breakout-longs are risky; fade at resistances with tight risk.`);
    help.push(`• ⚪️ ${B('Cooling')}: trade levels, base size; wait for signals.`);
  } else {
    help.push(`• 🟢 ${B('Приток лонгов')}: тренд-лонг по откату; не гнаться.`);
    help.push(`• 🟡 ${B('Short-cover')}: не шортить в догонку; лонг после отката/подтверждения.`);
    help.push(`• 🟠 ${B('Впитывание')}: пробойные лонги опасны; работать от сопротивлений с узким риском.`);
    help.push(`• ⚪️ ${B('Охлаждение')}: торговать от уровней, базовый размер; ждать сигналов.`);
  }

  help.push('');

  help.push(`${B(isEn?'¹⁰ Leaders of interest':'¹⁰ Лидеры интереса')} ${isEn
    ? '— composite ranking by |OIΔ%| and |CVD$| to highlight tickers where position build-up aligns with aggressive flow.'
    : '— композитный рейтинг по |OIΔ%| и |CVD$|; показывает, где совпадают набор позиций и агрессивный поток.'}`);
  help.push(
    isEn
      ? `${B('In lines:')} OI Δ — % change for the window; CVD — net taker volume for the window (Binance units; focus on sign and relative size).`
      : `${B('В строках:')} OI Δ — изменение OI за окно в %; CVD — нетто-объём агрессоров за окно (единицы как на Binance; важны знак и относительная величина).`
  );

  help.push('');

  help.push(`${B(isEn?'¹¹ Net flows':'¹¹ Притоки/оттоки')} ${isEn?'— inflow = potential sell pressure; outflow = support.':'— приток = возможное давление продаж; отток = поддержка.'}`);
  if (snapshots.BTC) help.push(`• ${B('BTC')}: ${conciseFlowsAdvice((snapshots.BTC||{}).netFlowsUSDNow,isEn)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH')}: ${conciseFlowsAdvice((snapshots.ETH)||{}.netFlowsUSDNow,isEn)}`);

  help.push('');

  help.push(`${B(isEn?'¹² Funding':'¹² Фандинг')} ${isEn?'— perp funding rate.':'— ставка финансирования на перпетуалах.'}`);
  if (snapshots.BTC) help.push(`• ${B('BTC')}: ${conciseFundingAdvice((snapshots.BTC||{}).fundingNow,isEn)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH')}: ${conciseFundingAdvice((snapshots.ETH||{}).fundingNow,isEn)}`);

  help.push('');

  help.push(`${B(isEn?'¹³ Risk':'¹³ Риск')} ${isEn?'— aggregate indicator combining price change, funding, L/S, OI/CVD, FGI and market breadth. Higher risk → smaller size, tighter stops, avoid adding leverage; lower risk → work by plan, entries only on signals.':'— агрегатор цены, фандинга, L/S, OI/CVD, FGI и широты рынка. Высокий риск → уменьшать размер, тянуть/сужать стопы, не повышать плечо; низкий риск → работать по плану, входы по сетапам, без разгона плеча.'}`);

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

  const footerHtml = `\n${T.asof}: ${B(`${asOf}${tzSuffix}`)} — ${T.updatesNote}`;

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
    updatesNote:'updates every 30 min'
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
    updatesNote:'обновляются каждые 30 мин'
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
  const footerHtml = `\n${T.asof}: ${B(`${asOf}${tzSuffix}`)} — ${T.updatesNote}`;

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
  const { fullHtml } = buildMorningReportParts(snapshots, lang, tsIsoKyiv, tsEpoch, extras);
  return fullHtml;
}

export async function startMarketMonitor(){ return { ok:true }; }

export async function broadcastMarketSnapshot(bot, { batchSize=MARKET_BATCH_SIZE || 25, pauseMs=MARKET_BATCH_PAUSE_MS || 400 } = {}){
  if (!usersCollection) return { ok:false, reason:'mongo_not_connected' };

  const recipients = await usersCollection.find(
    { botBlocked: { $ne: true }, sendMarketReport: { $ne: false } },
    { projection: { userId: 1, lang: 1 } }
  ).toArray();

  if (!recipients.length) return { ok:true, delivered:0, users:0, batchSize, pauseMs };

  const snap = await getMarketSnapshot(['BTC','ETH','PAXG']).catch(()=>null);
  if (!snap?.ok) return { ok:false, reason:'snapshot_failed', delivered:0, users:recipients.length };

  const { snapshots, atIsoKyiv, fetchedAt, btcDominancePct, btcDominanceDelta, spx, totals, fgiNow, fgiDelta, oiCvdBTC, oiCvdETH, leadersTop } = snap;

  let delivered = 0;
  for (let i = 0; i < recipients.length; i += batchSize) {
    const chunk = recipients.slice(i, i + batchSize);
    await Promise.all(chunk.map(async (u) => {
      try {
        const lang = await resolveUserLang(u.userId).catch(() => u.lang || 'ru');
        const parts = buildMorningReportParts(
          snapshots,
          lang,
          atIsoKyiv,
          fetchedAt,
          { btcDominancePct, btcDominanceDelta, spx, totals, fgiNow, fgiDelta, oiCvdBTC, oiCvdETH, leadersTop }
        );
        const isEn = String(lang).toLowerCase().startsWith('en');
        const kb = { inline_keyboard: [[
            { text: isEn ? 'Short report' : 'Краткий отчёт', callback_data: 'market_short' },
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
  const parts = buildMorningReportParts(
    snap.snapshots,
    lang,
    snap.atIsoKyiv || '',
    snap.fetchedAt ?? null,
    { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, spx: snap.spx, totals: snap.totals, fgiNow: snap.fgiNow, fgiDelta: snap.fgiDelta, oiCvdBTC: snap.oiCvdBTC, oiCvdETH: snap.oiCvdETH, leadersTop: snap.leadersTop }
  );
  const isEn = String(lang).toLowerCase().startsWith('en');
  const kb = { inline_keyboard: [[
      { text: isEn ? 'Short report' : 'Краткий отчёт', callback_data: 'market_short' },
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
      { text: isEn ? 'Full report' : 'Полный отчёт', callback_data: 'market_full' },
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
    if(!snap?.ok) { await ctx.answerCbQuery(isEn?'Error':'Ошибка'); return; }
    const parts = buildMorningReportParts(
      snap.snapshots,
      lang,
      snap.atIsoKyiv || '',
      snap.fetchedAt ?? null,
      { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, spx: snap.spx, totals: snap.totals, fgiNow: snap.fgiNow, fgiDelta: snap.fgiDelta, oiCvdBTC: snap.oiCvdBTC, oiCvdETH: snap.oiCvdETH, leadersTop: snap.leadersTop }
    );
    const kb = { inline_keyboard: [[
        { text: isEn ? 'Short report' : 'Краткий отчёт', callback_data: 'market_short' },
        { text: isEn ? 'Full report'  : 'Полный отчёт',   callback_data: 'market_full'  }
      ]] };
    await ctx.reply(parts.helpHtml + '\n' + parts.footerHtml, { parse_mode:'HTML', reply_markup: kb });
    await ctx.answerCbQuery(okText);
  } catch { try { await ctx.answerCbQuery('Ошибка'); } catch {} }
}

export async function editReportMessageToShort(ctx){
  try{
    const userId = ctx.from?.id;
    const lang = await resolveUserLang(userId).catch(()=> 'ru');
    const isEn = String(lang).toLowerCase().startsWith('en');
    const snap=await getMarketSnapshot(['BTC','ETH','PAXG']);
    if(!snap?.ok) { await ctx.answerCbQuery(isEn?'Error':'Ошибка'); return; }
    const { shortHtml, footerHtml } = buildShortReportParts(
      snap.snapshots,
      lang,
      snap.atIsoKyiv || '',
      snap.fetchedAt ?? null,
      { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, totals: snap.totals, fgiNow: snap.fgiNow, fgiDelta: snap.fgiDelta }
    );
    const kb = { inline_keyboard: [[
        { text: isEn ? 'Full report' : 'Полный отчёт', callback_data: 'market_full' },
        { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
      ]] };
    await ctx.editMessageText(shortHtml + '\n' + footerHtml, { parse_mode:'HTML', reply_markup: kb });
    await ctx.answerCbQuery(isEn?'Done.':'Готово.');
  } catch { try { await ctx.answerCbQuery('Ошибка'); } catch {} }
}

export async function editReportMessageToFull(ctx){
  try{
    const userId = ctx.from?.id;
    const lang = await resolveUserLang(userId).catch(()=> 'ru');
    const isEn = String(lang).toLowerCase().startsWith('en');
    const snap=await getMarketSnapshot(['BTC','ETH','PAXG']);
    if(!snap?.ok) { await ctx.answerCbQuery(isEn?'Error':'Ошибка'); return; }
    const parts = buildMorningReportParts(
      snap.snapshots, lang, snap.atIsoKyiv || '', snap.fetchedAt ?? null,
      { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, spx: snap.spx, totals: snap.totals, fgiNow: snap.fgiNow, fgiDelta: snap.fgiDelta, oiCvdBTC: snap.oiCvdBTC, oiCvdETH: snap.oiCvdETH, leadersTop: snap.leadersTop }
    );
    const kb = { inline_keyboard: [[
        { text: isEn ? 'Short report' : 'Краткий отчёт', callback_data: 'market_short' },
        { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
      ]] };
    await ctx.editMessageText(parts.headHtml + '\n' + parts.footerHtml, { parse_mode:'HTML', reply_markup: kb });
    await ctx.answerCbQuery(isEn?'Done.':'Готово.');
  } catch { try { await ctx.answerCbQuery('Ошибка'); } catch {} }
}
