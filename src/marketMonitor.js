// src/marketMonitor.js
import { resolveUserLang } from './cache.js';
import { usersCollection, client } from './db.js';
import { MARKET_BATCH_SIZE, MARKET_BATCH_PAUSE_MS } from './constants.js';

const ADMIN_ID = process.env.CREATOR_ID ? String(process.env.CREATOR_ID) : '';

const esc = (s) => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const B = (s) => `<b>${esc(s)}</b>`;
const U = (s) => `<u>${esc(s)}</u>`;
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
function aggregateScore({ priceRisk, fundingRisk=0, sentimentRisk=0 }){
  const s = 0.5*priceRisk + 0.2*fundingRisk + 0.3*sentimentRisk;
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
function fgiEmojiFromValue(v){
  const val = Number(v);
  if (!Number.isFinite(val)) return '—';
  if (val <= 24) return '😱';
  if (val <= 44) return '😟';
  if (val <= 54) return '😐';
  if (val <= 74) return '🙂';
  return '😎';
}

function renderLsBlock(ls, isEn, label){
  const lbl = label || (isEn ? 'Asset' : 'Актив');
  if (!ls || !Number.isFinite(ls.longPct) || !Number.isFinite(ls.shortPct)) return `${lbl}: —`;
  const greens = Math.max(0, Math.min(10, Math.round(ls.longPct/10)));
  const reds   = 10 - greens;
  const bar = '🟩'.repeat(greens) + '🟥'.repeat(reds);
  const L = isEn ? 'Longs' : 'Лонги';
  const S = isEn ? 'Shorts' : 'Шорты';
  return `${lbl}:\n• ${L} ${B(`${ls.longPct}%`)} | ${S} ${B(`${ls.shortPct}%`)}\n${bar}`;
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

function concisePriceAdvice(pct,isEn){
  if (!Number.isFinite(pct)) return isEn?'Wait for confirmations; don’t enter without a setup.':'Ждать подтверждений; не входить без сетапа.';
  if (pct >= 3) return isEn?'Hold/partial TP; don’t chase or add leverage.':'Держать/частично фиксировать; не догонять и не увеличивать плечо.';
  if (pct >= 1) return isEn?'Hold/soft DCA; avoid impulsive leveraged longs.':'Держать/мягкий DCA; избегать импульсных лонгов с плечом.';
  if (pct <= -3) return isEn?'Wait for reversal; don’t catch knives, reduce size.':'Ждать разворота; не ловить ножи, размер снижать.';
  if (pct <= -1) return isEn?'Wait for confirmations; do not raise risk.':'Ждать подтверждений; риск не повышать.';
  return isEn?'Neutral; entries only on signals.':'Нейтрально; входы только по сигналу.';
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
function conciseLsAdvice(longPct,isEn){
  if (!Number.isFinite(longPct)) return isEn?'Watch price/volume; L/S low value now.':'Смотри цену/объём; L/S малоинформативен сейчас.';
  if (longPct > 65) return isEn?'Longs crowded — long-squeeze risk; don’t add leverage.':'Перегружены лонги — риск лонг-сквиза; не добавлять плечо.';
  if (longPct < 45) return isEn?'Shorts crowded — short-squeeze risk; careful with shorts.':'Перегружены шорты — риск шорт-сквиза; осторожно с шортами.';
  return isEn?'Balanced positioning — no extremes.':'Баланс позиций — без крайностей.';
}
function conciseRiskAdvice(score,isEn){
  const pct = Math.round((Number(score)||0)*100);
  if (pct >= 60) return isEn?'Reduce exposure, partial TP; avoid new aggressive longs.':'Снижать экспозицию, частично фиксировать; не открывать новые агрессивные лонги.';
  if (pct >= 30) return isEn?'Cut leverage, trail stops; don’t ramp positions.':'Резать плечо, тянуть стопы; не разгонять позицию.';
  if (pct >= 10) return isEn?'Entries only on confirmation; don’t add leverage.':'Входы только по подтверждению; не добавлять плечо.';
  return isEn?'Hold / gentle DCA; do not raise risk.':'Держать/аккуратно усреднять; риск не повышать.';
}

function flowsHeaderLine(sym, isEn){
  const now = Number(sym?.netFlowsUSDNow);
  const prev = Number(sym?.netFlowsUSDPrev);
  const diff = Number(sym?.netFlowsUSDDiff);
  if (!Number.isFinite(now) && !Number.isFinite(prev)) return '—';
  const abbr = Number.isFinite(now) ? `${now>=0?'+':'−'}${abbrevWithUnit(Math.abs(now), isEn)}` : '';
  let deltaPart = '';
  if (Number.isFinite(prev) && Math.abs(prev) > 0 && Number.isFinite(diff)) {
    const diffPct = (diff/Math.abs(prev))*100;
    if (Number.isFinite(diffPct)) {
      const circ = circleByDelta(diffPct);
      deltaPart = ` ${circ}(${B(`${diffPct>0?'+':''}${diffPct.toFixed(2)}%`)} ${isEn?'vs prev 24h':'к пред. 24ч'})`;
    }
  }
  return `${B(abbr || '—')}${deltaPart}`;
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
  const proj = { at:1, snapshots:1, btcDominancePct:1, spx:1, totals:1 };
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
  const cur = db.collection(collection).find({}, { projection: { at:1, snapshots:1, btcDominancePct:1, spx:1, totals:1 } }).sort({ at: -1 }).limit(500);
  while (await cur.hasNext()) { const d = await cur.next(); if (hasValue(d)) return d; }
  return null;
}

export async function getMarketSnapshot(symbols=['BTC','ETH','PAXG']){
  const dbName = process.env.DB_NAME || 'crypto_alert_dev';
  const collection = process.env.COLLLECTION || process.env.COLLECTION || 'marketSnapshots';
  const db = client.db(dbName);

  const freshest = await db.collection(collection).find({}, { projection: { snapshots:1, at:1, atIsoKyiv:1, btcDominancePct:1, spx:1, totals:1 } }).sort({ at: -1 }).limit(1).next();
  if (!freshest || !freshest.snapshots) return { ok:false, reason:'no_snapshot' };

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

  return {
    ok:true,
    snapshots: subset,
    fetchedAt: freshest.at,
    atIsoKyiv: freshest.atIsoKyiv || '',
    btcDominancePct: isNum(domNowVal) ? domNowVal : null,
    btcDominanceDelta: isNum(domDeltaVal) ? domDeltaVal : null,
    spx,
    totals,
    fgiNow: isNum(fgiNow) ? fgiNow : null,
    fgiDelta: isNum(fgiDelta) ? fgiDelta : null
  };
}

function buildMorningReportParts(snapshots, lang='ru', tsIsoKyiv='', tsEpoch=null, extras={}){
  const isEn=String(lang).toLowerCase().startsWith('en');
  const T=isEn?{
    report:'REPORT',
    asof:'As of',
    price:'Price *¹',
    fgi:'Fear & Greed *²',
    dom:'BTC Dominance *³',
    spx:'S&P 500 *⁴',
    totals:'Market cap *⁵',
    volumes:'24h Volume *⁶',
    rsi:'RSI (14) *⁷',
    flows:'Net flows *⁸',
    funding:'Funding rate (avg) *⁹',
    ls:'Longs vs Shorts *¹⁰',
    risks:'Risk *¹¹',
    over24h:'over 24h',
    updatesNote:'updates every 30 min'
  }:{
    report:'ОТЧЕТ',
    asof:'Данные на',
    price:'Цена *¹',
    fgi:'Индекс страха и жадности *²',
    dom:'Доминация BTC *³',
    spx:'S&P 500 *⁴',
    totals:'Рыночная капитализация *⁵',
    volumes:'Объем 24 ч *⁶',
    rsi:'RSI (14) *⁷',
    flows:'Притоки/оттоки *⁸',
    funding:'Фандинг (ср.) *⁹',
    ls:'Лонги vs Шорты *¹⁰',
    risks:'Риск *¹¹',
    over24h:'за 24 часа',
    updatesNote:'обновляются каждые 30 мин'
  };

  const when = formatKyiv(tsEpoch, tsIsoKyiv);
  const asOf = isEn ? when.en : when.ru;
  const tzSuffix = ' (Europe/Kyiv)';

  const priceLine = (sym, label) => {
    const pct = Number(sym?.pct24);
    const circ = circleByDelta(pct);
    const pctTxt = Number.isFinite(pct) ? `${circ} (${B(`${pct>0?'+':''}${pct.toFixed(2)}%`)} ${T.over24h})` : '(—)';
    const p = Number.isFinite(sym?.price) ? `$${isEn?humanFmtEN(sym.price):humanFmt(sym.price)}` : '—';
    const lbl = label ? `${label} ` : '';
    return `${lbl}${B(p)} ${pctTxt}`;
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
  if (snapshots.BTC) head.push(`BTC ${priceLine((snapshots.BTC)||{})}`);
  if (snapshots.ETH) head.push(`ETH ${priceLine((snapshots.ETH)||{})}`);
  if (snapshots.PAXG) head.push(`PAXG ${priceLine((snapshots.PAXG)||{}, '')}`);
  head.push('');

  head.push(BU(T.dom));
  const domPct = typeof extras?.btcDominancePct === 'number' ? extras.btcDominancePct : null;
  const domDelta = typeof extras?.btcDominanceDelta === 'number' ? extras.btcDominanceDelta : null;
  const domParts = [];
  if (Number.isFinite(domPct)) domParts.push(B(`${domPct.toFixed(2)}%`));
  if (Number.isFinite(domDelta)) {
    const circ = circleByDelta(domDelta);
    domParts.push(`${circ} (${B(`${domDelta>0?'+':''}${domDelta.toFixed(2)}%`)} ${T.over24h})`);
  }
  head.push(`• ${domParts.length ? domParts.join(' ') : '—'}`);
  head.push('');

  head.push(BU(T.fgi));
  head.push(`• ${fgiLine((snapshots.BTC)||{})}`);
  head.push('');

  head.push(BU(T.ls));
  if (snapshots.BTC) head.push(renderLsBlock(((snapshots.BTC)||{}).longShort, isEn, 'BTC'));
  if (snapshots.ETH) head.push(renderLsBlock(((snapshots.ETH)||{}).longShort, isEn, 'ETH'));
  head.push('');

  head.push(BU(T.spx));
  const spxPrice = (extras?.spx && typeof extras.spx.price === 'number') ? extras.spx.price : null;
  const spxPct = (extras?.spx && typeof extras.spx.pct === 'number') ? extras.spx.pct : null;
  const spxParts = [];
  if (Number.isFinite(spxPrice)) spxParts.push(B(isEn?humanFmtEN(spxPrice):humanFmt(spxPrice)));
  if (Number.isFinite(spxPct)) {
    const spxCirc = circleByDelta(spxPct);
    spxParts.push(`${spxCirc} (${B(`${spxPct>0?'+':''}${spxPct.toFixed(2)}%`)} ${T.over24h})`);
  }
  head.push(`• ${spxParts.length ? spxParts.join(' ') : '—'}`);
  head.push('');

  head.push(BU(T.totals));
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
  head.push('');

  head.push(BU(T.volumes));
  if (snapshots.BTC) head.push(`• BTC: ${volumeLine((snapshots.BTC)||{})}`);
  if (snapshots.ETH) head.push(`• ETH: ${volumeLine((snapshots.ETH)||{})}`);
  head.push('');

  head.push(BU(T.rsi));
  if (snapshots.BTC) head.push(`• BTC: ${rsiLine((snapshots.BTC)||{})}`);
  if (snapshots.ETH) head.push(`• ETH: ${rsiLine((snapshots.ETH)||{})}`);
  head.push('');

  head.push(BU(T.flows));
  if (snapshots.BTC) head.push(`• BTC: ${flowsHeaderLine((snapshots.BTC)||{}, isEn)}`);
  if (snapshots.ETH) head.push(`• ETH: ${flowsHeaderLine((snapshots.ETH)||{}, isEn)}`);
  head.push('');

  head.push(BU(T.funding));
  if (snapshots.BTC) head.push(`• BTC: ${fundingLine((snapshots.BTC)||{})}`);
  if (snapshots.ETH) head.push(`• ETH: ${fundingLine((snapshots.ETH)||{})}`);
  head.push('');

  head.push(BU(T.risks));
  const scoreBTC = aggregateScore({
    priceRisk: priceChangeRisk((snapshots.BTC||{}).pct24),
    fundingRisk: fundingRiskFromNow((snapshots.BTC||{}).fundingNow),
    sentimentRisk: sentimentRiskFromLS((snapshots.BTC||{}).longShort?.longPct)
  });
  const scoreETH = aggregateScore({
    priceRisk: priceChangeRisk((snapshots.ETH||{}).pct24),
    fundingRisk: fundingRiskFromNow((snapshots.ETH||{}).fundingNow),
    sentimentRisk: sentimentRiskFromLS((snapshots.ETH||{}).longShort?.longPct)
  });
  const rBbar = `${riskBar(scoreBTC)} ${B(`${Math.round(scoreBTC*100)}%`)}`;
  const rEbar = `${riskBar(scoreETH)} ${B(`${Math.round(scoreETH*100)}%`)}`;
  if (snapshots.BTC) head.push(`• BTC:\n${rBbar}`);
  if (snapshots.ETH) head.push(`• ETH:\n${rEbar}`);
  head.push('');

  const help=[];
  help.push(BU(isEn?'Guide':'Справка'));
  help.push('');

  help.push(`${B(isEn?'¹ Price: spot.':'¹ Цена: спот.')} ${isEn?'— snapshot of current price and 24h change. PAXG = tokenized gold (≈ 1 troy oz per token).':'— кратко фиксирует текущую цену и её изменение за 24ч. PAXG — токенизированное золото (≈ 1 унция золота на 1 токен).'}${snapshots.PAXG?'':''}`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${isEn?'Wait for confirmations; do not raise risk.':'Ждать подтверждений; риск не повышать.'}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${isEn?'Wait for confirmations; do not raise risk.':'Ждать подтверждений; риск не повышать.'}`);
  if (snapshots.PAXG) help.push(`• ${B('PAXG:')} ${isEn?'Wait for confirmations; do not raise risk.':'Ждать подтверждений; риск не повышать.'}`);

  const fgiVal = Number((snapshots.BTC||{}).fgiValue);
  let fgiAdvice = isEn?'Neutral — stick to plan; don’t chase.':'Нейтрально — держать план; не бегать за движением.';
  if (Number.isFinite(fgiVal)) {
    if (fgiVal <= 24) fgiAdvice = isEn?'Extreme fear — reduce size; A+ setups only.':'Экстремальный страх — размер снижать, входы только по A+ сетапам.';
    else if (fgiVal <= 44) fgiAdvice = isEn?'Fear — entries only on confirmations; no averaging without stop.':'Страх — входы только по подтверждениям; не усреднять без стопа.';
    else if (fgiVal >= 75) fgiAdvice = isEn?'Extreme greed — cut leverage; take profits per plan.':'Экстремальная жадность — снижать плечо; фиксировать по правилам.';
    else if (fgiVal >= 55) fgiAdvice = isEn?'Greed — trim leverage; partial TP by rules.':'Жадность — резать плечо, частичная фиксация по плану.';
    else fgiAdvice = isEn?'Neutral — stick to plan; don’t chase.':'Нейтрально — держать план; не бегать за движением.';
  }

  help.push('');
  help.push(`${B(isEn?'² Fear & Greed':'² Индекс страха и жадности')} ${isEn?'— composite BTC sentiment.':'— сводный индикатор настроений по BTC.'}`);
  help.push(`• ${B(isEn?'BTC/Market:':'BTC/Market:')} ${fgiAdvice}`);

  help.push('');
  help.push(`${B(isEn?'³ BTC Dominance':'³ Доминация BTC')} ${isEn?'— BTC share of total crypto market cap. Rising = capital rotates to BTC; falling = interest in alts.':'— доля BTC в общей капитализации рынка. Рост — капитал уходит в BTC; падение — интерес к альтам.'}`);

  help.push('');
  help.push(`${B('⁴ S&P 500')} ${isEn?'— broad risk barometer; weakness pressures crypto, strength supports risk.':'— ориентир риска; слабость давит на крипту, рост поддерживает риск.'}`);

  help.push('');
  help.push(`${B(isEn?'⁵ Market cap':'⁵ Рыночная капитализация')} ${isEn?'— breadth of the crypto market.':'— ширина/масштаб крипторынка.'}`);
  help.push(`• ${B('TOTAL')}: ${isEn?'Total crypto market cap.':'Вся капитализация крипторынка.'} ${isEn?'When falling — reduce risk; when rising with volume — follow trend, partial TP by plan.':'При падении — снижать риск; при росте на объёме — работать по тренду, частичная фиксация по плану.'}`);
  help.push(`• ${B(isEn?'TOTAL2 (ex-BTC)':'TOTAL2 (без BTC)')}: ${isEn?'Market cap without BTC — proxy for altcoin breadth.':'Капитализация без BTC — прокси широты «альтсезона».'} ${isEn?'Rising TOTAL2 > TOTAL suggests alt rotation; fading warns to avoid weak alts.':'Рост TOTAL2 относительно TOTAL — ротация в альты; угасание — не залетать в слабые альты.'}`);
  help.push(`• ${B(isEn?'TOTAL3 (ex-BTC & ETH)':'TOTAL3 (без BTC и ETH)')}: ${isEn?'Altcoins without BTC & ETH — high beta segment.':'Альты без BTC и ETH — высокобета-сегмент.'} ${isEn?'Use for risk-on/off in small/mid-caps; manage size strictly.':'Используй для оценки risk-on/off в small/mid-cap; строго контролируй размер.'}`);

  help.push('');
  help.push(`${B(isEn?'⁶ 24h Volume':'⁶ Объем 24 ч')} ${isEn?'— confirms/weakens price moves.':'— подтверждает/ослабляет движение цены.'}`);

  help.push('');
  help.push(`${B('⁷ RSI(14)')} ${isEn?'— momentum: ~70 overbought, ~30 oversold.':'— импульс: ≈70 перегрев, ≈30 перепроданность.'}`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${conciseRsiAdvice((snapshots.BTC||{}).rsi14,isEn)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${conciseRsiAdvice((snapshots.ETH||{}).rsi14,isEn)}`);

  help.push('');
  help.push(`${B(isEn?'⁸ Net flows':'⁸ Net flows')} ${isEn?'— exchange inflows/outflows (inflow = sell pressure, outflow = support).':'— чистые притоки/оттоки на биржи (приток = давление продажи, отток = поддержка).'}`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${conciseFlowsAdvice((snapshots.BTC||{}).netFlowsUSDNow,isEn)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${conciseFlowsAdvice((snapshots.ETH||{}).netFlowsUSDNow,isEn)}`);

  help.push('');
  help.push(`${B(isEn?'⁹ Funding':'⁹ Funding')} ${isEn?'— perp rate between longs & shorts.':'— ставка между лонгами и шортами на фьючерсах.'}`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${conciseFundingAdvice((snapshots.BTC||{}).fundingNow,isEn)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${conciseFundingAdvice((snapshots.ETH||{}).fundingNow,isEn)}`);

  help.push('');
  help.push(`${B(isEn?'¹⁰ Longs/Shorts (L/S)':'¹⁰ Лонги/Шорты (L/S)')} ${isEn?'— imbalance raises squeeze risk.':'— перекос повышает риск сквиза.'}`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${conciseLsAdvice((snapshots.BTC||{}).longShort?.longPct,isEn)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${conciseLsAdvice((snapshots.ETH||{}).longShort?.longPct,isEn)}`);

  help.push('');
  help.push(`${B(isEn?'¹¹ Risk':'¹¹ Риск')} ${isEn?'— aggregate of price, funding, and L/S (0% low, 100% high).':'— агрегат цены, funding и L/S (0% низкий, 100% высокий).'}`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${conciseRiskAdvice(scoreBTC,isEn)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${conciseRiskAdvice(scoreETH,isEn)}`);

  if (asOf) {
    help.push('');
    help.push(`${T.asof}: ${B(`${asOf}${tzSuffix}`)} - ${T.updatesNote}`);
  }

  const headHtml = head.join('\n');
  const helpHtml = help.join('\n');
  const fullHtml = headHtml + '\n' + helpHtml;
  return { headHtml, helpHtml, fullHtml };
}

function buildShortReportParts(snapshots, lang='ru', extras={}){
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
    ratio:'BTC/ETH'
  } : {
    short:'КРАТКИЙ ОТЧЕТ',
    market:'Рынок',
    btc:'BTC',
    eth:'ETH',
    gold:'Золото',
    total:'Total',
    rsi:'RSI (BTC)',
    dom:'BTC.D',
    ratio:'BTC/ETH'
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
    return (isEn ? abbrevWithUnit(v, true) : abbrevWithUnit(v, false))
      .replace(/ (?=[A-Za-zА-Яа-яЁё.]+$)/, '');
  };
  const ratioFmt = (v) => Number.isFinite(v) ? v.toFixed(4) : '—';
  const circ = (v) => circleByDelta(Number(v));

  const lines = [];
  lines.push(`📌 ${BU(T.short)}`);

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

  return { shortHtml: lines.join('\n') };
}

export async function buildMorningReportHtml(snapshots, lang='ru', tsIsoKyiv='', tsEpoch=null, extras={}){
  const { fullHtml } = buildMorningReportParts(snapshots, lang, tsIsoKyiv, tsEpoch, extras);
  return fullHtml;
}

async function maybeNotifyAdmin(bot, text){
  try {
    if (!ADMIN_ID) return;
    await bot.telegram.sendMessage(ADMIN_ID, text);
  } catch {}
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

  const { snapshots, atIsoKyiv, fetchedAt, btcDominancePct, btcDominanceDelta, spx, totals, fgiNow, fgiDelta } = snap;

  let delivered = 0;
  for (let i = 0; i < recipients.length; i += batchSize) {
    const chunk = recipients.slice(i, i + batchSize);
    await Promise.all(chunk.map(async (u) => {
      try {
        const lang = await resolveUserLang(u.userId).catch(() => u.lang || 'ru');
        const parts = buildMorningReportParts(snapshots, lang, atIsoKyiv, fetchedAt, { btcDominancePct, btcDominanceDelta, spx, totals, fgiNow, fgiDelta });
        const isEn = String(lang).toLowerCase().startsWith('en');
        const kb = { inline_keyboard: [[
            { text: isEn ? 'Short report' : 'Краткий отчёт', callback_data: 'market_short' },
            { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
          ]] };
        await bot.telegram.sendMessage(u.userId, parts.headHtml, { parse_mode:'HTML', reply_markup: kb });
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
    { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, spx: snap.spx, totals: snap.totals, fgiNow: snap.fgiNow, fgiDelta: snap.fgiDelta }
  );
  const isEn = String(lang).toLowerCase().startsWith('en');
  const kb = { inline_keyboard: [[
      { text: isEn ? 'Short report' : 'Краткий отчёт', callback_data: 'market_short' },
      { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
    ]] };
  await bot.telegram.sendMessage(userId, parts.headHtml, { parse_mode:'HTML', reply_markup: kb });
  return { ok:true };
}

export async function sendShortReportToUser(bot, userId){
  const snap=await getMarketSnapshot(['BTC','ETH','PAXG']);
  if(!snap?.ok) return { ok:false };
  const lang=await resolveUserLang(userId).catch(()=> 'ru');
  const { shortHtml } = buildShortReportParts(
    snap.snapshots,
    lang,
    { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, totals: snap.totals, fgiNow: snap.fgiNow, fgiDelta: snap.fgiDelta }
  );
  const isEn = String(lang).toLowerCase().startsWith('en');
  const kb = { inline_keyboard: [[
      { text: isEn ? 'Full report' : 'Полный отчёт', callback_data: 'market_full' },
      { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
    ]] };
  await bot.telegram.sendMessage(userId, shortHtml, { parse_mode:'HTML', reply_markup: kb });
  return { ok:true };
}

export async function editReportMessageWithHelp(ctx){
  try {
    const userId = ctx.from?.id;
    const lang = await resolveUserLang(userId).catch(()=> 'ru');
    const isEn = String(lang).toLowerCase().startsWith('en');
    const okText = isEn ? 'Done.' : 'Готово.';
    const errText = isEn ? 'Error' : 'Ошибка';

    const snap=await getMarketSnapshot(['BTC','ETH','PAXG']);
    if(!snap?.ok) { await ctx.answerCbQuery(errText); return; }
    const parts = buildMorningReportParts(
      snap.snapshots,
      lang,
      snap.atIsoKyiv || '',
      snap.fetchedAt ?? null,
      { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, spx: snap.spx, totals: snap.totals, fgiNow: snap.fgiNow, fgiDelta: snap.fgiDelta }
    );
    await ctx.editMessageText(parts.fullHtml, { parse_mode:'HTML', reply_markup: { inline_keyboard: [[
          { text: isEn ? 'Short report' : 'Краткий отчёт', callback_data: 'market_short' }
        ]] } });
    await ctx.answerCbQuery(okText);
  } catch {
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
}

export async function editReportMessageToShort(ctx){
  try{
    const userId = ctx.from?.id;
    const lang = await resolveUserLang(userId).catch(()=> 'ru');
    const isEn = String(lang).toLowerCase().startsWith('en');
    const snap=await getMarketSnapshot(['BTC','ETH','PAXG']);
    if(!snap?.ok) { await ctx.answerCbQuery(isEn?'Error':'Ошибка'); return; }
    const { shortHtml } = buildShortReportParts(
      snap.snapshots,
      lang,
      { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, totals: snap.totals, fgiNow: snap.fgiNow, fgiDelta: snap.fgiDelta }
    );
    const kb = { inline_keyboard: [[
        { text: isEn ? 'Full report' : 'Полный отчёт', callback_data: 'market_full' },
        { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
      ]] };
    await ctx.editMessageText(shortHtml, { parse_mode:'HTML', reply_markup: kb });
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
      { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, spx: snap.spx, totals: snap.totals, fgiNow: snap.fgiNow, fgiDelta: snap.fgiDelta }
    );
    const kb = { inline_keyboard: [[
        { text: isEn ? 'Short report' : 'Краткий отчёт', callback_data: 'market_short' },
        { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
      ]] };
    await ctx.editMessageText(parts.headHtml, { parse_mode:'HTML', reply_markup: kb });
    await ctx.answerCbQuery(isEn?'Done.':'Готово.');
  } catch { try { await ctx.answerCbQuery('Ошибка'); } catch {} }
}
