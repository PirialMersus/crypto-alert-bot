// src/marketMonitor.js
import { resolveUserLang } from './cache.js';
import { usersCollection, client } from './db.js';
import { MARKET_BATCH_SIZE, MARKET_BATCH_PAUSE_MS } from './constants.js';

const esc = (s) => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const B = (s) => `<b>${esc(s)}</b>`;
const U = (s) => `<u>${esc(s)}</u>`;
const BU = (s) => `<b><u>${esc(s)}</u></b>`;
const nearZero = (v) => Number.isFinite(v) && Math.abs(v) < 1e-8;
const isNum = (v) => Number.isFinite(Number(v));

function humanFmt(n) {
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
function pctStr(v) { return `${v>0?'+':''}${v.toFixed(2)}%`; }
function ppStr(v){ const sign = v>0?'+':''; return `${sign}${v.toFixed(2)} п.п.`; }

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

function renderLsBlock(ls, isEn, label){
  const lbl = label || (isEn ? 'Asset' : 'Актив');
  if (!ls || !Number.isFinite(ls.longPct) || !Number.isFinite(ls.shortPct)) return `${lbl}: —`;
  const greens = Math.max(0, Math.min(10, Math.round(ls.longPct/10)));
  const reds   = 10 - greens;
  const bar = '🟩'.repeat(greens) + '🟥'.repeat(reds);
  const l = isEn ? 'Longs' : 'Longs';
  const s = isEn ? 'Shorts' : 'Shorts';
  return `${lbl}:\n• ${l} ${B(`${ls.longPct}%`)} | ${s} ${B(`${ls.shortPct}%`)}\n${bar}`;
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

function concisePriceAdvice(pct, isEn){
  if (!Number.isFinite(pct)) return isEn?'Wait for confirmations; no entry without setup.':'Ждать подтверждений; не входить без сетапа.';
  if (pct >= 3) return isEn?'Hold/partial TP; don’t chase or add leverage.':'Держать/частично фиксировать; не догонять и не увеличивать плечо.';
  if (pct >= 1) return isEn?'Hold/soft DCA; avoid impulsive leveraged longs.':'Держать/мягкий DCA; избегать импульсных лонгов с плечом.';
  if (pct <= -3) return isEn?'Wait for reversal; don’t catch knives; reduce size.':'Ждать разворота; не ловить ножи, размер снижать.';
  if (pct <= -1) return isEn?'Wait for confirmations; don’t raise risk.':'Ждать подтверждений; риск не повышать.';
  return isEn?'Neutral; entries only on signals.':'Нейтрально; входы только по сигналу.';
}
function conciseRsiAdvice(v, isEn){
  if (!Number.isFinite(v)) return isEn?'No RSI — rely on price/volume.':'Без RSI — опора на цену/объём.';
  if (v >= 70) return isEn?'Overbought risk — tighten risk, watch divergences.':'Риск перекупленности — ужать риск, искать дивергенции.';
  if (v <= 30) return isEn?'Oversold — wait for reversal, avoid blind shorts.':'Перепроданность — ждать разворота, не шортить без подтверждения.';
  return isEn?'Momentum neutral — trade trend with stops.':'Импульс нейтрален — работать по тренду и стопам.';
}
function conciseFlowsAdvice(usd, isEn){
  if (!Number.isFinite(usd)) return isEn?'Don’t rely on flows alone; use confluence.':'Не полагайся на потоки отдельно; решения по совокупности сигналов.';
  if (usd > 0) return isEn?'Inflow — possible sell pressure; avoid all-in on pumps.':'Приток — возможные продажи; не входить all-in на росте.';
  if (usd < 0) return isEn?'Outflow — supportive; look for spot adds on pullbacks.':'Отток — поддержка; смотреть спот-добавки на откатах.';
  return isEn?'Flat — stick to the plan.':'Ровно — держать план.';
}
function conciseFundingAdvice(f, isEn){
  if (!Number.isFinite(f)) return isEn?'Assess without funding; don’t overweigh it.':'Оценивай без funding; не переоценивать метрику.';
  if (Math.abs(f) > 0.0003) return isEn?'Elevated funding — cut leverage; be ready for squeezes.':'Повышенный funding — резать плечо; готовность к сквизам.';
  return isEn?'Moderate funding — don’t add leverage without confirmation.':'Умеренный funding — плечо не увеличивать без подтверждений.';
}
function conciseLsAdvice(longPct, isEn){
  if (!Number.isFinite(longPct)) return isEn?'Use price/volume; L/S not informative now.':'Смотри цену/объём; L/S малоинформативен сейчас.';
  if (longPct > 65) return isEn?'Long-crowded — long squeeze risk; don’t add leverage.':'Перегружены лонги — риск лонг-сквиза; не добавлять плечо.';
  if (longPct < 45) return isEn?'Short-crowded — short squeeze risk; hedge shorts.':'Перегружены шорты — риск шорт-сквиза; осторожно с шортами.';
  return isEn?'Balanced positions — no extremes.':'Баланс позиций — без крайностей.';
}
function conciseRiskAdvice(score, isEn){
  const pct = Math.round((Number(score)||0)*100);
  if (pct >= 60) return isEn?'Reduce exposure, partial TP; avoid new aggressive longs.':'Снижать экспозицию, частично фиксировать; не открывать новые агрессивные лонги.';
  if (pct >= 30) return isEn?'Cut leverage, trail stops; don’t ramp positions.':'Резать плечо, тянуть стопы; не разгонять позицию.';
  if (pct >= 10) return isEn?'Entries only on confirmation; no leverage adds.':'Входы только по подтверждению; не добавлять плечо.';
  return isEn?'Hold/slow DCA; don’t raise risk.':'Держать/аккуратно усреднять; риск не повышать.';
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
      deltaPart = ` ${circ}(${B(pctStr(diffPct))} ${isEn?'vs prev 24h':'к пред. 24ч'})`;
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
  const proj = { at:1, snapshots:1, btcDominancePct:1, spx:1 };
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
  const cur = db.collection(collection).find({}, { projection: { at:1, snapshots:1, btcDominancePct:1, spx:1 } }).sort({ at: -1 }).limit(500);
  while (await cur.hasNext()) { const d = await cur.next(); if (hasValue(d)) return d; }
  return null;
}

export async function getMarketSnapshot(symbols=['BTC','ETH','PAXG']){
  const dbName = process.env.DB_NAME || 'crypto_alert_dev';
  const collection = process.env.COLLECTION || 'marketSnapshots';
  const db = client.db(dbName);

  const freshest = await db.collection(collection).find({}, { projection: { snapshots:1, at:1, atIsoKyiv:1, btcDominancePct:1, spx:1 } }).sort({ at: -1 }).limit(1).next();
  if (!freshest || !freshest.snapshots) return { ok:false, reason:'no_snapshot' };

  const subset = pickSubsetBySymbols(freshest.snapshots, symbols);

  const domNowDoc = isNum(freshest.btcDominancePct) ? freshest : (await findLatestDocWith(db, collection, d => isNum(d?.btcDominancePct)));
  const domNowVal = isNum(domNowDoc?.btcDominancePct) ? Number(domNowDoc.btcDominancePct) : null;
  let domDeltaVal = null;
  if (isNum(domNowDoc?.at) && isNum(domNowVal)) {
    const target = Number(domNowDoc.at) - 24*3600*1000;
    const ref = await findClosestWith(db, collection, target, d => isNum(d?.btcDominancePct));
    const refVal = isNum(ref?.btcDominancePct) ? Number(ref.btcDominancePct) : null;
    if (isNum(refVal)) domDeltaVal = domNowVal - refVal;
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

  return {
    ok:true,
    snapshots: subset,
    fetchedAt: freshest.at,
    atIsoKyiv: freshest.atIsoKyiv || '',
    btcDominancePct: isNum(domNowVal) ? domNowVal : null,
    btcDominanceDelta: isNum(domDeltaVal) ? domDeltaVal : null,
    spx
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
    volumes:'24h Volume *⁵',
    rsi:'RSI (14) *⁶',
    flows:'Net flows *⁷',
    funding:'Funding rate (avg) *⁸',
    ls:'Longs vs Shorts *⁹',
    risks:'Risk *¹⁰',
    over24h:'over 24h',
    ref:'Guide',
    updatesNote:'updates every 30 min'
  }:{
    report:'ОТЧЕТ',
    asof:'Данные на',
    price:'Цена *¹',
    fgi:'Индекс страха и жадности *²',
    dom:'Доминация BTC *³',
    spx:'S&P 500 *⁴',
    volumes:'Объем 24 ч *⁵',
    rsi:'RSI (14) *⁶',
    flows:'Притоки/оттоки *⁷',
    funding:'Funding rate (avg) *⁸',
    ls:'Лонги vs Шорты *⁹',
    risks:'Риск *¹⁰',
    over24h:'за 24 часа',
    ref:'Справка',
    updatesNote:'обновляются каждые 30 мин'
  };

  const when = formatKyiv(tsEpoch, tsIsoKyiv);
  const asOf = isEn ? when.en : when.ru;
  const tzSuffix = ' (Europe/Kyiv)';

  const priceLine = (sym, label) => {
    const pct = Number(sym?.pct24);
    const circ = circleByDelta(pct);
    const pctTxt = Number.isFinite(pct) ? `${circ} (${B(`${pct>0?'+':''}${pct.toFixed(2)}%`)} ${T.over24h})` : '(—)';
    const p = Number.isFinite(sym?.price) ? `$${humanFmt(sym.price)}` : '—';
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
    const base = B(humanFmt(now));
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
      const dTxt = `${circ}(${B(`${(bps>0?'+':'')}${(bps).toFixed(2)} б.п.`)})`;
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

  head.push(BU(T.fgi));
  head.push(`• ${fgiLine((snapshots.BTC)||{})}`);
  head.push('');

  head.push(BU(T.dom));
  const domPct = typeof extras?.btcDominancePct === 'number' ? extras.btcDominancePct : null;
  const domDelta = typeof extras?.btcDominanceDelta === 'number' ? extras.btcDominanceDelta : null;
  const domParts = [];
  if (Number.isFinite(domPct)) domParts.push(B(`${domPct.toFixed(2)}%`));
  if (Number.isFinite(domDelta) && Math.abs(domDelta) > 0) {
    const circ = circleByDelta(domDelta);
    domParts.push(`${circ} (${B(pctStr(domDelta))} ${T.over24h})`);
  }
  head.push(`• ${domParts.length ? domParts.join(' ') : '—'}`);
  head.push('');

  head.push(BU(T.spx));
  const spxPrice = (extras?.spx && typeof extras.spx.price === 'number') ? extras.spx.price : null;
  const spxPct = (extras?.spx && typeof extras.spx.pct === 'number') ? extras.spx.pct : null;
  const spxParts = [];
  if (Number.isFinite(spxPrice)) spxParts.push(B(humanFmt(spxPrice)));
  if (Number.isFinite(spxPct)) {
    const spxCirc = circleByDelta(spxPct);
    spxParts.push(`${spxCirc} (${B(`${spxPct>0?'+':''}${spxPct.toFixed(2)}%`)} ${T.over24h})`);
  }
  head.push(`• ${spxParts.length ? spxParts.join(' ') : '—'}`);
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

  head.push(BU(T.ls));
  if (snapshots.BTC) head.push(renderLsBlock(((snapshots.BTC)||{}).longShort, isEn, 'BTC'));
  if (snapshots.ETH) head.push(renderLsBlock(((snapshots.ETH)||{}).longShort, isEn, 'ETH'));
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
  help.push(BU(T.ref));
  help.push('');

  help.push(`${B(isEn?'¹ Price: spot.':'¹ Цена: спот.')} ${isEn?'Snapshot of current price and 24h change. PAXG ≈ tokenized gold (≈1 troy oz per token).':'— кратко фиксирует текущую цену и её изменение за 24ч. PAXG — токенизированное золото (≈ 1 унция золота на 1 токен).'}`);
  if (snapshots.BTC) help.push(`• BTC: ${concisePriceAdvice((snapshots.BTC||{}).pct24, isEn)}`);
  if (snapshots.ETH) help.push(`• ETH: ${concisePriceAdvice((snapshots.ETH||{}).pct24, isEn)}`);
  if (snapshots.PAXG) help.push(`• PAXG: ${concisePriceAdvice((snapshots.PAXG||{}).pct24, isEn)}`);

  const fgiVal = Number((snapshots.BTC||{}).fgiValue);
  let fgiAdvice = isEn?'Neutral — stick to plan; don’t chase.':'Нейтрально — держать план; не бегать за движением.';
  if (Number.isFinite(fgiVal)) {
    if (fgiVal <= 24) fgiAdvice = isEn?'Extreme fear — reduce size; A+ setups only.':'Экстремальный страх — размер снижать, входы только по A+ сетапам.';
    else if (fgiVal <= 44) fgiAdvice = isEn?'Fear — entries on confirmations only; no blind averaging.':'Страх — входы только по подтверждениям; не усреднять без стопа.';
    else if (fgiVal >= 75) fgiAdvice = isEn?'Extreme greed — cut leverage; take profits per plan.':'Экстремальная жадность — снижать плечо; фиксировать по правилам.';
    else if (fgiVal >= 55) fgiAdvice = isEn?'Greed — tighten risk; partial TP.':'Жадность — резать плечо, частичная фиксация по плану.';
  }

  help.push('');
  help.push(`${B(isEn?'² Fear & Greed':'² Индекс страха и жадности')} ${isEn? '— market sentiment composite for BTC.':'— сводный индикатор настроений по BTC.'}`);
  help.push(`• ${isEn?'BTC/Market:':'BTC/Market:'} ${fgiAdvice}`);

  help.push('');
  help.push(`${B(isEn?'³ BTC Dominance':'³ Доминация BTC')} ${isEn?'— BTC share of total crypto market cap. Rising → rotation to BTC; falling → interest in alts.':'— доля BTC в общей капитализации рынка. Рост — капитал уходит в BTC, падение — интерес к альтам.'}`);

  help.push('');
  help.push(`${B('⁴ S&P 500')} ${isEn?'— traditional risk barometer; weakness pressures crypto, strength supports risk.':'— ориентир риска на традиционных рынках; слабость часто давит на крипту, рост поддерживает риск.'}`);

  help.push('');
  help.push(`${B(isEn?'⁵ 24h Volume':'⁵ Объем 24 ч')} ${isEn?'— confirms/weakens price moves.':'— подтверждает/ослабляет движение цены.'}`);

  help.push('');
  help.push(`${B('⁶ RSI(14)')} ${isEn?'— momentum: ≈70 overbought, ≈30 oversold.':'— импульс: ≈70 перегрев, ≈30 перепроданность.'}`);
  if (snapshots.BTC) help.push(`• BTC: ${conciseRsiAdvice((snapshots.BTC||{}).rsi14, isEn)}`);
  if (snapshots.ETH) help.push(`• ETH: ${conciseRsiAdvice((snapshots.ETH||{}).rsi14, isEn)}`);

  help.push('');
  help.push(`${B(isEn?'⁷ Net flows':'⁷ Net flows')} ${isEn?'— exchange inflows/outflows (inflow=sell pressure, outflow=support).':'— чистые притоки/оттоки на биржи (приток = давление продажи, отток = поддержка).'}`);
  if (snapshots.BTC) help.push(`• BTC: ${conciseFlowsAdvice((snapshots.BTC||{}).netFlowsUSDNow, isEn)}`);
  if (snapshots.ETH) help.push(`• ETH: ${conciseFlowsAdvice((snapshots.ETH||{}).netFlowsUSDNow, isEn)}`);

  help.push('');
  help.push(`${B('⁸ Funding')} ${isEn?'— perp rate between longs & shorts.':'— ставка между лонгами и шортами на фьючерсах.'}`);
  if (snapshots.BTC) help.push(`• BTC: ${conciseFundingAdvice((snapshots.BTC||{}).fundingNow, isEn)}`);
  if (snapshots.ETH) help.push(`• ETH: ${conciseFundingAdvice((snapshots.ETH||{}).fundingNow, isEn)}`);

  help.push('');
  help.push(`${B(isEn?'⁹ Longs/Shorts (L/S)':'⁹ Лонги/Шорты (L/S)')} ${isEn?'— imbalance raises squeeze risk.':'— перекос повышает риск сквиза.'}`);
  if (snapshots.BTC) help.push(`• BTC: ${conciseLsAdvice((snapshots.BTC||{}).longShort?.longPct, isEn)}`);
  if (snapshots.ETH) help.push(`• ETH: ${conciseLsAdvice((snapshots.ETH||{}).longShort?.longPct, isEn)}`);

  help.push('');
  help.push(`${B(isEn?'¹⁰ Risk':'¹⁰ Риск')} ${isEn?'— aggregate of price, funding, and L/S (0% low, 100% high).':'— агрегат цены, funding и L/S (0% низкий, 100% высокий).'}`);
  if (snapshots.BTC) help.push(`• BTC: ${conciseRiskAdvice(scoreBTC, isEn)}`);
  if (snapshots.ETH) help.push(`• ETH: ${conciseRiskAdvice(scoreETH, isEn)}`);

  if (asOf) {
    help.push('');
    help.push(`${T.asof}: ${B(`${asOf}${tzSuffix}`)} - ${T.updatesNote}`);
  }

  const headHtml = head.join('\n');
  const helpHtml = help.join('\n');
  const fullHtml = headHtml + '\n' + helpHtml;
  return { headHtml, helpHtml, fullHtml };
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

  const { snapshots, atIsoKyiv, fetchedAt, btcDominancePct, btcDominanceDelta, spx } = snap;

  let delivered = 0;
  for (let i = 0; i < recipients.length; i += batchSize) {
    const chunk = recipients.slice(i, i + batchSize);
    await Promise.all(chunk.map(async (u) => {
      try {
        const lang = await resolveUserLang(u.userId).catch(() => u.lang || 'ru');
        const parts = buildMorningReportParts(snapshots, lang, atIsoKyiv, fetchedAt, { btcDominancePct, btcDominanceDelta, spx });
        const isEn = String(lang).toLowerCase().startsWith('en');
        const kb = { inline_keyboard: [[{ text: isEn ? 'Get data guide' : 'Получить справку по отчёту', callback_data: 'market_help' }]] };
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
    { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, spx: snap.spx }
  );
  const isEn = String(lang).toLowerCase().startsWith('en');
  const kb = { inline_keyboard: [[{ text: isEn ? 'Get data guide' : 'Получить справку по отчёту', callback_data: 'market_help' }]] };
  await bot.telegram.sendMessage(userId, parts.headHtml, { parse_mode:'HTML', reply_markup: kb });
  return { ok:true };
}

export async function editReportMessageWithHelp(ctx){
  try {
    const userId = ctx.from?.id;
    const lang = await resolveUserLang(userId).catch(()=> 'ru');
    const snap=await getMarketSnapshot(['BTC','ETH','PAXG']);
    if(!snap?.ok) { await ctx.answerCbQuery(); return; }
    const parts = buildMorningReportParts(
      snap.snapshots,
      lang,
      snap.atIsoKyiv || '',
      snap.fetchedAt ?? null,
      { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, spx: snap.spx }
    );
    await ctx.editMessageText(parts.fullHtml, { parse_mode:'HTML', reply_markup: { inline_keyboard: [] } });
    await ctx.answerCbQuery();
  } catch {
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
}
