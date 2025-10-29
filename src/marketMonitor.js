// src/marketMonitor.js
import { httpGetWithRetry } from './httpClient.js';
import { resolveUserLang } from './cache.js';
import { usersCollection, client } from './db.js';
import { MARKET_BATCH_SIZE, MARKET_BATCH_PAUSE_MS } from './constants.js';

const UA = { headers: { 'User-Agent': 'Mozilla/5.0 Chrome/120' } };

const esc = (s) => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const B = (s) => `<b>${esc(s)}</b>`;
const U = (s) => `<u>${esc(s)}</u>`;
const BU = (s) => `<b><u>${esc(s)}</u></b>`;

function humanFmt(n) {
  if (!Number.isFinite(n)) return '—';
  try {
    if (Math.abs(n) >= 1000) return Intl.NumberFormat('en-US',{maximumFractionDigits:0}).format(Math.round(n));
    if (Math.abs(n) >= 1)   return Intl.NumberFormat('en-US',{maximumFractionDigits:2}).format(Number(n.toFixed(2)));
    return Number(n).toPrecision(6).replace(/(?:\.0+$|(?<=\.[0-9]*?)0+)$/,'');
  } catch { return String(n); }
}
const nearZero = (v) => Number.isFinite(v) && Math.abs(v) < 1e-8;

function fmtFunding(v) { if(!Number.isFinite(v)) return '—'; return Number(v).toFixed(8).replace(/\.0+$|0+$/,''); }
function circleByDelta(x) { if(!Number.isFinite(x) || x===0) return '⚪'; return x>0?'🟢':'🔴'; }
function pctStr(v) { return `${v>0?'+':''}${v.toFixed(2)}%`; }

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
  if (val <= 24) color = '🟥';
  else if (val <= 44) color = '🟧';
  else if (val <= 54) color = '🟨';
  else color = '🟩';
  return color.repeat(filled) + '⬜'.repeat(10 - filled);
}
function translateFgiClass(cls, isEn) {
  if (!cls) return null;
  const dict = {
    'Extreme Fear': { ru: 'Экстремальный страх', en: 'Extreme Fear' },
    'Fear':         { ru: 'Страх',               en: 'Fear' },
    'Neutral':      { ru: 'Нейтрально',          en: 'Neutral' },
    'Greed':        { ru: 'Жадность',            en: 'Greed' },
    'Extreme Greed':{ ru: 'Экстремальная жадность', en: 'Extreme Greed' }
  };
  const rec = dict[cls] || null;
  return isEn ? (rec?.en || cls) : (rec?.ru || cls);
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
function renderLsBlock(ls, isEn, label){
  const lbl = label || (isEn ? 'Asset' : 'Актив');
  if (!ls || !Number.isFinite(ls.longPct) || !Number.isFinite(ls.shortPct)) return `${lbl}: —`;
  const greens = Math.max(0, Math.min(10, Math.round(ls.longPct/10)));
  const reds   = 10 - greens;
  const bar = '🟩'.repeat(greens) + '🟥'.repeat(reds);
  return `${lbl}:\n• Longs ${B(`${ls.longPct}%`)} | Shorts ${B(`${ls.shortPct}%`)}\n${bar}`;
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

function concisePriceAdvice(pct){
  if (!Number.isFinite(pct)) return 'Ждать подтверждений; не входить без сетапа.';
  if (pct >= 3) return 'Держать/частично фиксировать; не догонять и не увеличивать плечо.';
  if (pct >= 1) return 'Держать/мягкий DCA; избегать импульсных лонгов с плечом.';
  if (pct <= -3) return 'Ждать разворота; не ловить ножи, размер снижать.';
  if (pct <= -1) return 'Ждать подтверждений; риск не повышать.';
  return 'Нейтрально; входы только по сигналу.';
}
function conciseVolAdvice(delta){
  if (!Number.isFinite(delta)) return 'Смотри другие сигналы; не делай выводы по объёму.';
  if (delta > 5) return 'Объём растет — подтверждение тренда; соблюдай риск-менеджмент.';
  if (delta < -5) return 'Объём слабеет — снизить агрессию, брать A+ сетапы.';
  return 'Нейтрально; учитывать контекст.';
}
function conciseRsiAdvice(v){
  if (!Number.isFinite(v)) return 'Без RSI — опора на цену/объём.';
  if (v >= 70) return 'Риск перекупленности — ужать риск, искать дивергенции.';
  if (v <= 30) return 'Перепроданность — ждать разворота, не шортить без подтверждения.';
  return 'Импульс нейтрален — работать по тренду и стопам.';
}
function conciseFlowsAdvice(usd){
  if (!Number.isFinite(usd)) return 'Не полагайся на потоки отдельно; решения по совокупности сигналов.';
  if (usd > 0) return 'Приток — возможные продажи; не входить all-in на росте.';
  if (usd < 0) return 'Отток — поддержка; лонги только по подтверждению.';
  return 'Ровно — держать план; не делать выводы по потокам.';
}
function conciseFundingAdvice(f){
  if (!Number.isFinite(f)) return 'Оценивай без funding; не переоценивать метрику.';
  if (Math.abs(f) > 0.0003) return 'Повышенный funding — резать плечо, готовность к сквизам.';
  return 'Умеренный funding — плечо не увеличивать без подтверждений.';
}
function conciseLsAdvice(longPct){
  if (!Number.isFinite(longPct)) return 'Смотри цену/объём; L/S малоинформативен сейчас.';
  if (longPct > 65) return 'Перегружены лонги — риск лонг-сквиза; не добавлять плечо.';
  if (longPct < 45) return 'Перегружены шорты — риск шорт-сквиза; осторожно с шортами.';
  return 'Баланс позиций — без крайностей.';
}
function conciseRiskAdvice(score){
  const pct = Math.round((Number(score)||0)*100);
  if (pct >= 60) return 'Снижать экспозицию, частично фиксировать; не открывать новые агрессивные лонги.';
  if (pct >= 30) return 'Резать плечо, тянуть стопы; не разгонять позицию.';
  if (pct >= 10) return 'Входы только по подтверждению; не добавлять плечо.';
  return 'Держать/аккуратно усреднять; риск не повышать.';
}

function flowsHeaderLine(sym, isEn){
  const now = Number(sym?.netFlowsUSDNow);
  const prev = Number(sym?.netFlowsUSDPrev);
  const diff = Number(sym?.netFlowsUSDDiff);
  if (!Number.isFinite(now) && !Number.isFinite(prev)) return '—';
  const sNowMoney = Number.isFinite(now) ? `${now>=0?'+':'−'}$${humanFmt(Math.abs(now))}` : '—';
  const sNowAbbr  = Number.isFinite(now) ? `${now>=0?'+':'−'}${abbrevWithUnit(Math.abs(now), isEn)}` : '';
  let deltaPart = '';
  if (Number.isFinite(prev) && Math.abs(prev) > 0 && Number.isFinite(diff)) {
    const diffPct = (diff/Math.abs(prev))*100;
    if (Number.isFinite(diffPct)) {
      const circ = circleByDelta(diffPct);
      deltaPart = ` ${circ}(${B(pctStr(diffPct))} ${isEn?'vs prev 24h':'к пред. 24ч'})`;
    }
  }
  return `${B(`${sNowMoney}`)} (${B(sNowAbbr)})${deltaPart}`;
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
    ref:'Справка',
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
    const clsRaw = sym?.fgiClass || null;
    if (!Number.isFinite(v)) return '—';
    const cls = translateFgiClass(clsRaw, isEn);
    const bar = fearGreedBarColorized(v);
    return `${B(String(v))}${cls ? ` (${B(cls)})` : ''}\n${bar}`;
  };
  const volumeLine = (sym) => {
    const vol = Number(sym?.vol24);
    const deltaPct = Number(sym?.volDeltaPct);
    const circ = circleByDelta(deltaPct);
    const fullMoney = Number.isFinite(vol) ? `$${humanFmt(vol)}` : '—';
    const abbrVal = Number.isFinite(vol) ? abbrevWithUnit(vol, isEn) : '';
    const abbr = abbrVal ? `(${B(abbrVal)})` : '';
    const pctTxt = Number.isFinite(deltaPct) ? `${circ}(${B(`${deltaPct>0?'+':''}${deltaPct.toFixed(2)}%`)} ${T.over24h})` : '(—)';
    return `${B(fullMoney)} ${abbr} ${pctTxt}`;
  };
  const rsiLine = (sym) => {
    const now = Number(sym?.rsi14), prev = Number(sym?.rsi14Prev);
    if(!Number.isFinite(now)) return '—';
    const base = B(humanFmt(now));
    if(Number.isFinite(prev)){
      const d = now - prev;
      const bps = d * 10000;
      const circ = circleByDelta(d);
      const dTxt = `${circ}(${B(`${d>0?'+':''}${d.toFixed(2)}`)} ${T.over24h}, ${B(`${d>0?'+':''}${Math.round(bps)} б.п.`)})`;
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
      const dTxt = `${circ}(${B(`${d>0?'+':''}${fmtFunding(d)}`)} ${T.over24h}, ${B(`${(bps>0?'+':'')}${(bps).toFixed(2)} б.п.`)})`;
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
  if (snapshots.PAXG) {
    const lbl = isEn ? 'PAXG (tokenized gold) ' : 'PAXG (токенизированное золото) ';
    head.push(priceLine((snapshots.PAXG)||{}, lbl));
  }
  head.push('');

  head.push(BU(T.fgi));
  head.push(`• ${fgiLine((snapshots.BTC)||{})}`);
  head.push('');

  head.push(BU(T.dom));
  const domPct = typeof extras?.btcDominancePct === 'number' ? extras.btcDominancePct : null;
  head.push(`• ${Number.isFinite(domPct) ? B(`${domPct.toFixed(2)}%`) : '—'}`);
  head.push('');

  head.push(BU(T.spx));
  const spxPrice = (extras?.spx && typeof extras.spx.price === 'number') ? extras.spx.price : null;
  const spxPct = (extras?.spx && typeof extras.spx.pct === 'number') ? extras.spx.pct : null;
  let spxLine = '—';
  if (Number.isFinite(spxPrice) || Number.isFinite(spxPct)) {
    const parts = [];
    if (Number.isFinite(spxPrice)) parts.push(B(humanFmt(spxPrice)));
    if (Number.isFinite(spxPct)) {
      const spxCirc = circleByDelta(spxPct);
      parts.push(`${spxCirc} (${B(`${spxPct>0?'+':''}${spxPct.toFixed(2)}%`)} ${T.over24h})`);
    }
    spxLine = parts.join(' ');
  }
  head.push(`• ${spxLine}`);
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

  help.push(`${B('¹ Цена: спот.')} — кратко фиксирует текущую цену и её изменение за 24ч.`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${concisePriceAdvice((snapshots.BTC||{}).pct24)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${concisePriceAdvice((snapshots.ETH||{}).pct24)}`);
  if (snapshots.PAXG) help.push(`• ${B('PAXG:')} ${concisePriceAdvice((snapshots.PAXG||{}).pct24)}`);

  const fgiVal = Number((snapshots.BTC||{}).fgiValue);
  let fgiAdvice = 'Нейтрально — держать план; не бегать за движением.';
  if (Number.isFinite(fgiVal)) {
    if (fgiVal <= 25) fgiAdvice = 'Страх — входы только по подтверждениям; не усреднять без стопа.';
    else if (fgiVal >= 75) fgiAdvice = 'Жадность — снижать плечо; фиксировать по правилам.';
  }

  help.push('');
  help.push(`${B('² Индекс страха и жадности')} — сводный индикатор настроений по BTC.`);
  help.push(`• ${B('BTC/Market:')} ${fgiAdvice}`);

  help.push('');
  help.push(`${B('³ Доминация BTC')} — доля BTC в общей капитализации рынка. Рост — капитал уходит в BTC, падение — интерес к альтам.`);

  help.push('');
  help.push(`${B('⁴ S&P 500')} — ориентир риска на традиционных рынках; слабость часто давит на крипту, рост поддерживает риск.`);

  help.push('');
  help.push(`${B('⁵ Объем 24 ч')} — подтверждает/ослабляет движение цены.`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${conciseVolAdvice((snapshots.BTC||{}).volDeltaPct)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${conciseVolAdvice((snapshots.ETH||{}).volDeltaPct)}`);

  help.push('');
  help.push(`${B('⁶ RSI(14)')} — импульс: ≈70 перегрев, ≈30 перепроданность.`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${conciseRsiAdvice((snapshots.BTC||{}).rsi14)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${conciseRsiAdvice((snapshots.ETH||{}).rsi14)}`);

  help.push('');
  help.push(`${B('⁷ Net flows')} — чистые притоки/оттоки на биржи (приток = давление продажи, отток = поддержка).`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${conciseFlowsAdvice((snapshots.BTC||{}).netFlowsUSDNow)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${conciseFlowsAdvice((snapshots.ETH||{}).netFlowsUSDNow)}`);

  help.push('');
  help.push(`${B('⁸ Funding')} — ставка между лонгами и шортами на фьючерсах.`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${conciseFundingAdvice((snapshots.BTC||{}).fundingNow)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${conciseFundingAdvice((snapshots.ETH||{}).fundingNow)}`);

  help.push('');
  help.push(`${B('⁹ Лонги/Шорты (L/S)')} — перекос повышает риск сквиза.`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${conciseLsAdvice((snapshots.BTC||{}).longShort?.longPct)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${conciseLsAdvice((snapshots.ETH||{}).longShort?.longPct)}`);

  help.push('');
  help.push(`${B('¹⁰ Риск')} — агрегат цены, funding и L/S (0% низкий, 100% высокий).`);
  if (snapshots.BTC) help.push(`• ${B('BTC:')} ${conciseRiskAdvice(scoreBTC)}`);
  if (snapshots.ETH) help.push(`• ${B('ETH:')} ${conciseRiskAdvice(scoreETH)}`);

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

function pickSubsetBySymbols(snapshots, symbols){
  const out={};
  for(const s of symbols){ if (snapshots?.[s]) out[s]=snapshots[s]; }
  return out;
}

export async function getMarketSnapshot(symbols=['BTC','ETH','PAXG']){
  const dbName = process.env.DB_NAME || 'crypto_alert_dev';
  const collection = process.env.COLLECTION || 'marketSnapshots';
  const db = client.db(dbName);
  const doc = await db.collection(collection).find().sort({ at: -1 }).limit(1).next();
  if (!doc || !doc.snapshots) return { ok:false, reason:'no_snapshot' };
  const subset = pickSubsetBySymbols(doc.snapshots, symbols);
  const domPct = Number.isFinite(Number(doc.btcDominancePct)) ? Number(doc.btcDominancePct) : null;
  const spx = doc.spx && typeof doc.spx === 'object' ? { price: Number.isFinite(Number(doc.spx.price)) ? Number(doc.spx.price) : null, pct: Number.isFinite(Number(doc.spx.pct)) ? Number(doc.spx.pct) : null, src: doc.spx.src || null } : { price:null, pct:null, src:null };
  return { ok:true, snapshots: subset, fetchedAt: doc.at, atIsoKyiv: doc.atIsoKyiv || '', btcDominancePct: domPct, spx };
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
  const { snapshots, atIsoKyiv, fetchedAt, btcDominancePct, spx } = snap;
  let delivered = 0;
  for (let i = 0; i < recipients.length; i += batchSize) {
    const chunk = recipients.slice(i, i + batchSize);
    await Promise.all(chunk.map(async (u) => {
      try {
        const lang = await resolveUserLang(u.userId).catch(() => u.lang || 'ru');
        const parts = buildMorningReportParts(snapshots, lang, atIsoKyiv, fetchedAt, { btcDominancePct, spx });
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
  const parts = buildMorningReportParts(snap.snapshots, lang, snap.atIsoKyiv || '', snap.fetchedAt ?? null, { btcDominancePct: snap.btcDominancePct, spx: snap.spx });
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
    const parts = buildMorningReportParts(snap.snapshots, lang, snap.atIsoKyiv || '', snap.fetchedAt ?? null, { btcDominancePct: snap.btcDominancePct, spx: snap.spx });
    await ctx.editMessageText(parts.fullHtml, { parse_mode:'HTML', reply_markup: { inline_keyboard: [] } });
    await ctx.answerCbQuery();
  } catch {
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
}
