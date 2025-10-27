// файл: src/marketMonitor.js
import { httpGetWithRetry } from './httpClient.js';
import { resolveUserLang } from './cache.js';
import { usersCollection } from './db.js';
import { MARKET_BATCH_SIZE, MARKET_BATCH_PAUSE_MS } from './constants.js';

// ---------- CONFIG / CONST ----------
const symbolsCfg = {
  BTC: { binance: 'BTCUSDT', coingecko: 'bitcoin' },
  ETH: { binance: 'ETHUSDT', coingecko: 'ethereum' },
  PAXG: { binance: 'PAXGUSDT', coingecko: 'pax-gold' }
};

const EXCHANGES = [
  'binance','coinbase','kraken','bybit','okx','bitfinex','huobi','kucoin','mexc','gate-io',
  'bitstamp','bingx','upbit','gemini','poloniex','bitget','deribit','btse','zb','bithumb'
];

const HARD_TIMEOUT_MS = Number.isFinite(Number(process.env.HARD_TIMEOUT_MS))
  ? Number(process.env.HARD_TIMEOUT_MS) : 8000;

const SNAPSHOT_TTL_MS = Number.isFinite(Number(process.env.SNAPSHOT_TTL_MS))
  ? Number(process.env.SNAPSHOT_TTL_MS) : (30 * 60 * 1000);

const BUST_CACHE = String(process.env.BUST_CACHE || '0') === '1';
const ALWAYS_PROXY = String(process.env.ALWAYS_PROXY || '0') === '1';
const PROXY_FETCH = process.env.PROXY_FETCH || '';

// общий UA
const UA = {
  headers: {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36'
  }
};

const SYNTH_FLOWS = String(process.env.SYNTH_FLOWS || '1') === '1';
const SYNTH_ALPHA = Number.isFinite(Number(process.env.SYNTH_ALPHA)) ? Number(process.env.SYNTH_ALPHA) : 0.6;

// ---------- UTILS ----------
function withTimeout(promise, ms = HARD_TIMEOUT_MS, label = 'req') {
  return Promise.race([
    promise,
    new Promise((_, rej) => setTimeout(() => rej(new Error(`timeout:${label}`)), ms))
  ]);
}

function prox(url) {
  try {
    if (ALWAYS_PROXY && PROXY_FETCH) {
      // прокси-воркер ожидает ?url=
      const u = new URL(PROXY_FETCH);
      u.searchParams.set('url', url);
      return u.toString();
    }
  } catch {}
  return url;
}

const esc = (s) => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const B = (s) => `<b>${esc(s)}</b>`;
const U = (s) => `<u>${esc(s)}</u>`;

function humanFmt(n) {
  if (!Number.isFinite(n)) return '—';
  try {
    if (Math.abs(n) >= 1_000_000) return Intl.NumberFormat('en-US',{maximumFractionDigits:0}).format(Math.round(n));
    if (Math.abs(n) >= 1000)      return Intl.NumberFormat('en-US',{maximumFractionDigits:0}).format(Math.round(n));
    if (Math.abs(n) >= 1)         return Intl.NumberFormat('en-US',{maximumFractionDigits:2}).format(Number(n.toFixed(2)));
    return Number(n).toPrecision(6).replace(/(?:\.0+|(?<=\.[0-9]*?)0+)$/,'');
  } catch { return String(n); }
}
const nearZero = (v) => Number.isFinite(v) && Math.abs(v) < 1e-8;

function fmtFunding(v) {
  if(!Number.isFinite(v)) return '—';
  return Number(v).toFixed(8).replace(/\.0+$|0+$/,'');
}

function circleByDelta(x) {
  if(!Number.isFinite(x) || x===0) return '⚪';
  return x>0?'🟢':'🔴';
}

function pctStr(v) { return `${v>0?'+':''}${v.toFixed(2)}%`; }

function abbrevWithUnit(n, isEn=false) {
  if(!Number.isFinite(n)) return '';
  const v = Math.abs(n);
  if (v >= 1_000_000_000_000) return `${(v/1_000_000_000_000).toFixed(2)} ${isEn?'T':'трлн'}`;
  if (v >= 1_000_000_000)     return `${(v/1_000_000_000).toFixed(2)} ${isEn?'B':'млрд'}`;
  if (v >= 1_000_000)         return `${(v/1_000_000).toFixed(2)} ${isEn?'M':'млн'}`;
  if (v >= 1_000)             return `${(v/1_000).toFixed(2)} ${isEn?'K':'тыс.'}`;
  return `${v.toFixed(2)}`;
}

function computeRSI(closes=[], period=14) {
  try {
    if(!Array.isArray(closes)||closes.length<period+1) return null;
    let gains=0,losses=0;
    for(let i=1;i<=period;i++){ const d=closes[i]-closes[i-1]; if(d>0) gains+=d; else losses+=Math.abs(d); }
    let avgGain=gains/period, avgLoss=losses/period;
    for(let i=period+1;i<closes.length;i++){ const d=closes[i]-closes[i-1]; avgGain=((avgGain*(period-1))+Math.max(0,d))/period; avgLoss=((avgLoss*(period-1))+Math.max(0,-d))/period; }
    if(avgLoss===0) return 100;
    const rs=avgGain/avgLoss, rsi=100-(100/(1+rs));
    return Number.isFinite(rsi)?Number(rsi.toFixed(2)):null;
  }catch{return null;}
}

// ---------- FETCH: COINGECKO ----------
async function fetchCoingeckoMarkets(ids=['bitcoin','ethereum']) {
  try {
    const url=`https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=${encodeURIComponent(ids.join(','))}&order=market_cap_desc&per_page=250&page=1&sparkline=false&price_change_percentage=24h&_t=${Date.now()}`;
    const res=await withTimeout(httpGetWithRetry(url,1,UA),HARD_TIMEOUT_MS,'coingecko:markets');
    return res?.data||null;
  }catch{return null;}
}
async function fetchCoingeckoMarketChart(id,days=15) {
  try {
    const url=`https://api.coingecko.com/api/v3/coins/${encodeURIComponent(id)}/market_chart?vs_currency=usd&days=${encodeURIComponent(String(days))}&interval=daily&_t=${Date.now()}`;
    const res=await withTimeout(httpGetWithRetry(url,1,UA),HARD_TIMEOUT_MS,`coingecko:chart:${id}`);
    return res?.data||null;
  }catch{return null;}
}

// ---------- FETCH: BINANCE (PRICE/VOL) ----------
async function fetchBinanceTicker24h(symbol) {
  try {
    const url=prox(`https://api.binance.com/api/v3/ticker/24hr?symbol=${encodeURIComponent(symbol)}&_t=${Date.now()}`);
    const res=await withTimeout(httpGetWithRetry(url,1,UA),HARD_TIMEOUT_MS,`binance:24hr:${symbol}`);
    const d=res?.data; if(!d) return null;
    const price=Number(d.lastPrice), pct24=Number(d.priceChangePercent), volQuote=Number(d.quoteVolume);
    return { price:Number.isFinite(price)?price:null, pct24:Number.isFinite(pct24)?pct24:null, vol24:Number.isFinite(volQuote)?volQuote:null };
  }catch{return null;}
}

// ---------- FETCH: BINANCE (FUNDING) ----------
async function fetchBinanceFundingSeries(symbol, limit=24) {
  try {
    const url=prox(`https://fapi.binance.com/fapi/v1/fundingRate?symbol=${encodeURIComponent(symbol)}&limit=${encodeURIComponent(String(limit))}&_t=${Date.now()}`);
    const res=await withTimeout(httpGetWithRetry(url,2,UA),HARD_TIMEOUT_MS,`binance:funding:${symbol}`);
    const arr=Array.isArray(res?.data)?res.data:[];
    const vals = arr.map(r=>Number(r.fundingRate)).filter(v=>Number.isFinite(v) && !nearZero(v));
    if (vals.length) return vals;
    throw new Error('empty_or_zeros');
  } catch {
    return [];
  }
}

async function fetchFundingSeriesViaWWW(symbol, limit=48) {
  try{
    const url = prox(`https://www.binance.com/futures/data/fundingRate?symbol=${encodeURIComponent(symbol)}&limit=${encodeURIComponent(String(limit))}&_t=${Date.now()}`);
    const res = await withTimeout(httpGetWithRetry(url,2,UA),HARD_TIMEOUT_MS,`binance-www:funding:${symbol}`);
    const arr = Array.isArray(res?.data) ? res.data : [];
    const vals = arr.map(r => Number(r.fundingRate)).filter(v => Number.isFinite(v) && !nearZero(v));
    return vals;
  }catch{ return []; }
}

async function fetchFundingNowFallback(symbol){
  try{
    const url = prox(`https://fapi.binance.com/fapi/v1/premiumIndex?symbol=${encodeURIComponent(symbol)}&_t=${Date.now()}`);
    const res = await withTimeout(httpGetWithRetry(url,2,UA),HARD_TIMEOUT_MS,`binance:premiumIndex:${symbol}`);
    const v = Number(res?.data?.lastFundingRate);
    return (Number.isFinite(v) && !nearZero(v)) ? v : null;
  }catch{return null;}
}

// ---------- FETCH: LONG/SHORT ----------
const lsCache = new Map();
const LS_TTL_MS = 5 * 60 * 1000;

function readLsCache(symbol){
  const r=lsCache.get(symbol);
  if(!r) return null;
  if(Date.now()-r.ts>LS_TTL_MS) return null;
  return r.data;
}
function writeLsCache(symbol, data){
  if(!data) return;
  lsCache.set(symbol, { ts: Date.now(), data });
}
function deriveLsFromRatio(ratio) {
  if (!Number.isFinite(ratio) || ratio <= 0) return null;
  const longPct = (ratio / (1 + ratio)) * 100;
  const shortPct = 100 - longPct;
  return {
    longPct: Number(longPct.toFixed(2)),
    shortPct: Number(shortPct.toFixed(2)),
    ls: Number(ratio.toFixed(2))
  };
}
async function fetchGlobalLongShort(symbol, period, limit=30) {
  const url = prox(`https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=${encodeURIComponent(symbol)}&period=${encodeURIComponent(period)}&limit=${encodeURIComponent(String(limit))}&_t=${Date.now()}`);
  const res = await withTimeout(httpGetWithRetry(url,1,UA),HARD_TIMEOUT_MS,`binance:ls:${symbol}:${period}`);
  const arr = Array.isArray(res?.data) ? res.data : [];
  if (!arr.length) return null;
  const last = arr[arr.length-1];
  const ratio = Number(last?.longShortRatio);
  const longAccount = Number(last?.longAccount);
  const shortAccount = Number(last?.shortAccount);
  if (Number.isFinite(longAccount) && Number.isFinite(shortAccount) && (longAccount + shortAccount) > 0) {
    const sum = longAccount + shortAccount;
    return {
      longPct: Number(((longAccount / sum) * 100).toFixed(2)),
      shortPct: Number(((shortAccount / sum) * 100).toFixed(2)),
      ls: Number((ratio && Number.isFinite(ratio) ? ratio : (longAccount/shortAccount)).toFixed(2))
    };
  }
  return deriveLsFromRatio(ratio);
}
async function fetchTopLongShortAccounts(symbol, period='4h', limit=30) {
  const url = prox(`https://fapi.binance.com/futures/data/topLongShortAccountRatio?symbol=${encodeURIComponent(symbol)}&period=${encodeURIComponent(period)}&limit=${encodeURIComponent(String(limit))}&_t=${Date.now()}`);
  const res = await withTimeout(httpGetWithRetry(url,1,UA),HARD_TIMEOUT_MS,`binance:tlsa:${symbol}:${period}`);
  const arr = Array.isArray(res?.data) ? res.data : [];
  if (!arr.length) return null;
  const last = arr[arr.length-1];
  const ratio = Number(last?.longShortRatio);
  return deriveLsFromRatio(ratio);
}
async function fetchTopLongShortPositions(symbol, period='4h', limit=30) {
  const url = prox(`https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=${encodeURIComponent(symbol)}&period=${encodeURIComponent(period)}&limit=${encodeURIComponent(String(limit))}&_t=${Date.now()}`);
  const res = await withTimeout(httpGetWithRetry(url,1,UA),HARD_TIMEOUT_MS,`binance:tlsp:${symbol}:${period}`);
  const arr = Array.isArray(res?.data) ? res.data : [];
  if (!arr.length) return null;
  const last = arr[arr.length-1];
  const ratio = Number(last?.longShortRatio);
  return deriveLsFromRatio(ratio);
}
async function fetchLongShortRatio(symbol){
  const cached = readLsCache(symbol);
  if (cached) return cached;
  const periods = ['4h','1h','30m'];
  for (const p of periods) {
    try {
      const r = await fetchGlobalLongShort(symbol, p);
      if (r && Number.isFinite(r.longPct) && Number.isFinite(r.shortPct) && Number.isFinite(r.ls)) {
        writeLsCache(symbol, r);
        return r;
      }
    } catch {}
  }
  try { const r = await fetchTopLongShortAccounts(symbol, '4h'); if (r) { writeLsCache(symbol, r); return r; } } catch {}
  try { const r = await fetchTopLongShortPositions(symbol, '4h'); if (r) { writeLsCache(symbol, r); return r; } } catch {}
  return null;
}

// ---------- FETCH: CEX NET FLOWS (DefiLlama) ----------
// (без изменений)
function pickNum(...vals){
  for(const v of vals){ const n=Number(v); if(Number.isFinite(n)) return n; }
  return null;
}
function normPoint(p){
  const ts=Number(p?.timestamp??p?.t??p?.time??p?.date??p?.x??0);
  const native=pickNum(p?.balance,p?.amount,p?.qty,p?.tokenBalance,p?.asset,p?.value_native,p?.native,p?.n);
  const usd=pickNum(p?.usd,p?.usd_value,p?.value_usd,p?.valueUSD,p?.totalUSD,p?.y,p?.value,p?.usdValue,p?.sumUSD);
  return { ts, native, usd };
}
function readSeriesForSymbol(data, symbol){
  symbol=String(symbol).toUpperCase();
  const out=[];
  const add = (arr)=>{ for(const p of (arr||[])) { const pt=normPoint(p); if(Number.isFinite(pt.ts)) out.push(pt);} };
  add(Array.isArray(data?.tokens)?data.tokens:[]);
  add(Array.isArray(data?.assets)?data.assets:[]);
  add(Array.isArray(data?.data)?data.data:[]);
  add(Array.isArray(data?.series)?data.series:[]);
  if (Array.isArray(data?.charts)) {
    for(const ch of data.charts){
      if((ch?.symbol||ch?.token||ch?.name||'').toUpperCase()===symbol) add(ch?.data||[]);
    }
  }
  return out
    .filter(pt => Number.isFinite(pt.ts) && (Number.isFinite(pt.native)||Number.isFinite(pt.usd)))
    .sort((a,b)=>a.ts-b.ts);
}
async function loadExchangeDatasetStable(slug){
  try{
    const url=`https://api.llama.fi/cex/reserves/${encodeURIComponent(slug)}?_t=${Date.now()}`;
    const r=await withTimeout(httpGetWithRetry(url,1,UA),HARD_TIMEOUT_MS,`llama:stable:${slug}`);
    if(r?.data) return r.data;
  }catch{}
  return null;
}
async function loadExchangeDatasetPreview(slug){
  try{
    const url=`https://preview.dl.llama.fi/cex/${encodeURIComponent(slug)}?_t=${Date.now()}`;
    const r=await withTimeout(httpGetWithRetry(url,1,UA),HARD_TIMEOUT_MS,`llama:preview:${slug}`);
    if(r?.data) return r.data;
  }catch{}
  return null;
}
async function loadExchangeDataset(slug){
  return (await loadExchangeDatasetStable(slug))||(await loadExchangeDatasetPreview(slug));
}
async function fetchCexDeltasTwoWindows(slug, symbol){
  const data=await loadExchangeDataset(slug);
  if(!data) return { nowUSD:null, prevUSD:null };
  const series=readSeriesForSymbol(data, symbol);
  if(!series.length) return { nowUSD:null, prevUSD:null };
  const last=series[series.length-1];
  const target1 = last.ts - 24*3600*1000;
  const target2 = last.ts - 48*3600*1000;
  let p1=series[0], b1=Infinity, p2=series[0], b2=Infinity;
  for(const p of series){
    const d1=Math.abs(p.ts-target1); if(d1<b1){b1=d1; p1=p;}
    const d2=Math.abs(p.ts-target2); if(d2<b2){b2=d2; p2=p;}
  }
  const usdNow  = (Number.isFinite(last.usd) && Number.isFinite(p1.usd)) ? (last.usd - p1.usd) : (Number.isFinite(last.native)&&Number.isFinite(p1.native)? (last.native - p1.native):null);
  const usdPrev = (Number.isFinite(p1.usd)   && Number.isFinite(p2.usd)) ? (p1.usd - p2.usd)   : (Number.isFinite(p1.native) && Number.isFinite(p2.native)? (p1.native - p2.native):null);
  return { nowUSD:usdNow, prevUSD:usdPrev, last, p1, p2 };
}
async function loadGlobalAssetSeries(symbol){
  const candidates=[
    `https://api.llama.fi/cex/asset/${encodeURIComponent(symbol)}?_t=${Date.now()}`,
    `https://api.llama.fi/cex/assets/${encodeURIComponent(symbol)}?_t=${Date.now()}`,
    `https://api.llama.fi/cex/reserves/asset/${encodeURIComponent(symbol)}?_t=${Date.now()}`,
    `https://preview.dl.llama.fi/cex/asset/${encodeURIComponent(symbol)}?_t=${Date.now()}`
  ];
  for(const url of candidates){
    try{
      const r=await withTimeout(httpGetWithRetry(url,1,UA),HARD_TIMEOUT_MS,`llama:global:${symbol}`);
      if(r?.data) return r.data;
    }catch{}
  }
  return null;
}
function readGlobalSeries(data){
  const out=[]; const arrays=[];
  if(Array.isArray(data)) arrays.push(data);
  if(Array.isArray(data?.data)) arrays.push(data.data);
  if(Array.isArray(data?.series)) arrays.push(data.series);
  if(Array.isArray(data?.chart)) arrays.push(data.chart);
  if(Array.isArray(data?.charts)) arrays.push(...data.charts);
  for(const arr of arrays){
    for(const p of arr){
      const pt=normPoint(p);
      if(Number.isFinite(pt.ts)&&(Number.isFinite(pt.native)||Number.isFinite(pt.usd))) out.push(pt);
    }
  }
  return out.sort((a,b)=>a.ts-b.ts);
}
function windowDeltasTwo(series, spotUSD){
  if(!series.length) return { now:null, prev:null };
  const last=series[series.length-1];
  const target1 = last.ts - 24*3600*1000;
  const target2 = last.ts - 48*3600*1000;
  let p1=series[0], b1=Infinity, p2=series[0], b2=Infinity;
  for(const p of series){
    const d1=Math.abs(p.ts-target1); if(d1<b1){b1=d1; p1=p;}
    const d2=Math.abs(p.ts-target2); if(d2<b2){b2=d2; p2=p;}
  }
  const toUSD = (valUSD, valNative) => Number.isFinite(valUSD) ? valUSD : (Number.isFinite(valNative)&&Number.isFinite(spotUSD) ? valNative*spotUSD : null);
  const now  = toUSD(last.usd, last.native) - toUSD(p1.usd, p1.native);
  const prev = toUSD(p1.usd,  p1.native)    - toUSD(p2.usd, p2.native);
  return { now: Number.isFinite(now)?now:null, prev: Number.isFinite(prev)?prev:null };
}
const netflowsCache=new Map();
const NETFLOWS_TTL_MS=60*60*1000;
function readNetflowsCache(sym){
  const rec=netflowsCache.get(sym);
  if(!rec) return null;
  if(Date.now()-rec.ts>NETFLOWS_TTL_MS) return null;
  return rec;
}
function writeNetflowsCache(sym, payload){
  netflowsCache.set(sym,{ ts:Date.now(), ...payload });
}
function synthNetFlowsUSD(pct24, vol24){
  if(!SYNTH_FLOWS) return null;
  if(!Number.isFinite(pct24) || !Number.isFinite(vol24)) return null;
  const magnitude = SYNTH_ALPHA * vol24 * Math.abs(pct24)/100;
  const sign = (pct24 < 0) ? +1 : (pct24 > 0 ? -1 : 0);
  if (sign === 0) return 0;
  return sign * magnitude;
}
async function fetchProxyNetFlowsUSDWithPrev(assetKey, spotUSD, pctNow, volNow, pctPrev, volPrev){
  const sym=assetKey==='BTC'?'BTC':(assetKey==='ETH'?'ETH':null);
  if(!sym){ return { nowUSD:null, prevUSD:null, diffUSD:null }; }
  const cached=readNetflowsCache(sym);
  if(cached) return cached;

  let sumNow=0, sumPrev=0, seenNow=0, seenPrev=0;
  const tasks=EXCHANGES.map(async (slug)=>{
    try{
      const r=await withTimeout(fetchCexDeltasTwoWindows(slug,sym),HARD_TIMEOUT_MS,`llama:${slug}:${sym}`);
      const { nowUSD, prevUSD }=r||{};
      if(Number.isFinite(nowUSD)){ sumNow+=nowUSD; seenNow++; }
      if(Number.isFinite(prevUSD)){ sumPrev+=prevUSD; seenPrev++; }
    }catch{}
  });
  await Promise.allSettled(tasks);

  if(seenNow>0 || seenPrev>0){
    const payload = {
      nowUSD: seenNow>0 ? sumNow : null,
      prevUSD: seenPrev>0 ? sumPrev : null,
      diffUSD: (seenNow>0 && seenPrev>0) ? (sumNow - sumPrev) : null
    };
    writeNetflowsCache(sym,payload);
    return payload;
  }

  try{
    const data=await loadGlobalAssetSeries(sym).catch(()=>null);
    if(data){
      const series=readGlobalSeries(data);
      const { now, prev } = windowDeltasTwo(series, spotUSD);
      if(Number.isFinite(now) || Number.isFinite(prev)){
        const payload = {
          nowUSD: Number.isFinite(now)?now:null,
          prevUSD: Number.isFinite(prev)?prev:null,
          diffUSD: (Number.isFinite(now)&&Number.isFinite(prev)) ? (now - prev) : null
        };
        writeNetflowsCache(sym,payload);
        return payload;
      }
    }
  }catch{}

  const synthNow = synthNetFlowsUSD(pctNow,  volNow);
  const synthPrv = synthNetFlowsUSD(pctPrev, volPrev);
  if (Number.isFinite(synthNow) || Number.isFinite(synthPrv)) {
    const payload = {
      nowUSD:  Number.isFinite(synthNow)?synthNow:null,
      prevUSD: Number.isFinite(synthPrv)?synthPrv:null,
      diffUSD: (Number.isFinite(synthNow)&&Number.isFinite(synthPrv)) ? (synthNow - synthPrv) : null
    };
    writeNetflowsCache(sym,payload);
    return payload;
  }

  return { nowUSD:null, prevUSD:null, diffUSD:null };
}

// ---------- FETCH: GOLD ----------
async function fetchGoldSpotAndDelta(){
  try {
    const candidates = [
      `https://api.metals.live/v1/spot/gold?_t=${Date.now()}`,
      `https://api.metals.live/v1/spot/XAU?_t=${Date.now()}`,
      `https://api.metals.live/v1/spot?_t=${Date.now()}`
    ];
    let raw = null;
    for (const url of candidates) {
      try {
        const r = await withTimeout(httpGetWithRetry(url, 1, UA), HARD_TIMEOUT_MS, `gold:${url}`);
        if (Array.isArray(r?.data) && r.data.length) { raw = r.data; break; }
      } catch {}
    }
    if (!Array.isArray(raw) || !raw.length) {
      try {
        const y = await withTimeout(
          httpGetWithRetry(`https://query1.finance.yahoo.com/v8/finance/chart/GC=F?range=2d&interval=1d&_t=${Date.now()}`, 1, UA),
          HARD_TIMEOUT_MS,
          'gold:yahoo'
        );
        const res = y?.data?.chart?.result?.[0];
        const c = Number(res?.meta?.regularMarketPrice);
        const prev = Array.isArray(res?.indicators?.quote?.[0]?.close) ? Number(res.indicators.quote[0].close?.[0]) : null;
        if (Number.isFinite(c) && c>0) {
          const pct = (Number.isFinite(prev) && prev>0) ? ((c - prev) / prev) * 100 : null;
          return { price: c, pct24: Number.isFinite(pct)?pct:null, source: 'XAU' };
        }
      } catch {}
      return { price:null, pct24:null, source:null };
    }

    const nums = [];
    for (const item of raw) {
      if (typeof item === 'number') { if (item > 0) nums.push(item); continue; }
      if (Array.isArray(item)) {
        const v = Number(item[1] ?? item[0]);
        if (Number.isFinite(v) && v > 0) nums.push(v);
        continue;
      }
      if (item && typeof item === 'object') {
        const keys = ['gold','xau','price','ask','bid','close'];
        let v = null;
        for (const k of keys) {
          const n = Number(item[k]);
          if (Number.isFinite(n) && n > 0) { v = n; break; }
        }
        if (v == null) {
          const any = Number(Object.values(item).find(x => Number.isFinite(Number(x))));
          if (Number.isFinite(any) && any > 0) v = any;
        }
        if (v != null) nums.push(v);
      }
    }
    if (nums.length < 1) return { price:null, pct24:null, source:null };

    const price = nums[nums.length - 1];
    let prev = null;
    for (let i = nums.length - 2; i >= 0; i--) {
      const n = nums[i];
      if (Number.isFinite(n) && n > 0 && n !== price) { prev = n; break; }
    }
    const pct = (Number.isFinite(prev) && prev > 0) ? ((price - prev) / prev) * 100 : null;
    return { price, pct24: (Number.isFinite(pct) ? pct : null), source: 'XAU' };
  } catch {
    return { price:null, pct24:null, source:null };
  }
}

// ---------- FGI ----------
async function fetchFearGreedIndex() {
  try {
    const url = `https://api.alternative.me/fng/?limit=1&_t=${Date.now()}`;
    const r = await withTimeout(httpGetWithRetry(url,1,UA),HARD_TIMEOUT_MS,'fgi');
    const item = Array.isArray(r?.data?.data) ? r.data.data[0] : null;
    if (!item) return { value:null, classification:null, ts:null, timeUntilUpdate:null };
    const value = Number(item.value);
    const classification = item.value_classification || null;
    const ts = item.timestamp ? Number(item.timestamp) * 1000 : null;
    const timeUntilUpdate = Number(r?.data?.metadata?.time_until_update ?? null);
    return {
      value: Number.isFinite(value) ? value : null,
      classification,
      ts: Number.isFinite(ts) ? ts : null,
      timeUntilUpdate: Number.isFinite(timeUntilUpdate) ? timeUntilUpdate : null
    };
  } catch {
    return { value:null, classification:null, ts:null, timeUntilUpdate:null };
  }
}

// ---------- RISK / TEXT HELPERS ----------
function priceChangeRisk(pct24h){
  if(typeof pct24h!=='number'||Number.isNaN(pct24h)||pct24h<=0) return 0;
  return Math.min(1,pct24h/10);
}
function aggregateScore({ priceRisk, fundingRisk=0, sentimentRisk=0 }){
  return Math.max(0, Math.min(1, 0.7*priceRisk + 0.2*fundingRisk + 0.1*sentimentRisk));
}
function riskBar(score){
  const n=Math.max(0,Math.min(10,Math.round((score||0)*10)));
  return '🟥'.repeat(n)+'⬜'.repeat(10-n);
}

// ---------- SNAPSHOT CACHE ----------
const snapshotCache = new Map();
function keyForSymbols(symbols){ return String(symbols).toUpperCase(); }
function readSnapshotCache(symbols){
  if (BUST_CACHE) return null;
  const key = keyForSymbols(symbols);
  const rec = snapshotCache.get(key);
  if(!rec) return null;
  if(Date.now() - rec.ts > SNAPSHOT_TTL_MS) return null;
  return rec.payload;
}
function writeSnapshotCache(symbols, payload){
  const key = keyForSymbols(symbols);
  snapshotCache.set(key, { ts: Date.now(), payload });
}
export function invalidateMarketSnapshotCache(){ snapshotCache.clear(); }

// ---------- MAIN SNAPSHOT ----------
export async function getMarketSnapshot(symbols=['BTC','ETH']){
  const cached = readSnapshotCache(symbols);
  if (cached) return cached;

  try{
    const ids=symbols.map(s=>symbolsCfg[s]?.coingecko).filter(Boolean);
    const markets=await fetchCoingeckoMarkets(ids).catch(()=>null);

    const snapshots={};
    for(const s of symbols){
      const id=symbolsCfg[s]?.coingecko;
      const binanceSym=symbolsCfg[s]?.binance;

      let price=null,pct24=null,vol24=null;
      const m=Array.isArray(markets)?markets.find(x=>x.id===id):null;
      if(m){
        price=Number(m.current_price)||null;
        pct24=(typeof m?.price_change_percentage_24h==='number')?Number(m.price_change_percentage_24h):null;
        vol24=(typeof m?.total_volume==='number')?Number(m.total_volume):null;
      }
      if(!Number.isFinite(price)||!Number.isFinite(pct24)||!Number.isFinite(vol24)){
        const t24=await fetchBinanceTicker24h(binanceSym).catch(()=>null);
        if(!Number.isFinite(price)&&Number.isFinite(t24?.price)) price=t24.price;
        if(!Number.isFinite(pct24)&&Number.isFinite(t24?.pct24)) pct24=t24.pct24;
        if(!Number.isFinite(vol24)&&Number.isFinite(t24?.vol24)) vol24=t24.vol24;
      }

      let rsi14=null, rsi14Prev=null, pctPrev=null, volPrev=null, volDeltaPct=null;
      try{
        const chart=await withTimeout(fetchCoingeckoMarketChart(id,16),HARD_TIMEOUT_MS,`coingecko:rsi:${id}`).catch(()=>null);
        const closes=Array.isArray(chart?.prices)?chart.prices.map(p=>Number(p[1])):[];
        if(closes.length>=16){
          rsi14=computeRSI(closes.slice(-15));
          rsi14Prev=computeRSI(closes.slice(-16,-1));
        }
        if (closes.length >= 3) {
          const c2 = closes[closes.length-2];
          const c3 = closes[closes.length-3];
          if (Number.isFinite(c2) && Number.isFinite(c3) && c3 !== 0) {
            pctPrev = ((c2 - c3)/c3)*100;
          }
        }
      }catch{}

      try{
        const chart = await fetchCoingeckoMarketChart(id,3).catch(()=>null);
        const vols = Array.isArray(chart?.total_volumes)?chart.total_volumes.map(p=>Number(p[1])):[];
        if(vols.length>=2 && Number.isFinite(vols[vols.length-2]) && Number.isFinite(vols[vols.length-1])){
          const prev = vols[vols.length-2], last = vols[vols.length-1];
          if (prev > 0) volDeltaPct = (last - prev) / prev * 100;
          volPrev = prev;
        }
      }catch{}

      const flows = await fetchProxyNetFlowsUSDWithPrev(s, price, pct24, vol24, pctPrev, volPrev)
        .catch(()=>({nowUSD:null,prevUSD:null,diffUSD:null}));

      const ls = await fetchLongShortRatio(binanceSym).catch(()=>null);

      const series1 = (s==='PAXG') ? [] : await fetchBinanceFundingSeries(binanceSym, 24).catch(()=>[]);
      let fundingNow=null, fundingPrev=null, fundingDelta=null;

      let src = series1;
      if (s!=='PAXG' && !src.length) {
        const series2 = await fetchFundingSeriesViaWWW(binanceSym, 48).catch(()=>[]);
        if (series2.length) src = series2;
      }
      if (src.length >= 6){
        const nowArr = src.slice(-3);
        const prevArr = src.slice(-6,-3);
        const avg = (arr)=>arr.reduce((a,b)=>a+b,0)/arr.length;
        fundingNow = avg(nowArr);
        fundingPrev = avg(prevArr);
        fundingDelta = fundingNow - fundingPrev;
      } else if (src.length >= 3){
        fundingNow = (src.slice(-3).reduce((a,b)=>a+b,0))/3;
      }

      if (!Number.isFinite(fundingNow) && s!=='PAXG') {
        const fn = await fetchFundingNowFallback(binanceSym).catch(()=>null);
        if (Number.isFinite(fn)) { fundingNow = fn; fundingPrev = null; fundingDelta = null; }
      }

      if (nearZero(fundingNow))  fundingNow = null;
      if (nearZero(fundingPrev)) fundingPrev = null;
      if (nearZero(fundingDelta)) fundingDelta = null;

      const priceRisk=Number.isFinite(pct24)?priceChangeRisk(pct24):0;
      const fundingRisk = Number.isFinite(fundingNow) ? Math.min(1, Math.abs(fundingNow)*10_000/50) : 0;
      const sentimentRisk = Number.isFinite(ls?.longPct) ? Math.max(0, (ls.longPct-60)/40) : 0;
      const score=aggregateScore({ priceRisk, fundingRisk, sentimentRisk });

      snapshots[s]={
        symbol:s, price, pct24,
        vol24, volPrev, volDeltaPct,
        rsi14, rsi14Prev,
        fundingNow, fundingPrev, fundingDelta,
        netFlowsUSDNow: flows.nowUSD,
        netFlowsUSDPrev: flows.prevUSD,
        netFlowsUSDDiff: flows.diffUSD,
        longShort: ls,
        score,
        fgiValue: null,
        fgiClass: null,
        fgiTs: null
      };
    }

    const goldInfo=await fetchGoldSpotAndDelta().catch(()=>({ price:null, pct24:null, source:null }));
    for(const s of symbols){
      snapshots[s].goldPrice=goldInfo.price;
      snapshots[s].goldPct24=goldInfo.pct24;
      snapshots[s].goldSource=goldInfo.source;
    }

    const fgi = await fetchFearGreedIndex().catch(()=>({ value:null, classification:null, ts:null, timeUntilUpdate:null }));
    for (const s of symbols) {
      snapshots[s].fgiValue = fgi.value;
      snapshots[s].fgiClass = fgi.classification;
      snapshots[s].fgiTs = fgi.ts;
    }

    const payload = { ok:true, snapshots, fetchedAt:Date.now() };
    writeSnapshotCache(symbols, payload);
    return payload;
  }catch(e){
    return { ok:false, error:String(e?.message||e) };
  }
}

// ---------- COPY / TEXT ----------
function guidePriceOne(pct, isEn){
  if(!Number.isFinite(pct)) return isEn?'No clear price signal.':'Сигнал цены неясен.';
  if(Math.abs(pct)<1) return isEn?'Wait for confirmations.':'Ждать подтверждений.';
  if(pct>0) return isEn?'Hold/slow DCA; avoid adding leverage.':'Держать/аккуратно усреднять; не добавлять плечо.';
  return isEn?'Avoid knife catching; wait for reversal signs.':'Не ловить ножи; ждать признаков разворота.';
}
function guideVolOne(dPct,isEn){
  if(!Number.isFinite(dPct)) return isEn?'Volume signal unclear.':'Сигнал по объёму неясен.';
  if(dPct>3) return isEn?'Rising volume confirms the move.':'Рост объёма подтверждает движение.';
  if(dPct<-3) return isEn?'Fading volume — caution.':'Объёмы слабеют — осторожно.';
  return isEn?'Neutral volume.':'Объёмы нейтральны.';
}
function guideRSIOne(v,isEn){
  if(!Number.isFinite(v)) return isEn?'No RSI data.':'Данных RSI нет.';
  if(v>=70) return isEn?'Overbought risk — tighten risk.':'Риск перекупленности — ужать риск.';
  if(v<=30) return isEn?'Oversold zone — look for reversals.':'Перепроданность — искать разворот.';
  return isEn?'Momentum neutral.':'Импульс нейтральный.';
}
function guideFlowsOne(usd,isEn){
  if(!Number.isFinite(usd)) return isEn?'No flows signal.':'Сигнал по потокам отсутствует.';
  if(usd>0) return isEn?'Net inflow → possible sell pressure.':'Приток → возможное давление продажи.';
  if(usd<0) return isEn?'Net outflow → supportive.':'Отток → поддержка.';
  return isEn?'Flows flat.':'Потоки нейтральны.';
}
function guideFundingOne(f,isEn){
  if(!Number.isFinite(f)) return isEn?'No funding data.':'Данных по funding нет.';
  if(Math.abs(f)>0.0003) return isEn?'Elevated funding — mind leverage.':'Повышенный funding — аккуратно с плечом.';
  return isEn?'Funding moderate.':'Funding умеренный.';
}
function guideLSOne(longPct,isEn){
  if(!Number.isFinite(longPct)) return isEn?'No LS data.':'Данных по LS нет.';
  if(longPct>65) return isEn?'Longs crowded — beware squeeze.':'Лонги перегружены — риск сквиза.';
  if(longPct<45) return isEn?'Shorts crowded — squeeze risk.':'Шорты перегружены — риск шорт-сквиза.';
  return isEn?'Positioning balanced.':'Позиционирование сбалансировано.';
}
function guideGoldOne(pct,isEn){
  if(!Number.isFinite(pct)) return isEn?'No gold cue.':'Сигнала по золоту нет.';
  if(pct>0.5) return isEn?'Risk-off tilt (gold up).':'Уклон в risk-off (золото растёт).';
  if(pct<-0.5) return isEn?'Benign macro (gold down).':'Мягкий макрофон (золото слабеет).';
  return isEn?'Macro neutral.':'Макро нейтрально.';
}
function actionByRisk(score,isEn){
  if (!Number.isFinite(score)) score = 0;
  if (score < 0.10) return isEn?'Hold/slow DCA; do not add leverage.':'Держать/аккуратно усреднять; плечо не добавлять.';
  if (score < 0.30) return isEn?'Enter only on confirmations; tighter stops; no extra leverage.':'Вход только по подтверждениям; стопы ближе; плечо не добавлять.';
  if (score < 0.60) return isEn?'Cut leverage; trail stops; take partial profits.':'Сократить плечо; тянуть стопы; частично фиксировать.';
  return isEn?'Avoid aggressive longs; reduce exposure.':'Избегать агрессивных лонгов; снижать экспозицию.';
}
function flowsDeltaText(prev, now, diffPct, isEn){
  if (!Number.isFinite(prev) || !Number.isFinite(now) || !Number.isFinite(diffPct)) return '';
  const prevInflow = prev > 0, nowInflow = now > 0;
  const more = isEn ? 'more' : 'больше';
  const less = isEn ? 'less' : 'меньше';
  const inflows = isEn ? 'inflows' : 'притоков';
  const outflows = isEn ? 'outflows' : 'оттоков';
  if (prevInflow === nowInflow) {
    const word = nowInflow ? inflows : outflows;
    const trend = Math.abs(now) > Math.abs(prev) ? more : less;
    return `${trend} ${word}`;
  }
  if (nowInflow && !prevInflow) return isEn ? 'shift to inflows' : 'смена на притоки';
  if (!nowInflow && prevInflow) return isEn ? 'shift to outflows' : 'смена на оттоки';
  return '';
}
function renderLsBlock(ls, isEn, label){
  const lbl = label || (isEn ? 'Asset' : 'Актив');
  if (!ls || !Number.isFinite(ls.longPct) || !Number.isFinite(ls.shortPct)) return `${lbl}: —`;
  const greens = Math.max(0, Math.min(10, Math.round(ls.longPct/10)));
  const reds   = 10 - greens;
  const bar = '🟩'.repeat(greens) + '🟥'.repeat(reds);
  if (isEn) return `${lbl}:\n• ${B('Longs')} ${B(`${ls.longPct}%`)} | ${B('Shorts')} ${B(`${ls.shortPct}%`)}\n${bar}`;
  return `${lbl}:\n• ${B('Лонги')} ${B(`${ls.longPct}%`)} | ${B('Шорты')} ${B(`${ls.shortPct}%`)}\n${bar}`;
}
function fearGreedBar(v){
  const val = Number(v);
  const n = Math.max(0, Math.min(10, Math.round(val / 10)));
  if (!Number.isFinite(val)) return '⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜';
  if (val < 50) return '🟥'.repeat(n) + '⬜'.repeat(10 - n);
  return '🟩'.repeat(n) + '⬜'.repeat(10 - n);
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

// ---------- RENDER ----------
export async function buildMorningReportHtml(snapshots, lang='ru'){
  const isEn=String(lang).toLowerCase().startsWith('en');

  const T=isEn?{
    report:'📊 report',
    price:'Price *¹',
    fgi:'Fear & Greed *²',
    volumes:'Volumes (24h) *³',
    rsi:'RSI (14) *⁴',
    flows:'Inflows / outflows *⁵',
    funding:'Funding rate (avg) *⁶',
    ls:'Longs vs Shorts *⁷',
    gold:'Gold *⁸',
    risks:'Risks *⁹',
    over24h:'over 24h',
    ref:'Reference',
    prev24:'vs prev 24h',
    introPrice:'¹ Price: spot.',
    introFGI:'² Bitcoin market sentiment (0 fear → 100 greed).',
    introVol:'³ 24h volume: rising volume confirms trend.',
    introRSI:'⁴ RSI(14): momentum; ~70/30 — risk/opportunity zones.',
    introFlows:'⁵ Net flows: inflow = potential sell pressure; outflow = supportive.',
    introFunding:'⁶ Funding: positive → longs pay; parentheses show delta and bps.',
    introLS:'⁷ Long/Short: share of accounts (Binance Futures), L/S > 1 — longs dominate.',
    introGold:'⁸ Gold: XAU/USD spot; % is daily change.',
    introRisk:'⁹ Risk: 0–100%, blend of price, funding, and L/S.'
  }:{
    report:'📊 отчет',
    price:'Цена *¹',
    fgi:'Индекс страха и жадности *²',
    volumes:'Объёмы (24h) *³',
    rsi:'RSI (14) *⁴',
    flows:'Притоки / оттоки *⁵',
    funding:'Funding rate (avg) *⁶',
    ls:'Лонги vs Шорты *⁷',
    gold:'Золото *⁸',
    risks:'Риски *⁹',
    over24h:'за 24 часа',
    ref:'Справка',
    prev24:'к предыдущим 24ч',
    introPrice:'¹ Цена: спот.',
    introFGI:'² Рыночные настроения по BTC (0 страх → 100 жадность).',
    introVol:'³ Объём 24ч: рост объёма подтверждает движение.',
    introRSI:'⁴ RSI(14): импульс; ~70/30 — зоны риска/возможностей.',
    introFlows:'⁵ Net flows: приток = возможное давление продажи; отток = поддержка.',
    introFunding:'⁶ Funding: положит. → лонги платят; в скобках — дельта и б.п.',
    introLS:'⁷ Лонги/Шорты: доля аккаунтов (Binance Futures), L/S > 1 — лонги преобладают.',
    introGold:'⁸ Золото: спот XAU/USD; % — суточное изменение.',
    introRisk:'⁹ Риск: 0–100%, смесь цены, funding и баланса L/S.'
  };

  const priceLine = (sym) => {
    const pct = Number(sym?.pct24);
    const circ = circleByDelta(pct);
    const pctTxt = Number.isFinite(pct) ? `${circ} (${B(`${pct>0?'+':''}${pct.toFixed(2)}%`)} ${T.over24h})` : '(—)';
    const p = Number.isFinite(sym?.price) ? `$${humanFmt(sym.price)}` : '—';
    return `${B(p)} ${pctTxt}`;
  };

  const fgiLine = (sym) => {
    const v = Number(sym?.fgiValue);
    const clsRaw = sym?.fgiClass || null;
    if (!Number.isFinite(v)) return '—';
    const cls = translateFgiClass(clsRaw, isEn);
    const bar = fearGreedBar(v);
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

  const flowsLine = (sym) => {
    const now = Number(sym?.netFlowsUSDNow);
    const prev = Number(sym?.netFlowsUSDPrev);
    const diff = Number(sym?.netFlowsUSDDiff);
    if(!Number.isFinite(now) && !Number.isFinite(prev)) return '—';
    const sNowMoney = Number.isFinite(now) ? `${now>=0?'+':'−'}$${humanFmt(Math.abs(now))}` : '—';
    const sNowAbbr  = Number.isFinite(now) ? `${now>=0?'+':'−'}${abbrevWithUnit(Math.abs(now), isEn)}` : '';
    let deltaPart = '';
    if (Number.isFinite(prev) && Math.abs(prev) > 0 && Number.isFinite(diff)) {
      const diffPct = (diff/Math.abs(prev))*100;
      if (Number.isFinite(diffPct)) {
        const circ = circleByDelta(diffPct);
        const phrase = flowsDeltaText(prev, now, diffPct, isEn);
        deltaPart = ` ${circ}(${B(pctStr(diffPct))} ${T.prev24}${phrase ? `, ${B(phrase)}` : ''})`;
      }
    }
    return `${B(`${sNowMoney}`)} (${B(sNowAbbr)})${deltaPart}`;
  };

  const riskBarStr = (sym) => {
    const score = Number.isFinite(sym?.score) ? sym.score : 0;
    const bar = riskBar(score);
    const pct = `${Math.round(score*100)}%`;
    return { bar: `${bar} ${B(pct)}`, score };
  };

  const lines=[];
  lines.push(T.report);
  lines.push('');

  // PRICE (BTC, ETH, PAXG c подписью)
  lines.push(U(T.price));
  const priceSymbols = ['BTC','ETH','PAXG'].filter(k => snapshots && snapshots[k]);
  for (const key of priceSymbols) {
    const label = (key==='PAXG') ? (isEn ? ' (tokenized gold)' : ' (токенизированное золото)') : '';
    lines.push(`${key}${label} ${priceLine((snapshots[key])||{})}`);
  }
  lines.push('');

  lines.push(U(T.fgi));
  lines.push(`• ${fgiLine((snapshots.BTC)||{})}`);
  lines.push('');

  lines.push(U(T.volumes));
  lines.push(`• BTC: ${volumeLine((snapshots.BTC)||{})}`);
  lines.push(`• ETH: ${volumeLine((snapshots.ETH)||{})}`);
  lines.push('');

  lines.push(U(T.rsi));
  lines.push(`• BTC: ${rsiLine((snapshots.BTC)||{})}`);
  lines.push(`• ETH: ${rsiLine((snapshots.ETH)||{})}`);
  lines.push('');

  lines.push(U(T.flows));
  lines.push(`• BTC: ${flowsLine((snapshots.BTC)||{})}`);
  lines.push(`• ETH: ${flowsLine((snapshots.ETH)||{})}`);
  lines.push('');

  lines.push(U(T.funding));
  lines.push(`• BTC: ${fundingLine((snapshots.BTC)||{})}`);
  lines.push(`• ETH: ${fundingLine((snapshots.ETH)||{})}`);
  lines.push('');

  lines.push(U(T.ls));
  lines.push(renderLsBlock(((snapshots.BTC)||{}).longShort, isEn, 'BTC'));
  lines.push(renderLsBlock(((snapshots.ETH)||{}).longShort, isEn, 'ETH'));
  lines.push('');

  lines.push(U(T.risks));
  const rB = riskBarStr((snapshots.BTC)||{});
  const rE = riskBarStr((snapshots.ETH)||{});
  lines.push(`• BTC:\n${rB.bar}`);
  lines.push(`• ETH:\n${rE.bar}`);
  lines.push('');

  lines.push(T.ref);

  const priceNow = isEn
    ? `Now: BTC — ${guidePriceOne(((snapshots.BTC)||{}).pct24,true)}; ETH — ${guidePriceOne(((snapshots.ETH)||{}).pct24,true)}; PAXG — ${guidePriceOne(((snapshots.PAXG)||{}).pct24,true)}.`
    : `Сейчас: BTC — ${guidePriceOne(((snapshots.BTC)||{}).pct24,false)}; ETH — ${guidePriceOne(((snapshots.ETH)||{}).pct24,false)}; PAXG — ${guidePriceOne(((snapshots.PAXG)||{}).pct24,false)}.`;
  const fgiNow = isEn ? `${T.introFGI}` : `${T.introFGI}`;
  const volNow = isEn
    ? `Now: BTC — ${guideVolOne(((snapshots.BTC)||{}).volDeltaPct,true)}; ETH — ${guideVolOne(((snapshots.ETH)||{}).volDeltaPct,true)}.`
    : `Сейчас: BTC — ${guideVolOne(((snapshots.BTC)||{}).volDeltaPct,false)}; ETH — ${guideVolOne(((snapshots.ETH)||{}).volDeltaPct,false)}.`;
  const rsiNow = isEn
    ? `Now: BTC — ${guideRSIOne(((snapshots.BTC)||{}).rsi14,true)}; ETH — ${guideRSIOne(((snapshots.ETH)||{}).rsi14,true)}.`
    : `Сейчас: BTC — ${guideRSIOne(((snapshots.BTC)||{}).rsi14,false)}; ETH — ${guideRSIOne(((snapshots.ETH)||{}).rsi14,false)}.`;
  const flowsNow = isEn
    ? `Now: BTC — ${guideFlowsOne(((snapshots.BTC)||{}).netFlowsUSDNow,true)}; ETH — ${guideFlowsOne(((snapshots.ETH)||{}).netFlowsUSDNow,true)}.`
    : `Сейчас: BTC — ${guideFlowsOne(((snapshots.BTC)||{}).netFlowsUSDNow,false)}; ETH — ${guideFlowsOne(((snapshots.ETH)||{}).netFlowsUSDNow,false)}.`;
  const fundingNow = isEn
    ? `Now: BTC — ${guideFundingOne(((snapshots.BTC)||{}).fundingNow,true)}; ETH — ${guideFundingOne(((snapshots.ETH)||{}).fundingNow,true)}.`
    : `Сейчас: BTC — ${guideFundingOne(((snapshots.BTC)||{}).fundingNow,false)}; ETH — ${guideFundingOne(((snapshots.ETH)||{}).fundingNow,false)}.`;
  const lsNow = isEn
    ? `Now: BTC — ${guideLSOne(((snapshots.BTC)||{}).longShort?.longPct,true)}; ETH — ${guideLSOne(((snapshots.ETH)||{}).longShort?.longPct,true)}.`
    : `Сейчас: BTC — ${guideLSOne(((snapshots.BTC)||{}).longShort?.longPct,false)}; ETH — ${guideLSOne(((snapshots.ETH)||{}).longShort?.longPct,false)}.`;
  const goldNow = isEn
    ? `Now: ${guideGoldOne(((snapshots.BTC)||{}).goldPct24,true)}`
    : `Сейчас: ${guideGoldOne(((snapshots.BTC)||{}).goldPct24,false)}`;
  const riskNow = isEn
    ? `Now: BTC — ${actionByRisk(rB.score,true)}; ETH — ${actionByRisk(rE.score,true)}.`
    : `Сейчас: BTC — ${actionByRisk(rB.score,false)}; ETH — ${actionByRisk(rE.score,false)}.`;

  lines.push(`${T.introPrice} ${priceNow}`);
  lines.push(fgiNow);
  lines.push(`${T.introVol} ${volNow}`);
  lines.push(`${T.introRSI} ${rsiNow}`);
  lines.push(`${T.introFlows} ${flowsNow}`);
  lines.push(`${T.introFunding} ${fundingNow}`);
  lines.push(`${T.introLS} ${lsNow}`);
  // introGold опционально можно убрать, если не нужно в "Справке"
  lines.push(`${T.introRisk} ${riskNow}`);

  return lines.join('\n');
}

// ---------- PUBLIC API ----------
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

  const [ruHtml, enHtml] = await Promise.all([
    buildMorningReportHtml(snap.snapshots, 'ru'),
    buildMorningReportHtml(snap.snapshots, 'en')
  ]);

  let delivered = 0;
  for (let i = 0; i < recipients.length; i += batchSize) {
    const chunk = recipients.slice(i, i + batchSize);
    await Promise.all(chunk.map(async (u) => {
      try {
        const lang = await resolveUserLang(u.userId).catch(() => u.lang || 'ru');
        const html = String(lang || '').toLowerCase().startsWith('en') ? enHtml : ruHtml;
        await bot.telegram.sendMessage(u.userId, html, { parse_mode:'HTML' });
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
  const html=await buildMorningReportHtml(snap.snapshots, lang);
  await bot.telegram.sendMessage(userId, html, { parse_mode:'HTML' });
  return { ok:true };
}
