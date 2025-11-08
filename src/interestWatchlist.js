// src/interestWatchlist.js
import axios from 'axios';

const FAPI_BASES = [
  'https://fapi.binance.com',
  'https://fapi1.binance.com',
  'https://fapi2.binance.com',
  'https://fapi3.binance.com'
];

const http = axios.create({
  timeout: 10000,
  headers: { 'User-Agent': 'crypto-alert-bot/1.0', 'Accept': 'application/json' }
});

let cache = { at: 0, n: 0, list: [] };
const TTL_MS = 5 * 60 * 1000;

let exchCache = { at: 0, set: null };
const EXCH_TTL_MS = 30 * 60 * 1000;

function shortBody(x) {
  if (x == null) return '';
  if (typeof x === 'string') return x.slice(0, 120);
  try { return JSON.stringify(x).slice(0, 120) } catch { return '' }
}

async function get24hrAll() {
  for (const base of FAPI_BASES) {
    const t0 = Date.now();
    try {
      const { data } = await http.get(`${base}/fapi/v1/ticker/24hr`);
      const ms = Date.now() - t0;
      if (Array.isArray(data) && data.length) {
        console.log(`[watchlist:t24] OK ${base} ${ms}ms items=${data.length}`);
        return data;
      }
      console.log(`[watchlist:t24] WARN ${base} ${ms}ms empty`);
    } catch (e) {
      const ms = Date.now() - t0;
      const st = e?.response?.status ?? '—';
      console.log(`[http] ERROR GET ${base}/fapi/v1/ticker/24hr code=${e?.code ?? '—'} status=${st} ${ms}ms ${shortBody(e?.response?.data)}`);
    }
  }
  return [];
}

async function getExchangeInfoUSDTPerpSet() {
  const now = Date.now();
  if (exchCache.set && now - exchCache.at < EXCH_TTL_MS) return exchCache.set;
  for (const base of FAPI_BASES) {
    const t0 = Date.now();
    try {
      const { data } = await http.get(`${base}/fapi/v1/exchangeInfo`);
      const ms = Date.now() - t0;
      const arr = Array.isArray(data?.symbols) ? data.symbols : [];
      const filt = arr.filter(s =>
        s?.status === 'TRADING' &&
        s?.contractType === 'PERPETUAL' &&
        s?.quoteAsset === 'USDT' &&
        typeof s?.symbol === 'string'
      );
      const set = new Set(filt.map(s => s.symbol));
      exchCache = { at: now, set };
      console.log(`[watchlist:exch] OK ${base} ${ms}ms symbols=${arr.length} usdt_perp=${set.size}`);
      return set;
    } catch (e) {
      const ms = Date.now() - t0;
      const st = e?.response?.status ?? '—';
      console.log(`[http] ERROR GET ${base}/fapi/v1/exchangeInfo code=${e?.code ?? '—'} status=${st} ${ms}ms ${shortBody(e?.response?.data)}`);
    }
  }
  console.log('[watchlist:exch] WARN all_bases_failed using empty set');
  return new Set();
}

export async function getWatchlistTopN(n = 50) {
  const now = Date.now();
  if (cache.list.length && cache.n >= n && now - cache.at < TTL_MS) {
    console.log(`[watchlist] cache_hit n=${n} age=${Math.round((now - cache.at)/1000)}s`);
    return cache.list.slice(0, n);
  }

  console.log(`[watchlist] build n=${n}`);
  const [arr, valid] = await Promise.all([get24hrAll(), getExchangeInfoUSDTPerpSet()]);
  const before = arr.length;

  const cleaned = arr
    .filter(x => typeof x?.symbol === 'string' && x.symbol.endsWith('USDT'))
    .filter(x => /^[A-Z0-9]{2,20}USDT$/.test(x.symbol));

  const filtered = valid && valid.size ? cleaned.filter(x => valid.has(x.symbol)) : cleaned;
  const top = filtered
    .map(x => ({ sym: x.symbol, qv: Number(x?.quoteVolume) || 0 }))
    .sort((a, b) => b.qv - a.qv)
    .slice(0, n)
    .map(x => x.sym.replace(/USDT$/, ''));

  cache = { at: now, n, list: top };
  console.log(`[watchlist] built top=${top.length} from=${before} cleaned=${cleaned.length} filtered=${filtered.length} ttl=300s`);
  return top;
}
