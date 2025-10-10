// src/prices.js
import { httpGetWithRetry } from './httpClient.js';
import { tickersCache, pricesCache } from './cache.js';
import { TICKERS_TTL, TICKERS_REFRESH_INTERVAL } from './constants.js';
import { pingHealthchecksOnce } from './healthchecks-pinger.js';
import { isDbConnected } from './db.js';
const pricePromises = new Map();
export async function refreshAllTickers() {
  const now = Date.now();
  if (now - tickersCache.time < TICKERS_TTL && tickersCache.map.size) return tickersCache.map;
  try {
    const res = await httpGetWithRetry('https://api.kucoin.com/api/v1/market/allTickers');
    const list = res?.data?.data?.ticker || [];
    const map = new Map();
    for (const t of list) {
      const p = t?.last ? Number(t.last) : NaN;
      if (t?.symbol && Number.isFinite(p)) map.set(t.symbol, p);
    }
    tickersCache.time = Date.now();
    tickersCache.map = map;
    try {
      if (typeof isDbConnected === 'function' && isDbConnected()) {
        pingHealthchecksOnce().catch(()=>{});
      }
    } catch (e) {
      try { console.warn('[healthchecks] isDbConnected check failed', e?.message || e); } catch (ee) {}
    }
    return map;
  } catch (e) { console.error('refreshAllTickers error:', e?.message || e); return tickersCache.map; }
}
export async function getPriceLevel1(symbol) {
  const cached = pricesCache.get(symbol);
  if (cached && (Date.now() - cached.time) < 20_000) return cached.price;
  if (pricePromises.has(symbol)) return await pricePromises.get(symbol);
  const p = httpGetWithRetry(`https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${symbol}`)
    .then(res => { const price = Number(res?.data?.data?.price); if (Number.isFinite(price)) { pricesCache.set(symbol, { price, time: Date.now() }); return price; } return null; })
    .catch(err => { console.error('getPriceLevel1 error for', symbol, err?.message || err); return null; })
    .finally(() => pricePromises.delete(symbol));
  pricePromises.set(symbol, p);
  return await p;
}
export async function getCachedPrice(symbol) {
  const cached = tickersCache.map.get(symbol);
  if (Number.isFinite(cached)) return cached;
  return await getPriceLevel1(symbol);
}
export function startTickersRefresher() {
  refreshAllTickers().catch(err => console.warn('initial refreshAllTickers failed', err?.message || err));
  setInterval(async () => { try { await refreshAllTickers(); } catch (e) { console.warn('scheduled refreshAllTickers failed', e?.message || e); } }, TICKERS_REFRESH_INTERVAL);
}
