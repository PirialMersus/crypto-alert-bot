// src/pricing.js
import axios from 'axios';
import { AXIOS_TIMEOUT, AXIOS_RETRIES, TICKERS_TTL } from './config.js';

const http = axios.create({ timeout: AXIOS_TIMEOUT, headers: { 'User-Agent': 'crypto-alert-bot/1.0' } });

let tickersCache = { time: 0, map: new Map() };
const pricesCache = new Map();
const pricePromises = new Map();

async function httpGetWithRetry(url, retries = AXIOS_RETRIES) {
  let attempt = 0;
  let lastErr = null;
  while (attempt <= retries) {
    try { return await http.get(url); }
    catch (e) { lastErr = e; await new Promise(r => setTimeout(r, Math.min(500 * 2 ** attempt, 2000))); attempt++; }
  }
  throw lastErr;
}

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
    tickersCache = { time: Date.now(), map };
    return map;
  } catch (e) {
    console.error('refreshAllTickers error', e?.message || e);
    return tickersCache.map;
  }
}

async function getPriceLevel1(symbol) {
  const cached = pricesCache.get(symbol);
  if (cached && (Date.now() - cached.time) < 20_000) return cached.price;
  if (pricePromises.has(symbol)) return await pricePromises.get(symbol);

  const p = httpGetWithRetry(`https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${symbol}`)
    .then(res => {
      const price = Number(res?.data?.data?.price);
      if (Number.isFinite(price)) { pricesCache.set(symbol, { price, time: Date.now() }); return price; }
      return null;
    })
    .catch(err => { console.error('getPriceLevel1 error', symbol, err?.message || err); return null; })
    .finally(() => pricePromises.delete(symbol));

  pricePromises.set(symbol, p);
  return await p;
}

export async function getPriceFast(symbol) {
  if (tickersCache.map.has(symbol) && (Date.now() - tickersCache.time) < TICKERS_TTL * 2) {
    if (Date.now() - tickersCache.time >= TICKERS_TTL) refreshAllTickers().catch(()=>{});
    return tickersCache.map.get(symbol);
  }
  const lvl1 = await getPriceLevel1(symbol);
  refreshAllTickers().catch(()=>{});
  return lvl1;
}
