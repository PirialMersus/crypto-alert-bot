import axios from 'axios'
import dotenv from 'dotenv'
dotenv.config()

const AXIOS_TIMEOUT = process.env.AXIOS_TIMEOUT_MS ? parseInt(process.env.AXIOS_TIMEOUT_MS, 10) : 10000

// Хосты, которые всегда гоняем через воркер (если PROXY_FETCH задан)
const NEED_PROXY = [
  'api.binance.com',
  'fapi.binance.com',
  'dapi.binance.com',
  'www.binance.com',
  'api.coingecko.com',
  'query1.finance.yahoo.com',
  'data-asg.goldprice.org',
  'api.metals.live',
  'stooq.pl'
];

const ALWAYS_PROXY = String(process.env.ALWAYS_PROXY || '0') === '1';

function isAlreadyProxied(u) {
  try {
    const p = process.env.PROXY_FETCH;
    if (!p) return false;
    const target = new URL(u);
    const proxy = new URL(p);
    // прямой вызов на воркер
    if (target.origin === proxy.origin) return true;
    // воркер с параметром ?url=
    if (target.searchParams?.has('url') && target.origin === proxy.origin) return true;
  } catch {}
  return false;
}

function proxiedUrl(u) {
  try {
    const url = new URL(u);
    const p = process.env.PROXY_FETCH;
    if (!p) return u;
    if (isAlreadyProxied(u)) return u; // защита от «двойного проксирования»
    if (ALWAYS_PROXY) return p + '?url=' + encodeURIComponent(u);
    if (!NEED_PROXY.includes(url.hostname)) return u;
    return p + '?url=' + encodeURIComponent(u);
  } catch {
    return u;
  }
}

export const httpClient = axios.create({
  timeout: AXIOS_TIMEOUT,
  headers: {
    'User-Agent': 'crypto-alert-bot/1.0',
    'Accept': 'application/json'
  }
})

export async function httpGetWithRetry(url, retries = 2, opts = {}) {
  let attempt = 0, lastErr = null
  while (attempt <= retries) {
    try {
      return await httpClient.get(proxiedUrl(url), opts)
    } catch (e) {
      lastErr = e
      const delay = Math.min(500 * Math.pow(1.6, attempt), 4000)
      await new Promise(r => setTimeout(r, delay))
      attempt++
    }
  }
  throw lastErr
}
