// src/oiCvd.js
import { httpGetWithRetry } from './httpClient.js';

const OI_CVD_PERIOD = process.env.OI_CVD_PERIOD || '5m';
const OI_CVD_LIMIT  = process.env.OI_CVD_LIMIT ? parseInt(process.env.OI_CVD_LIMIT, 10) : 6;

const FAPI_BASES = [
  'https://fapi.binance.com',
  'https://fapi1.binance.com',
  'https://fapi2.binance.com',
  'https://fapi3.binance.com',
];

const FUT_MAP = {
  BTC: 'BTCUSDT',
  ETH: 'ETHUSDT',
  SOL: 'SOLUSDT',
  BNB: 'BNBUSDT',
  XRP: 'XRPUSDT',
  DOGE: 'DOGEUSDT',
  TON: 'TONUSDT',
};
const toBinance = (s) => FUT_MAP[String(s).toUpperCase()] || `${String(s).toUpperCase()}USDT`;
const num = (v) => { const n = Number(v); return Number.isFinite(n) ? n : NaN; };

function totalWindowLabel(period, limit) {
  const mp = { '1m':1,'3m':3,'5m':5,'15m':15,'30m':30,'1h':60,'2h':120,'4h':240,'6h':360,'12h':720,'1d':1440 };
  const total = (mp[period] || 0) * (limit || 0);
  if (!total) return `${limit}×${period}`;
  if (total % 60 === 0) return `${total/60}ч`;
  return `${total}м`;
}

async function getJsonWithFallback(pathAndQuery, logTag) {
  let lastErr = null;
  for (const base of FAPI_BASES) {
    const url = `${base}${pathAndQuery}${pathAndQuery.includes('?') ? '&' : '?'}_=${Date.now()}`;
    try {
      const t0 = Date.now();
      const res = await httpGetWithRetry(url, 1, {
        headers: {
          'Accept': 'application/json',
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36',
          'Cache-Control': 'no-cache',
          'Pragma': 'no-cache',
        }
      });
      const ms = Date.now() - t0;
      const data = res?.data;
      if (Array.isArray(data)) {
        console.log(`[oiCvd:${logTag}] OK ${base} ${ms}ms len=${data.length}`);
        return data;
      }
      if (data && typeof data === 'object') {
        console.warn(`[oiCvd:${logTag}] WARN ${base} ${ms}ms object`, data);
        lastErr = new Error(data?.msg || 'Non-array response');
        continue;
      }
      console.warn(`[oiCvd:${logTag}] WARN ${base} ${ms}ms unexpected`, typeof data);
      lastErr = new Error('Unexpected response');
    } catch (e) {
      lastErr = e;
      console.warn(`[oiCvd:${logTag}] FAIL ${base}: ${e?.message || e}`);
    }
  }
  throw lastErr || new Error('All endpoints failed');
}

async function fetchOpenInterest(symbol, period = OI_CVD_PERIOD, limit = OI_CVD_LIMIT) {
  const sym = toBinance(symbol);
  const path = `/futures/data/openInterestHist?symbol=${encodeURIComponent(sym)}&period=${encodeURIComponent(period)}&limit=${encodeURIComponent(String(limit))}`;
  const rows = await getJsonWithFallback(path, `oi:${sym}:${period}x${limit}`);
  const series = rows.map(r => ({
    ts:  num(r?.timestamp),
    usd: num(r?.sumOpenInterestValue),
  })).filter(r => Number.isFinite(r.usd));
  if (!series.length) {
    console.warn(`[oiCvd] OI empty for ${sym} (${period}×${limit})`);
    return { oiChangePct: NaN, oiNowUsd: NaN };
  }
  const first = series[0].usd;
  const last  = series[series.length - 1].usd;
  const oiChangePct = (Number.isFinite(first) && first > 0) ? ((last - first) / first * 100) : NaN;
  return { oiChangePct, oiNowUsd: last };
}

async function fetchCvd(symbol, period = OI_CVD_PERIOD, limit = OI_CVD_LIMIT) {
  const sym = toBinance(symbol);
  const path = `/futures/data/takerlongshortRatio?symbol=${encodeURIComponent(sym)}&period=${encodeURIComponent(period)}&limit=${encodeURIComponent(String(limit))}`;
  const rows = await getJsonWithFallback(path, `cvd:${sym}:${period}x${limit}`);
  const series = rows.map(r => ({
    buy:  num(r?.buyVol),
    sell: num(r?.sellVol),
  })).filter(r => Number.isFinite(r.buy) && Number.isFinite(r.sell));
  if (!series.length) {
    console.warn(`[oiCvd] TakerVol empty for ${sym} (${period}×${limit})`);
    return { cvd: NaN, deltaLast: NaN, sumBuy: NaN, sumSell: NaN };
  }
  let acc = 0, sumBuy = 0, sumSell = 0;
  for (const x of series) { acc += (x.buy - x.sell); sumBuy += x.buy; sumSell += x.sell; }
  const last = series[series.length - 1];
  return { cvd: acc, deltaLast: last.buy - last.sell, sumBuy, sumSell };
}

function verdictBy(oiPct, cvd) {
  if (!Number.isFinite(oiPct) || !Number.isFinite(cvd)) return { emoji: '⚪️', text: 'нет данных' };
  const oiUp = oiPct > 0.2, oiDown = oiPct < -0.2, cvdUp = cvd > 0, cvdDown = cvd < 0;
  if (oiUp && cvdUp)   return { emoji: '🟢', text: 'приток лонгов' };
  if (oiDown && cvdUp) return { emoji: '🟡', text: 'short-cover' };
  if (oiUp && cvdDown) return { emoji: '🟠', text: 'впитывание' };
  if (oiDown && cvdDown) return { emoji: '⚪️', text: 'охлаждение' };
  return { emoji: '⚪️', text: 'нейтрально' };
}

export async function getOiCvdSnapshot(symbol) {
  const sym = String(symbol || '').toUpperCase();
  const period = OI_CVD_PERIOD;
  const limit = OI_CVD_LIMIT;
  try {
    console.log(`[oiCvd] snapshot start ${sym} (${period}×${limit}) → ${toBinance(sym)}`);
    const [oi, cvd] = await Promise.all([
      fetchOpenInterest(sym, period, limit),
      fetchCvd(sym, period, limit),
    ]);
    const oiPct  = Number.isFinite(oi?.oiChangePct) ? Number(oi.oiChangePct.toFixed(2)) : NaN;
    const cvdVal = Number.isFinite(cvd?.cvd) ? Number(cvd.cvd.toFixed(2)) : NaN;
    const label  = totalWindowLabel(period, limit);
    const v = verdictBy(oiPct, cvdVal);
    const out = {
      symbol: sym,
      period, limit, windowLabel: label,
      oiChangePct: oiPct,
      cvd: cvdVal,
      deltaLast: Number.isFinite(cvd?.deltaLast) ? Number(cvd.deltaLast.toFixed(2)) : NaN,
      verdictEmoji: v.emoji,
      verdictText: v.text,
    };
    console.log(`[oiCvd] snapshot ok ${sym}`, out);
    return out;
  } catch (e) {
    console.error(`[oiCvd] snapshot error ${sym}:`, e?.message || e);
    return {
      symbol: sym,
      period, limit, windowLabel: totalWindowLabel(period, limit),
      oiChangePct: NaN, cvd: NaN, deltaLast: NaN,
      verdictEmoji: '⚪️', verdictText: 'нет данных'
    };
  }
}
