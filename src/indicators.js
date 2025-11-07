// src/indicators.js
import { httpGetWithRetry } from './httpClient.js';

const UA = { headers: { 'User-Agent': 'Mozilla/5.0 Chrome/120' } };
const FAPI = 'https://fapi.binance.com';

function ema(arr, p) {
  const k = 2 / (p + 1);
  let e = arr[0];
  const out = [e];
  for (let i = 1; i < arr.length; i++) {
    e = arr[i] * k + e * (1 - k);
    out.push(e);
  }
  return out;
}

function mean(a) {
  if (!a.length) return 0;
  let s = 0;
  for (let i = 0; i < a.length; i++) s += a[i];
  return s / a.length;
}

function std(a) {
  if (a.length < 2) return 0;
  const m = mean(a);
  let s = 0;
  for (let i = 0; i < a.length; i++) {
    const d = a[i] - m;
    s += d * d;
  }
  return Math.sqrt(s / (a.length - 1));
}

function percentileRank(sample, value) {
  if (!sample.length) return 0;
  let cnt = 0;
  for (let i = 0; i < sample.length; i++) if (sample[i] <= value) cnt++;
  return (cnt / sample.length) * 100;
}

async function getKlines(symbol, interval = '5m', limit = 120) {
  const t0 = Date.now();
  const url = `${FAPI}/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&limit=${limit}`;
  console.log(`[ind:klines:${symbol}:${interval}x${limit}] START ${url}`);
  try {
    const resp = await httpGetWithRetry(url, UA, { retries: 2, timeout: 10000, backoffMs: 350 });
    const data = resp?.data;
    const ok = Array.isArray(data);
    console.log(`[ind:klines:${symbol}:${interval}x${limit}] ${ok ? 'OK' : 'BAD'} ${Date.now()-t0}ms len=${ok ? data.length : 0}`);
    if (!ok) throw new Error('bad_klines');
    return data.map(x => ({
      openTime: Number(x[0]),
      open: Number(x[1]),
      high: Number(x[2]),
      low: Number(x[3]),
      close: Number(x[4]),
      volume: Number(x[5]),
      closeTime: Number(x[6])
    }));
  } catch (e) {
    console.log(`[ind:klines:${symbol}:${interval}x${limit}] ERROR ${Date.now()-t0}ms ${e?.message||e}`);
    throw e;
  }
}

function calcEmaSlope(vals, period) {
  const e = ema(vals, period);
  if (e.length < 2) return { last: e[e.length - 1] || null, slopeUp: false };
  const last = e[e.length - 1];
  const prev = e[e.length - 2];
  return { last, slopeUp: last > prev };
}

function calcBbWidth(closes, period = 20, mult = 2) {
  if (closes.length < period) return { width: null, pctRank: null };
  const widths = [];
  for (let i = period; i <= closes.length; i++) {
    const slice = closes.slice(i - period, i);
    const m = mean(slice);
    const s = std(slice);
    const upper = m + mult * s;
    const lower = m - mult * s;
    const w = m !== 0 ? (upper - lower) / m : 0;
    widths.push(w);
  }
  const last = widths[widths.length - 1] ?? null;
  const base = widths.slice(0, -1);
  const pct = base.length ? percentileRank(base, last) : null;
  return { width: last, pctRank: pct };
}

function calcSessionVWAP(candles) {
  if (!candles.length) return { vwap: null, distPct: null };
  const last = candles[candles.length - 1];
  const day = new Date(last.closeTime).getUTCDate();
  let pv = 0;
  let v = 0;
  for (let i = 0; i < candles.length; i++) {
    const d = new Date(candles[i].closeTime).getUTCDate();
    if (d !== day) continue;
    const tp = (candles[i].high + candles[i].low + candles[i].close) / 3;
    const vol = candles[i].volume;
    pv += tp * vol;
    v += vol;
  }
  if (v === 0) return { vwap: null, distPct: null };
  const vwap = pv / v;
  const px = last.close;
  const distPct = vwap ? ((px - vwap) / vwap) * 100 : null;
  return { vwap, distPct };
}

async function getPremiumIndex(symbol) {
  const t0 = Date.now();
  const url = `${FAPI}/fapi/v1/premiumIndex?symbol=${encodeURIComponent(symbol)}`;
  console.log(`[ind:premium:${symbol}] START ${url}`);
  try {
    const resp = await httpGetWithRetry(url, UA, { retries: 2, timeout: 8000, backoffMs: 300 });
    const data = resp?.data || {};
    const mark = Number(data.markPrice);
    const index = Number(data.indexPrice);
    const basisPct = Number.isFinite(mark) && Number.isFinite(index) && index !== 0 ? ((mark - index) / index) * 100 : null;
    console.log(`[ind:premium:${symbol}] OK ${Date.now()-t0}ms basisPct=${Number.isFinite(basisPct)?basisPct.toFixed(4):'—'}`);
    return { mark, index, basisPct };
  } catch (e) {
    console.log(`[ind:premium:${symbol}] ERROR ${Date.now()-t0}ms ${e?.message||e}`);
    throw e;
  }
}

export async function getTrendPack(symbol) {
  const t0 = Date.now();
  console.log(`[ind:pack:${symbol}] START`);
  try {
    const kl = await getKlines(symbol, '5m', 120);
    const closes = kl.map(k => k.close);
    const ema20 = calcEmaSlope(closes, 20);
    const ema50 = calcEmaSlope(closes, 50);
    const bb = calcBbWidth(closes, 20, 2);
    const vwap = calcSessionVWAP(kl);
    const prem = await getPremiumIndex(symbol);
    const last = closes[closes.length - 1] ?? null;
    const pack = {
      price: last,
      ema20: ema20.last,
      ema50: ema50.last,
      ema20Up: ema20.slopeUp,
      ema50Up: ema50.slopeUp,
      above20: Number.isFinite(last) && Number.isFinite(ema20.last) ? last >= ema20.last : null,
      above50: Number.isFinite(last) && Number.isFinite(ema50.last) ? last >= ema50.last : null,
      bbWidth: bb.width,
      bbPctRank: bb.pctRank,
      vwap: vwap.vwap,
      vwapDistPct: vwap.distPct,
      basisPct: prem.basisPct
    };
    console.log(`[ind:pack:${symbol}] OK ${Date.now()-t0}ms above20=${pack.above20} above50=${pack.above50} bbPct=${Number.isFinite(pack.bbPctRank)?Math.round(pack.bbPctRank):'—'} vwapΔ=${Number.isFinite(pack.vwapDistPct)?pack.vwapDistPct.toFixed(2):'—'}% basis=${Number.isFinite(pack.basisPct)?pack.basisPct.toFixed(2):'—'}%`);
    return pack;
  } catch (e) {
    console.log(`[ind:pack:${symbol}] ERROR ${Date.now()-t0}ms ${e?.message||e}`);
    throw e;
  }
}
