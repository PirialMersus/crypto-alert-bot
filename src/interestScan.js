// src/interestScan.js
import axios from 'axios';
import { getWatchlistTopN } from './interestWatchlist.js';

const UA = { 'User-Agent': 'crypto-alert-bot/interest/1.0', 'Accept': 'application/json' };
const FAPI_BASES = [
  'https://fapi.binance.com',
  'https://fapi1.binance.com',
  'https://fapi2.binance.com',
  'https://fapi3.binance.com'
];

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const http = axios.create({ timeout: 8000, headers: UA });

function shortBody(x) {
  if (x == null) return '';
  if (typeof x === 'string') return x.slice(0, 120);
  try { return JSON.stringify(x).slice(0, 120) } catch { return '' }
}

async function getFromRotating(bases, path, params) {
  let lastErr = null;
  for (const base of bases) {
    const t0 = Date.now();
    try {
      const { data } = await http.get(`${base}${path}`, { params });
      const ms = Date.now() - t0;
      return { ok: true, base, ms, data };
    } catch (e) {
      const ms = Date.now() - t0;
      const st = e?.response?.status ?? '—';
      console.log(`[http] ERROR GET ${base}${path} code=${e?.code ?? '—'} status=${st} ${ms}ms ${shortBody(e?.response?.data)}`);
      // ВАЖНО: не выходим досрочно на 400/403/404 — пробуем следующую базу
      lastErr = { base, ms, e, status: st };
      continue;
    }
  }
  return { ok: false, base: lastErr?.base ?? null, ms: lastErr?.ms ?? 0, err: 'all_failed', status: lastErr?.status ?? '—' };
}

function pct(a, b) {
  if (typeof a !== 'number' || typeof b !== 'number' || b === 0) return null;
  return ((a - b) / b) * 100;
}

function classify(oiPct, cvdUsd) {
  if (oiPct == null || cvdUsd == null) return '⚪️ нет данных';
  if (oiPct > 0 && cvdUsd > 0) return '🟢 приток лонгов';
  if (oiPct < 0 && cvdUsd < 0) return '🔴 приток шортов';
  return '🟠 впитывание';
}

async function fetchOIChange(symbol, period = '5m', slices = 6) {
  const tag = `[interest:oi:${symbol}:${period}x${slices}]`;
  const r = await getFromRotating(FAPI_BASES, '/futures/data/openInterestHist', { symbol, period, limit: slices + 1 });
  if (!r.ok) { console.log(`${tag} FAIL ${r.base ?? '—'} ${r.ms}ms ${r.err ?? 'error'}`); return { ok: false, reason: r.err }; }
  const rows = Array.isArray(r.data) ? r.data : [];
  const last = rows.at(-1), prev = rows.at(-1 - slices);
  const lastOI = Number(last?.sumOpenInterestValue);
  const prevOI = Number(prev?.sumOpenInterestValue);
  if (!Number.isFinite(lastOI) || !Number.isFinite(prevOI)) { console.log(`${tag} WARN ${r.base} ${r.ms}ms no_oi`); return { ok: false, reason: 'no_oi' }; }
  const deltaPct = pct(lastOI, prevOI);
  console.log(`${tag} OK ${r.base} ${r.ms}ms`);
  return { ok: true, deltaPct, lastUsd: lastOI, prevUsd: prevOI };
}

async function fetchCVD(symbol, period = '5m', slices = 6) {
  const tag = `[interest:cvd:${symbol}:${period}x${slices}]`;

  // Правильный эндпоинт и правильный параметр: period (НЕ interval)
  const r = await getFromRotating(FAPI_BASES, '/futures/data/takerlongshortRatio', {
    symbol,
    period,     // "5m","15m","30m","1h","2h","4h","6h","12h","1d"
    limit: slices
  });

  if (!r.ok) {
    console.log(`${tag} FAIL ${r.base ?? '—'} ${r.ms}ms ${r.err ?? 'error'}`);
    return { ok: false, reason: 'http' };
  }

  const rows = Array.isArray(r.data) ? r.data : [];
  if (!rows.length) {
    console.log(`${tag} WARN ${r.base} ${r.ms}ms empty_rows`);
    return { ok: false, reason: 'empty' };
  }

  const toNum = (v) => (typeof v === 'number' || typeof v === 'string') ? Number(v) : NaN;

  let buy = 0, sell = 0;
  for (const it of rows) {
    // Для этого эндпоинта поля — buyVol/sellVol (строки), берём безопасно
    const b = toNum(it?.buyVol);
    const s = toNum(it?.sellVol);
    if (Number.isFinite(b)) buy += b;
    if (Number.isFinite(s)) sell += s;
  }

  if (!Number.isFinite(buy) || !Number.isFinite(sell)) {
    console.log(`${tag} WARN ${r.base} ${r.ms}ms no_numeric_fields`);
    return { ok: false, reason: 'no_fields' };
  }

  const cvdBase = Number((buy - sell).toFixed(6)); // в базовом активе
  console.log(`${tag} OK ${r.base} ${r.ms}ms sumBuy=${buy.toFixed(6)} sumSell=${sell.toFixed(6)} diffBase=${cvdBase}`);
  return { ok: true, cvdBase };
}

async function fetchT24(symbol) {
  const tag = `[interest:t24:${symbol}]`;
  const r = await getFromRotating(FAPI_BASES, '/fapi/v1/ticker/24hr', { symbol });
  if (!r.ok) { console.log(`${tag} FAIL ${r.base ?? '—'} ${r.ms}ms ${r.err ?? 'error'}`); return { ok: false, reason: r.err }; }
  const price = Number(r.data?.lastPrice);
  if (!Number.isFinite(price)) { console.log(`${tag} WARN ${r.base} ${r.ms}ms no_price`); return { ok: false, reason: 'no_price' }; }
  console.log(`${tag} OK ${r.base} ${r.ms}ms`);
  const qv = Number(r.data?.quoteVolume);
  return { ok: true, price, quoteVolume: Number.isFinite(qv) ? qv : null };
}

function fmtUsd(x) {
  if (x == null || !Number.isFinite(x)) return '—';
  const a = Math.abs(x);
  const sign = x >= 0 ? '' : '-';
  if (a >= 1_000_000) return `${sign}$${(a / 1_000_000).toFixed(2)} M`;
  if (a >= 1_000) return `${sign}$${(a / 1_000).toFixed(2)} K`;
  return `${sign}$${a.toFixed(2)}`;
}

export async function handleInterest(ctx, { size = 50, period = '5m', slices = 6 } = {}) {
  console.log(`[interest:action] interest_scan_30m top= ${size} user ${ctx?.from?.id ?? '—'}`);

  // быстрый ответ, чтобы не держать апдейт
  const pending = await ctx.reply('⏳ Сканирую 30м окно по watch-листу…');

  // тяжёлую работу запускаем в фоне и по готовности редактируем сообщение
  setImmediate(async () => {
    const t0 = Date.now();
    try {
      const base = await getWatchlistTopN(size);
      const symbols = base.map(s => `${s}USDT`);
      console.log(`[interest] scan start ${symbols.length} symbols, window=${period}x${slices}`);

      const results = [];
      for (const sym of symbols) {
        const t1 = Date.now();
        const [oi, cvd, t24] = await Promise.all([
          fetchOIChange(sym, period, slices).catch(() => ({ ok: false })),
          fetchCVD(sym, period, slices).catch(() => ({ ok: false })),
          fetchT24(sym).catch(() => ({ ok: false }))
        ]);

        if (!t24.ok) {
          console.log(`[interest:item] ${sym.replace('USDT','')} skip ${Date.now()-t1}ms no_price`);
          continue;
        }

        const oiPct  = oi.ok  && Number.isFinite(oi.deltaPct) ? Number(oi.deltaPct.toFixed(2)) : null;
        const cvdUsd = cvd.ok && Number.isFinite(cvd.cvdBase) && Number.isFinite(t24.price)
          ? Number((cvd.cvdBase * t24.price).toFixed(2))
          : null;

        console.log(`[interest:item] ${sym.replace('USDT','')} ok in ${Date.now()-t1}ms oi=${oiPct ?? '—'}% cvd=${cvdUsd ?? '—'} usd=${fmtUsd(cvdUsd)} price=${t24.price}`);
        const weightPct = 1;        // 1 балл за 1% OI
        const weightUsd = 1e-6;     // 1 балл за $1M CVD
        results.push({
          sym: sym.replace('USDT', ''),
          oiPct,
          cvdUsd,
          price: t24.price,

          sortKey: (Math.abs(oiPct || 0) * weightPct) + (Math.abs(cvdUsd || 0) * weightUsd)
        });

        await sleep(5); // лёгкая рассинхронизация, чтобы не бить в один тик
      }

      results.sort((a, b) => b.sortKey - a.sortKey);
      const top = results.slice(0, 15);

      const lines = top.map(it => {
        const sOi  = it.oiPct  == null ? '—' : `<b>${it.oiPct.toFixed(2)}%</b>`;
        const sCvd = it.cvdUsd == null ? '—' : `<b>${fmtUsd(it.cvdUsd)}</b>`;
        const mark = classify(it.oiPct, it.cvdUsd);
        return `• <b>${it.sym}</b>: OI Δ (30м): ${sOi} | CVD (30м): ${sCvd} — ${mark}`;
      });

      const when = new Date().toLocaleString('ru-RU', { timeZone: 'Europe/Kyiv' });
      const text = [
        `🔎 Скан интереса (30м)`,
        `Топ 15 по |OI Δ| из ${results.length}`,
        ``,
        ...lines,
        ``,
        `Данные на: ${when} (Europe/Kyiv)`,
        `Источник: Binance Futures public data`
      ].join('\n');

      await ctx.telegram.editMessageText(
        pending.chat.id,
        pending.message_id,
        undefined,
        text,
        { parse_mode: 'HTML', disable_web_page_preview: true }
      );

      console.log(`[interest] scan done in ${Date.now() - t0}ms`);
    } catch (e) {
      console.error('[interest:error]', e);
      await ctx.telegram.editMessageText(pending.chat.id, pending.message_id, undefined, '⚠️ Внутренняя ошибка, попробуй ещё раз.', {
        disable_web_page_preview: true
      });
    }
  });
}
