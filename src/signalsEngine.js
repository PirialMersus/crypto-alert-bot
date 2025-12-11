/* /src/signalsEngine.js */
import { client } from './db/db.js';
import { bot } from './bot.js';
import { resolveUserLang } from "./cache.js";

const DB_NAME = process.env.DB_NAME || 'crypto_alert_dev';
const SNAP_COLLECTION = process.env.COLLECTION || 'marketSnapshots';

const prevState = {};

function fmtMoney(v) {
  if (!Number.isFinite(v)) return '—';
  const s = v >= 0 ? '' : '-';
  const a = Math.abs(v);
  if (a >= 1_000_000) return `${s}$${(a / 1_000_000).toFixed(2)}M`;
  if (a >= 1_000) return `${s}$${(a / 1_000).toFixed(0)}K`;
  return `${s}$${a.toFixed(2)}`;
}

function fmtPct(v) {
  if (!Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
}

//
// ─────────────────────────────────────────
//   TEXT RUS
// ─────────────────────────────────────────
//

function t_ru(type, sym, data) {
  const price = data?.price;
  const rsi = data?.rsi;
  const dom = data?.dom;
  const volPct = data?.volPct;
  const funding = data?.funding;
  const flow = data?.delta;

  const p = price != null ? ` (<b>${sym}: $${Math.round(price)}</b>)` : '';
  const r = rsi != null ? ` (<b>RSI: ${rsi}</b>)` : '';
  const d = dom != null ? ` (<b>DOM: ${dom.toFixed(2)}%</b>)` : '';
  const v = volPct != null ? ` (<b>Vol: ${volPct.toFixed(1)}%</b>)` : '';
  const f = funding != null ? ` (<b>Funding: ${funding}</b>)` : '';
  const nf = flow != null ? ` (<b>${fmtMoney(flow)}</b>)` : '';

  // OI + CVD
  if (type === 'oiCvd_up_up') return `🚀 ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — агрессивные лонги, возможен рост${p}`;
  if (type === 'oiCvd_up_down') return `⚠️ ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — приток шортов, давление вниз, будь внимателен${p}`;
  if (type === 'oiCvd_down_down') return `🔻 ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — закрытие лонгов, слабость покупателей, риск снижения${p}`;
  if (type === 'oiCvd_down_up') return `🔻 ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — закрытие шортов, продавцы ослабевают, возможен локальный рост${p}`;

  // Funding
  if (type === 'funding_high') return `📉 ${sym}: Фандинг ${data.funding} — рынок перегрет в лонги${f}`;
  if (type === 'funding_low') return `📉 ${sym}: Фандинг ${data.funding} — рынок перегрет в шорты${f}`;

  // Netflows
  if (type === 'netflows_in') return `📊 ${sym}: Приток ${fmtMoney(data.delta)} за 15м — возможное давление продаж${nf}`;
  if (type === 'netflows_out') return `📊 ${sym}: Отток ${fmtMoney(data.delta)} за 15м — возможное накопление${nf}`;

  // RSI
  if (type === 'rsi_low') return `📈 ${sym}: RSI ${data.rsi} — перепроданность${r}`;
  if (type === 'rsi_high') return `📈 ${sym}: RSI ${data.rsi} — перекупленность${r}`;

  // Price spike
  if (type === 'price_up') return `💥 ${sym}: Цена выросла на ${fmtPct(data.pct)} за 15м${p}`;
  if (type === 'price_down') return `💥 ${sym}: Цена упала на ${fmtPct(data.pct)} за 15м${p}`;

  // Volume
  if (type === 'vol_up') return `📈 ${sym}: Объём вырос на ${fmtPct(data.volPct)} — всплеск активности${v}`;

  // Dominance
  if (type === 'dom_up') return `📌 BTC доминация выросла на ${fmtPct(data.diff)}${d}`;
  if (type === 'dom_down') return `📌 BTC доминация упала на ${fmtPct(data.diff)}${d}`;

  return null;
}

//
// ─────────────────────────────────────────
//   TEXT ENG
// ─────────────────────────────────────────
//

function t_en(type, sym, data) {
  const price = data?.price;
  const rsi = data?.rsi;
  const dom = data?.dom;
  const volPct = data?.volPct;
  const funding = data?.funding;
  const flow = data?.delta;

  const p = price != null ? ` (<b>${sym}: $${Math.round(price)}</b>)` : '';
  const r = rsi != null ? ` (<b>RSI: ${rsi}</b>)` : '';
  const d = dom != null ? ` (<b>DOM: ${dom.toFixed(2)}%</b>)` : '';
  const v = volPct != null ? ` (<b>Vol: ${volPct.toFixed(1)}%</b>)` : '';
  const f = funding != null ? ` (<b>Funding: ${funding}</b>)` : '';
  const nf = flow != null ? ` (<b>${fmtMoney(flow)}</b>)` : '';

  // OI + CVD
  if (type === 'oiCvd_up_up') return `🚀 ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — aggressive longs, possible rise${p}`;
  if (type === 'oiCvd_up_down') return `⚠️ ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — shorts entering, downside pressure${p}`;
  if (type === 'oiCvd_down_down') return `🔻 ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — longs closing, buyer weakness${p}`;
  if (type === 'oiCvd_down_up') return `🔻 ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — short covering, sellers weakening${p}`;

  // Funding
  if (type === 'funding_high') return `📉 ${sym}: Funding ${data.funding} — market overheated in longs${f}`;
  if (type === 'funding_low') return `📉 ${sym}: Funding ${data.funding} — market overheated in shorts${f}`;

  // Netflows
  if (type === 'netflows_in') return `📊 ${sym}: Inflow ${fmtMoney(data.delta)} in 15m — potential sell pressure${nf}`;
  if (type === 'netflows_out') return `📊 ${sym}: Outflow ${fmtMoney(data.delta)} in 15m — possible accumulation${nf}`;

  // RSI
  if (type === 'rsi_low') return `📈 ${sym}: RSI ${data.rsi} — oversold zone${r}`;
  if (type === 'rsi_high') return `📈 ${sym}: RSI ${data.rsi} — overbought zone${r}`;

  // Price
  if (type === 'price_up') return `💥 ${sym}: Price increased by ${fmtPct(data.pct)} in 15m${p}`;
  if (type === 'price_down') return `💥 ${sym}: Price decreased by ${fmtPct(data.pct)} in 15m${p}`;

  // Volume
  if (type === 'vol_up') return `📈 ${sym}: Volume increased by ${fmtPct(data.volPct)} — activity spike${v}`;

  // Dominance
  if (type === 'dom_up') return `📌 BTC dominance increased by ${fmtPct(data.diff)}${d}`;
  if (type === 'dom_down') return `📌 BTC dominance decreased by ${fmtPct(data.diff)}${d}`;

  return null;
}

function langMsg(lang, type, sym, data) {
  const isEn = String(lang || '').startsWith('en');
  return isEn ? t_en(type, sym, data) : t_ru(type, sym, data);
}

//
// ─────────────────────────────────────────
//   MAIN ENGINE
// ─────────────────────────────────────────
//

export async function runOnce() {
  const db = client.db(DB_NAME);
  const col = db.collection(SNAP_COLLECTION);

  const docs = await col.find({}, { sort: { at: -1 } }).limit(3).toArray();
  if (docs.length < 2) return [];

  const cur = docs[0];
  const prev = docs[1];
  const syms = new Set(Object.keys(cur.snapshots || {}));

  syms.add('BTC');
  syms.add('ETH');

  const signals = [];

  for (const sym of syms) {
    if (!prevState[sym]) prevState[sym] = {};

    const ss = cur.snapshots?.[sym];
    const ssPrev = prev.snapshots?.[sym];

    const local = [];

    //
    // OI + CVD
    //
    let oiPct = null;
    let cvd = null;
    if (cur.oiCvd?.[sym]) {
      oiPct = Number(cur.oiCvd[sym].oiChangePct);
      cvd = Number(cur.oiCvd[sym].cvdUSD ?? cur.oiCvd[sym].cvd);
    }

    if (Number.isFinite(oiPct) && Number.isFinite(cvd) &&
      Math.abs(oiPct) >= 0.005 && Math.abs(cvd) >= 5_000_000) {

      let type = null;
      if (oiPct > 0 && cvd > 0) type = 'oiCvd_up_up';
      else if (oiPct > 0 && cvd < 0) type = 'oiCvd_up_down';
      else if (oiPct < 0 && cvd < 0) type = 'oiCvd_down_down';
      else if (oiPct < 0 && cvd > 0) type = 'oiCvd_down_up';

      if (type && !prevState[sym][type]) {
        local.push({ type, data: { oiPct, cvd, price: ss?.price } });
        prevState[sym][type] = true;
      }
    } else {
      prevState[sym] = {};
    }

    //
    // Funding
    //
    if (Number.isFinite(ss?.fundingNow)) {
      if (ss.fundingNow >= 0.015 && !prevState[sym]['funding_high']) {
        local.push({ type: 'funding_high', data: { funding: ss.fundingNow } });
        prevState[sym]['funding_high'] = true;
      } else if (ss.fundingNow < 0.015) prevState[sym]['funding_high'] = false;

      if (ss.fundingNow <= -0.015 && !prevState[sym]['funding_low']) {
        local.push({ type: 'funding_low', data: { funding: ss.fundingNow } });
        prevState[sym]['funding_low'] = true;
      } else if (ss.fundingNow > -0.015) prevState[sym]['funding_low'] = false;
    }

    //
    // Netflows
    //
    if (Number.isFinite(ss?.netFlowsUSDNow) && Number.isFinite(ssPrev?.netFlowsUSDNow)) {
      const d = ss.netFlowsUSDNow - ssPrev.netFlowsUSDNow;
      if (Math.abs(d) >= 100_000_000) {
        if (d > 0 && !prevState[sym]['netflows_in']) {
          local.push({ type: 'netflows_in', data: { delta: d } });
          prevState[sym]['netflows_in'] = true;
        }
        if (d < 0 && !prevState[sym]['netflows_out']) {
          local.push({ type: 'netflows_out', data: { delta: d } });
          prevState[sym]['netflows_out'] = true;
        }
      } else {
        prevState[sym]['netflows_in'] = false;
        prevState[sym]['netflows_out'] = false;
      }
    }

    //
    // RSI
    //
    if (Number.isFinite(ss?.rsi14) && Number.isFinite(ssPrev?.rsi14)) {
      const r = ss.rsi14;
      const rPrev = ssPrev.rsi14;

      if (rPrev >= 25 && r < 25 && !prevState[sym]['rsi_low']) {
        local.push({ type: 'rsi_low', data: { rsi: r } });
        prevState[sym]['rsi_low'] = true;
      } else if (r >= 25) prevState[sym]['rsi_low'] = false;

      if (rPrev <= 75 && r > 75 && !prevState[sym]['rsi_high']) {
        local.push({ type: 'rsi_high', data: { rsi: r } });
        prevState[sym]['rsi_high'] = true;
      } else if (r <= 75) prevState[sym]['rsi_high'] = false;
    }

    //
    // Price spike (BTC/ETH only)
    //
    if ((sym === 'BTC' || sym === 'ETH') &&
      Number.isFinite(ss?.price) && Number.isFinite(ssPrev?.price)) {

      const pct = ((ss.price - ssPrev.price) / ssPrev.price) * 100;

      if (pct >= 1.5 && !prevState[sym]['price_up']) {
        local.push({ type: 'price_up', data: { pct, price: ss.price } });
        prevState[sym]['price_up'] = true;
      } else if (pct < 1.5) prevState[sym]['price_up'] = false;

      if (pct <= -1.5 && !prevState[sym]['price_down']) {
        local.push({ type: 'price_down', data: { pct, price: ss.price } });
        prevState[sym]['price_down'] = true;
      } else if (pct > -1.5) prevState[sym]['price_down'] = false;
    }

    //
    // Volume spike
    //
    if ((sym === 'BTC' || sym === 'ETH') &&
      Number.isFinite(ss?.volume24h) && Number.isFinite(ssPrev?.volume24h)) {

      const volPct = ((ss.volume24h - ssPrev.volume24h) / ssPrev.volume24h) * 100;

      if (volPct >= 40 && !prevState[sym]['vol_up']) {
        local.push({ type: 'vol_up', data: { volPct } });
        prevState[sym]['vol_up'] = true;
      } else if (volPct < 40) prevState[sym]['vol_up'] = false;
    }

    //
    // BTC Dominance
    //
    if (sym === 'BTC') {
      const dNow = cur.btcDominancePct;
      const dPrev = prev.btcDominancePct;
      if (Number.isFinite(dNow) && Number.isFinite(dPrev)) {
        const diff = dNow - dPrev;
        if (diff >= 0.5 && !prevState[sym]['dom_up']) {
          local.push({ type: 'dom_up', data: { diff, dom: dNow } });
          prevState[sym]['dom_up'] = true;
        } else if (diff < 0.5) prevState[sym]['dom_up'] = false;

        if (diff <= -0.5 && !prevState[sym]['dom_down']) {
          local.push({ type: 'dom_down', data: { diff, dom: dNow } });
          prevState[sym]['dom_down'] = true;
        } else if (diff > -0.5) prevState[sym]['dom_down'] = false;
      }
    }

    if (local.length) signals.push({ sym, items: local });
  }

  //
  // SEND ONLY TO CREATOR
  //
  const CREATOR_ID = Number(process.env.CREATOR_ID);

  for (const sig of signals) {
    for (const item of sig.items) {
      const lang = "ru";
      const text = langMsg(lang, item.type, sig.sym, item.data);
      if (!text) continue;

      try {
        await bot.telegram.sendMessage(CREATOR_ID, text, {
          parse_mode: "HTML",
          disable_web_page_preview: true,
        });
      } catch (e) {}
    }
  }

  return signals;
}

export default { runOnce };
