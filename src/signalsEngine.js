/* /src/signalsEngine.js */
import { client } from './db/db.js';
import { bot } from './bot.js';
import { resolveUserLang } from './cache.js';

const DB_NAME = process.env.DB_NAME || 'crypto_alert_dev';
const SNAP_COLLECTION = process.env.COLLECTION || 'marketSnapshots';

const RECIPIENTS = [
  Number(process.env.CREATOR_ID)
];

const prevState = {};

const THRESH = {
  OI_PCT: 1,
  CVD_USD: 200000000,

  FUNDING_HIGH: 0.02,
  FUNDING_LOW: -0.02,

  NETFLOWS_15M: 200000000,
  DOM_DELTA_15M: 1.0,

  RSI_LOW: 20,
  RSI_HIGH: 80,

  PRICE_SPIKE_PCT: 3,
  VOLUME_SPIKE_PCT: 80
};

function fmtMoney(v) {
  if (!Number.isFinite(v)) return '—';
  const s = v >= 0 ? '' : '-';
  const a = Math.abs(v);
  if (a >= 1_000_000) return `${s}$${(a / 1_000_000).toFixed(2)}M`;
  if (a >= 1_000) return `${s}$${(a / 1_000).toFixed(0)}K`;
  return `${s}$${a.toFixed(0)}`;
}

function fmtPct(v) {
  if (!Number.isFinite(v)) return '—';
  return `${v > 0 ? '+' : ''}${v.toFixed(2)}%`;
}

function shortNum(n) {
  if (!Number.isFinite(n)) return '—';
  return Math.round(n);
}

/* ---------------- RUS ---------------- */
function t_ru(type, sym, data, extra) {
  if (type === 'oiCvd_up_up')
    return `🚀 ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — агрессивные лонги, шанс роста выше, следи за пробоями (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'oiCvd_up_down')
    return `⚠️ ${sym}: OI ${fmtPct(data.oiPct)}, CVD -${fmtMoney(Math.abs(data.cvd))} — приток шортов, давление вниз, будь внимателен (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'oiCvd_down_down')
    return `🔻 ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — закрытие лонгов, слабость покупателей, риск снижения (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'oiCvd_down_up')
    return `🔻 ${sym}: OI ${fmtPct(data.oiPct)}, CVD +${fmtMoney(data.cvd)} — закрытие шортов, продавцы ослабевают (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'funding_high')
    return `📉 ${sym}: Funding ${data.funding} — рынок перегрет в лонги, возможна коррекция (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'funding_low')
    return `📉 ${sym}: Funding ${data.funding} — рынок перегрет в шорты, возможен отскок (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'netflows_in')
    return `📊 ${sym}: Приток ${fmtMoney(data.delta)} — давление продаж (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'netflows_out')
    return `📊 ${sym}: Отток ${fmtMoney(data.delta)} — возможное накопление (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'rsi_low')
    return `📈 ${sym}: RSI ${data.rsi} — перепроданность (${sym}: RSI <b>${Math.round(data.rsi)}</b>)`;

  if (type === 'rsi_high')
    return `📈 ${sym}: RSI ${data.rsi} — перекупленность (${sym}: RSI <b>${Math.round(data.rsi)}</b>)`;

  if (type === 'price_up')
    return `💥 ${sym}: Цена выросла на ${fmtPct(data.pct)} (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'price_down')
    return `💥 ${sym}: Цена упала на ${fmtPct(data.pct)} (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'vol_up')
    return `📈 ${sym}: Объём вырос на ${fmtPct(data.volPct)} (${sym} vol)`; // нет числового значения → не жирню

  if (type === 'dom_up')
    return `📌 BTC доминация выросла на ${fmtPct(data.diff)} (BTC.D: <b>${data.now.toFixed(2)}%</b>)`;

  if (type === 'dom_down')
    return `📌 BTC доминация упала на ${fmtPct(data.diff)} (BTC.D: <b>${data.now.toFixed(2)}%</b>)`;

  return null;
}

/* ---------------- ENG ---------------- */
function t_en(type, sym, data, extra) {
  if (type === 'oiCvd_up_up')
    return `🚀 ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — aggressive longs, breakout risk up (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'oiCvd_up_down')
    return `⚠️ ${sym}: OI ${fmtPct(data.oiPct)}, CVD -${fmtMoney(Math.abs(data.cvd))} — shorts entering, downside pressure (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'oiCvd_down_down')
    return `🔻 ${sym}: OI ${fmtPct(data.oiPct)}, CVD ${fmtMoney(data.cvd)} — longs closing, buyer weakness (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'oiCvd_down_up')
    return `🔻 ${sym}: OI ${fmtPct(data.oiPct)}, CVD +${fmtMoney(data.cvd)} — shorts closing, sellers weakening (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'funding_high')
    return `📉 ${sym}: Funding ${data.funding} — market overheated in longs (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'funding_low')
    return `📉 ${sym}: Funding ${data.funding} — market overheated in shorts (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'netflows_in')
    return `📊 ${sym}: Inflow ${fmtMoney(data.delta)} — sell pressure (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'netflows_out')
    return `📊 ${sym}: Outflow ${fmtMoney(data.delta)} — possible accumulation (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'rsi_low')
    return `📈 ${sym}: RSI ${data.rsi} — oversold (${sym}: RSI <b>${Math.round(data.rsi)}</b>)`;

  if (type === 'rsi_high')
    return `📈 ${sym}: RSI ${data.rsi} — overbought (${sym}: RSI <b>${Math.round(data.rsi)}</b>)`;

  if (type === 'price_up')
    return `💥 ${sym}: Price up ${fmtPct(data.pct)} (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'price_down')
    return `💥 ${sym}: Price down ${fmtPct(data.pct)} (${sym}: <b>$${shortNum(extra.price)}</b>)`;

  if (type === 'vol_up')
    return `📈 ${sym}: Volume up ${fmtPct(data.volPct)} (${sym} vol)`; // нет числа → не жирню

  if (type === 'dom_up')
    return `📌 BTC dominance increased by ${fmtPct(data.diff)} (BTC.D: <b>${data.now.toFixed(2)}%</b>)`;

  if (type === 'dom_down')
    return `📌 BTC dominance decreased by ${fmtPct(data.diff)} (BTC.D: <b>${data.now.toFixed(2)}%</b>)`;

  return null;
}

function langMsg(lang, type, sym, data, extra) {
  const isEn = String(lang || '').startsWith('en');
  return isEn ? t_en(type, sym, data, extra) : t_ru(type, sym, data, extra);
}

export async function runOnce() {
  const db = client.db(DB_NAME);
  const col = db.collection(SNAP_COLLECTION);

  const docs = await col.find({}, { sort: { at: -1 } }).limit(3).toArray();
  if (docs.length < 2) return [];

  const cur = docs[0];
  const prev = docs[1];

  const syms = new Set();
  if (cur.snapshots) Object.keys(cur.snapshots).forEach(s => syms.add(s));
  syms.add('BTC');
  syms.add('ETH');

  const signals = [];

  for (const sym of syms) {
    if (!prevState[sym]) prevState[sym] = {};

    const ss = cur.snapshots?.[sym];
    const ssPrev = prev.snapshots?.[sym];

    const local = [];

    let priceNow = Number(ss?.price);

    let oiPct = null;
    let cvd = null;

    if (cur.oiCvd?.[sym]) {
      oiPct = Number(cur.oiCvd[sym].oiChangePct);
      cvd = Number(cur.oiCvd[sym].cvdUSD ?? cur.oiCvd[sym].cvd);
    }

    if (Number.isFinite(oiPct) && Number.isFinite(cvd) &&
      Math.abs(oiPct) >= THRESH.OI_PCT &&
      Math.abs(cvd) >= THRESH.CVD_USD) {

      let type = null;
      if (oiPct > 0 && cvd > 0) type = 'oiCvd_up_up';
      else if (oiPct > 0 && cvd < 0) type = 'oiCvd_up_down';
      else if (oiPct < 0 && cvd < 0) type = 'oiCvd_down_down';
      else if (oiPct < 0 && cvd > 0) type = 'oiCvd_down_up';

      if (type && !prevState[sym][type]) {
        local.push({ type, data: { oiPct, cvd }, extra: { price: priceNow } });
        prevState[sym][type] = true;
      }
    } else {
      prevState[sym]['oiCvd_up_up'] = false;
      prevState[sym]['oiCvd_up_down'] = false;
      prevState[sym]['oiCvd_down_down'] = false;
      prevState[sym]['oiCvd_down_up'] = false;
    }

    if (Number.isFinite(ss?.fundingNow)) {
      if (ss.fundingNow >= THRESH.FUNDING_HIGH && !prevState[sym]['funding_high']) {
        local.push({ type: 'funding_high', data: { funding: ss.fundingNow }, extra: { price: priceNow }});
        prevState[sym]['funding_high'] = true;
      } else if (ss.fundingNow < THRESH.FUNDING_HIGH) prevState[sym]['funding_high'] = false;

      if (ss.fundingNow <= THRESH.FUNDING_LOW && !prevState[sym]['funding_low']) {
        local.push({ type: 'funding_low', data: { funding: ss.fundingNow }, extra: { price: priceNow }});
        prevState[sym]['funding_low'] = true;
      } else if (ss.fundingNow > THRESH.FUNDING_LOW) prevState[sym]['funding_low'] = false;
    }

    if (Number.isFinite(ss?.netFlowsUSDNow) && Number.isFinite(ssPrev?.netFlowsUSDNow)) {
      const d = ss.netFlowsUSDNow - ssPrev.netFlowsUSDNow;
      if (Math.abs(d) >= THRESH.NETFLOWS_15M) {
        if (d > 0 && !prevState[sym]['netflows_in']) {
          local.push({ type: 'netflows_in', data: { delta: d }, extra: { price: priceNow }});
          prevState[sym]['netflows_in'] = true;
        }
        if (d < 0 && !prevState[sym]['netflows_out']) {
          local.push({ type: 'netflows_out', data: { delta: d }, extra: { price: priceNow }});
          prevState[sym]['netflows_out'] = true;
        }
      } else {
        prevState[sym]['netflows_in'] = false;
        prevState[sym]['netflows_out'] = false;
      }
    }

    if (Number.isFinite(ss?.rsi14) && Number.isFinite(ssPrev?.rsi14)) {
      const rNow = ss.rsi14;
      const rPrev = ssPrev.rsi14;

      if (rPrev >= THRESH.RSI_LOW && rNow < THRESH.RSI_LOW && !prevState[sym]['rsi_low']) {
        local.push({ type: 'rsi_low', data: { rsi: rNow }, extra: { price: priceNow }});
        prevState[sym]['rsi_low'] = true;
      } else if (rNow >= THRESH.RSI_LOW) prevState[sym]['rsi_low'] = false;

      if (rPrev <= THRESH.RSI_HIGH && rNow > THRESH.RSI_HIGH && !prevState[sym]['rsi_high']) {
        local.push({ type: 'rsi_high', data: { rsi: rNow }, extra: { price: priceNow }});
        prevState[sym]['rsi_high'] = true;
      } else if (rNow <= THRESH.RSI_HIGH) prevState[sym]['rsi_high'] = false;
    }

    if (['BTC', 'ETH'].includes(sym) &&
      Number.isFinite(ss?.price) && Number.isFinite(ssPrev?.price)) {

      const pct = ((ss.price - ssPrev.price) / ssPrev.price) * 100;

      if (pct >= THRESH.PRICE_SPIKE_PCT && !prevState[sym]['price_up']) {
        local.push({ type: 'price_up', data: { pct }, extra: { price: priceNow }});
        prevState[sym]['price_up'] = true;
      } else if (pct < THRESH.PRICE_SPIKE_PCT) prevState[sym]['price_up'] = false;

      if (pct <= -THRESH.PRICE_SPIKE_PCT && !prevState[sym]['price_down']) {
        local.push({ type: 'price_down', data: { pct }, extra: { price: priceNow }});
        prevState[sym]['price_down'] = true;
      } else if (pct > -THRESH.PRICE_SPIKE_PCT) prevState[sym]['price_down'] = false;
    }

    if (['BTC', 'ETH'].includes(sym) &&
      Number.isFinite(ss?.volume24h) && Number.isFinite(ssPrev?.volume24h)) {

      const volPct = ((ss.volume24h - ssPrev.volume24h) / ssPrev.volume24h) * 100;
      if (volPct >= THRESH.VOLUME_SPIKE_PCT && !prevState[sym]['vol_up']) {
        local.push({ type: 'vol_up', data: { volPct }, extra: { price: priceNow }});
        prevState[sym]['vol_up'] = true;
      } else if (volPct < THRESH.VOLUME_SPIKE_PCT) prevState[sym]['vol_up'] = false;
    }

    if (sym === 'BTC') {
      const dNow = cur.btcDominancePct;
      const dPrev = prev.btcDominancePct;

      if (Number.isFinite(dNow) && Number.isFinite(dPrev)) {
        const diff = dNow - dPrev;

        if (diff >= THRESH.DOM_DELTA_15M && !prevState[sym]['dom_up']) {
          local.push({ type: 'dom_up', data: { diff, now: dNow }, extra: { price: priceNow }});
          prevState[sym]['dom_up'] = true;
        } else if (diff < THRESH.DOM_DELTA_15M) prevState[sym]['dom_up'] = false;

        if (diff <= -THRESH.DOM_DELTA_15M && !prevState[sym]['dom_down']) {
          local.push({ type: 'dom_down', data: { diff, now: dNow }, extra: { price: priceNow }});
          prevState[sym]['dom_down'] = true;
        } else if (diff > -THRESH.DOM_DELTA_15M) prevState[sym]['dom_down'] = false;
      }
    }

    if (local.length) signals.push({ sym, items: local });
  }

  for (const sig of signals) {
    for (const item of sig.items) {
      for (const uid of RECIPIENTS) {
        const lang = await resolveUserLang(uid).catch(() => 'ru');
        const text = langMsg(lang, item.type, sig.sym, item.data, item.extra);
        if (!text) continue;
        try {
          await bot.telegram.sendMessage(uid, text, {
            parse_mode: "HTML",
            disable_web_page_preview: true,
          });
        } catch (e) {}
      }
    }
  }

  return signals;
}

export default { runOnce };
