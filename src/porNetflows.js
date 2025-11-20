// src/porNetflows.js
export async function buildPorNetflowsBlock(lang = 'ru', options = {}) {
  const isEn = String(lang).toLowerCase().startsWith('en');
  const { btcPrice = null, ethPrice = null, cryptoquant = null } = options || {};

  if (!cryptoquant || typeof cryptoquant !== 'object') return '';

  const esc = (s) => String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
  const B = (s) => `<b>${esc(s)}</b>`;

  const num = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : NaN;
  };

  const fmtSignedCoin = (v, sym, en) => {
    const n = num(v);
    if (!Number.isFinite(n)) return en ? 'no data' : 'нет данных';
    const abs = Math.abs(n);
    const formatted = abs.toLocaleString(en ? 'en-US' : 'ru-RU', { maximumFractionDigits: 2 });
    const sign = n >= 0 ? '+' : '−';
    return `${sign}${formatted} ${sym}`;
  };

  const fmtSignedUsd = (v, en) => {
    const n = num(v);
    if (!Number.isFinite(n)) return en ? 'no data' : 'нет данных';
    const abs = Math.abs(n);
    let val = abs, unit = '';

    if (abs >= 1_000_000_000) { val = abs / 1_000_000_000; unit = 'B'; }
    else if (abs >= 1_000_000) { val = abs / 1_000_000; unit = 'M'; }
    else if (abs >= 1_000) { val = abs / 1_000; unit = 'K'; }

    const numStr = val.toLocaleString(en ? 'en-US' : 'ru-RU', { maximumFractionDigits: 2 });
    const sign = n >= 0 ? '+' : '−';
    return `${sign}${numStr}${unit ? ` ${unit}` : ''} $`;
  };

  const fmtSignedPct = (v, en) => {
    const n = num(v);
    if (!Number.isFinite(n)) return en ? 'no data' : 'нет данных';
    const sign = n >= 0 ? '+' : '−';
    return `${sign}${Math.abs(n).toFixed(2)}%`;
  };

  const buildOne = (symbol, priceNow) => {
    const cq = cryptoquant?.[symbol]?.netflow;
    if (!cq) return null;

    let coin = num(cq.currentCoin);
    let usd  = num(cq.currentUSD);
    const pct = num(cq.pct24h);

    if (!Number.isFinite(usd) && Number.isFinite(coin) && Number.isFinite(priceNow)) {
      usd = coin * priceNow;
    }

    if (!Number.isFinite(coin) && !Number.isFinite(usd)) return null;

    const coinStr = fmtSignedCoin(coin, symbol, isEn);
    const usdStr  = fmtSignedUsd(usd, isEn);
    const pctStr  = fmtSignedPct(pct, isEn);

    if (isEn) {
      return `• ${symbol}: ${B(coinStr)} (${B(usdStr)}); prev 24h: ${B(pctStr)}`;
    } else {
      return `• ${symbol}: ${B(coinStr)} (${B(usdStr)}); к предыдущим 24ч: ${B(pctStr)}`;
    }
  };

  const btcLine = buildOne('BTC', btcPrice);
  const ethLine = buildOne('ETH', ethPrice);

  if (!btcLine && !ethLine) return '';

  const header = isEn
    ? '(Inflow to all exchanges – outflow from all exchanges = Net flow)'
    : '(Приток на все биржи – отток со всех бирж = Чистый поток)';

  const out = [header];
  if (btcLine) out.push(btcLine);
  if (ethLine) out.push(ethLine);

  return out.join('\n');
}
