// src/alerts.js
import { getCollections } from './db.js';
import { refreshAllTickers, getPriceFast } from './pricing.js';
import { fmtNum, padLabel } from './utils.js';
import { ENTRIES_PER_PAGE } from './config.js';

function formatConditionShort(a) {
  const dir = a.condition === '>' ? '⬆️ выше' : '⬇️ ниже';
  return `${dir} ${a.price}`;
}

function formatAlertEntry(a, idx, cur, last) {
  const isSL = a.type === 'sl';
  const title = isSL ? `*${idx+1}. ${a.symbol} — 🛑 SL*` : `*${idx+1}. ${a.symbol}*`;
  const conditionStr = a.condition === '>' ? '⬆️ выше' : '⬇️ ниже';
  const curStr = fmtNum(cur);
  const priceStr = fmtNum(a.price);
  let changeText = '';
  if (typeof last === 'number' && last > 0 && typeof cur === 'number') {
    changeText = `\nС последнего просмотра: ${( ((cur-last)/last)*100 ).toFixed(2)}%`;
  }
  return `${title}\nТип: ${isSL ? '🛑 Стоп-лосс' : '🔔 Уведомление'}\nУсловие: ${conditionStr} *${priceStr}*\nТекущая: *${curStr}*${changeText}\n\n`;
}

export async function getUserAlertsCached(userId) {
  const { alertsCollection } = getCollections();
  return await alertsCollection.find({ userId }).toArray();
}

export function invalidateUserAlertsCache(/*userId*/) {
  // in this simplified module we don't keep in-memory cache here
}

export async function renderAlertsList(userId, options = { fast: false }) {
  const { alertsCollection } = getCollections();
  const alerts = await alertsCollection.find({ userId }).toArray();
  if (!alerts || !alerts.length) return { pages: [{ text: 'У тебя нет активных алертов.', buttons: [] }], pageCount: 1 };

  const uniqueSymbols = [...new Set(alerts.map(a => a.symbol))];
  const priceMap = new Map();

  if (!options.fast) {
    await refreshAllTickers();
    for (const s of uniqueSymbols) priceMap.set(s, await getPriceFast(s));
  } else {
    for (const a of alerts) {
      const s = a.symbol;
      // fallback to level1 cache
      priceMap.set(s, null);
    }
    refreshAllTickers().catch(()=>{});
  }

  const entries = alerts.map((a, idx) => {
    const cur = priceMap.get(a.symbol);
    return { text: formatAlertEntry(a, idx, cur, null), id: a._id.toString(), symbol: a.symbol, index: idx, alert: a };
  });

  if (entries.length <= ENTRIES_PER_PAGE) {
    let text = '📋 *Твои алерты:*\n\n';
    for (const e of entries) text += e.text;
    const buttons = [[{ text: '❌ Удалить пару № ...', callback_data: `show_delete_menu_0` }]];
    return { pages: [{ text, buttons }], pageCount: 1 };
  }

  const pages = [];
  for (let i = 0; i < entries.length; i += ENTRIES_PER_PAGE) {
    let text = '📋 *Твои алерты:*\n\n';
    for (let j = i; j < Math.min(i + ENTRIES_PER_PAGE, entries.length); j++) text += entries[j].text;
    pages.push({ text, entryIndexes: [], buttons: [] });
  }

  for (let p = 0; p < pages.length; p++) {
    pages[p].text += `Страница *${p+1}*/${pages.length}\n\n`;
    const rows = [];
    const nav = [];
    if (p > 0) nav.push({ text: '◀️ Предыдущая страница', callback_data: `alerts_page_${p-1}_view` });
    if (p < pages.length - 1) nav.push({ text: 'Следующая страница ▶️', callback_data: `alerts_page_${p+1}_view` });
    if (nav.length) rows.push(nav);
    rows.push([{ text: padLabel('❌ Удалить пару № ...'), callback_data: `show_delete_menu_${p}` }]);
    pages[p].buttons = rows;
  }

  return { pages, pageCount: pages.length };
}

export async function buildDeleteInlineForUser(userId, opts = { fast: false, sourcePage: null, totalPages: null }) {
  const { alertsCollection } = getCollections();
  const alerts = await alertsCollection.find({ userId }).toArray();
  if (!alerts || !alerts.length) return [];

  const entries = alerts.map((a, idx) => ({ id: a._id.toString(), symbol: a.symbol, index: idx, alert: a }));

  let pageEntries = entries;
  if (typeof opts.sourcePage === 'number') {
    const start = opts.sourcePage * ENTRIES_PER_PAGE;
    pageEntries = entries.slice(start, start + ENTRIES_PER_PAGE);
  }

  const inline = [];
  for (const e of pageEntries) {
    const cond = formatConditionShort(e.alert);
    const pageToken = (opts.sourcePage === null) ? 'all' : String(opts.sourcePage);
    inline.push([{ text: padLabel(`❌ ${e.index+1}: ${e.symbol}`), callback_data: `del_${e.id}_p${pageToken}` }]);
  }

  if (typeof opts.sourcePage === 'number' && typeof opts.totalPages === 'number' && opts.totalPages > 1) {
    const nav = [];
    const sp = opts.sourcePage;
    if (sp > 0) nav.push({ text: '◀️ Предыдущая страница', callback_data: `alerts_page_${sp-1}_view` });
    if (sp < opts.totalPages - 1) nav.push({ text: 'Следующая страница ▶️', callback_data: `alerts_page_${sp+1}_view` });
    if (nav.length) inline.push(nav);
  }

  if (typeof opts.sourcePage === 'number') inline.push([{ text: '⬆️ Свернуть', callback_data: `back_to_alerts_p${opts.sourcePage}` }]);
  else inline.push([{ text: '⬆️ Свернуть', callback_data: 'back_to_alerts' }]);

  if (pageEntries.length < entries.length) inline.push([{ text: '📂 Показать все алерты', callback_data: 'show_delete_menu_all' }]);

  return inline;
}
