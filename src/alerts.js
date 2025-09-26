// src/alerts.js
import { ENTRIES_PER_PAGE, BG_CHECK_INTERVAL, DELETE_LABEL_TARGET_LEN } from './constants.js';
import { alertsCollection, alertsArchiveCollection, usersCollection } from './db.js';
import { tickersCache, pricesCache, allAlertsCache, getUserAlertsCached, getAllAlertsCached, getUserLastViews, setUserLastViews, invalidateUserAlertsCache, getUserAlertsOrder } from './cache.js';
import { getPriceLevel1 } from './prices.js';
import { fmtNum, formatChangeWithIcons, padLabel } from './utils.js';

function t(lang, key, ...vars) {
  const isEn = String(lang || '').split('-')[0] === 'en';
  const dict = {
    en: {
      your_alerts_title: '📋 *Your alerts:*',
      no_active_alerts: 'You have no active alerts.',
      delete_menu: '❌ Delete pair № ...',
      page: (p, total) => `Page *${p}*/${total}`,
      prev: '◀️ Previous',
      next: 'Next ▶️',
      collapse: '⬆️ Collapse',
      show_all: '📂 Show all alerts',
      alert_title: (idx, sym, isSL) => `${idx+1}. ${sym}${isSL ? ' — 🛑 SL' : ''}`,
      type_label: 'Type',
      type_sl: '🛑 SL',
      type_alert: '🔔 Alert',
      condition_label: 'Condition',
      condition_above: '⬆️ when above',
      condition_below: '⬇️ when below',
      current_label: 'Current',
      left_label: 'left',
      from_last_view: 'From last view',
      old_alerts_title: '📜 *Old alerts:*',
      no_old_alerts: (sym) => sym ? `No old alerts for *${sym}* in the selected period.` : 'No old alerts for the selected period.',
      back: '↩️ Back',
      status_fired: '✅ Fired',
      status_deleted: '🗑️ Deleted',
      status_info: 'ℹ️ Status',
      time_label: 'Time',
      delete_reason: 'Reason of deletion',
      fired_price: 'Price when fired'
    },
    ru: {
      your_alerts_title: '📋 *Твои алерты:*',
      no_active_alerts: 'У тебя нет активных алертов.',
      delete_menu: '❌ Удалить пару № ...',
      page: (p, total) => `Страница *${p}*/${total}`,
      prev: '◀️ Предыдущая страница',
      next: 'Следующая страница ▶️',
      collapse: '⬆️ Свернуть',
      show_all: '📂 Показать все алерты',
      alert_title: (idx, sym, isSL) => `${idx+1}. ${sym}${isSL ? ' — 🛑 SL' : ''}`,
      type_label: 'Тип',
      type_sl: '🛑 Стоп-лосс',
      type_alert: '🔔 Уведомление',
      condition_label: 'Условие',
      condition_above: '⬆️ выше',
      condition_below: '⬇️ ниже',
      current_label: 'Текущая',
      left_label: 'осталось',
      from_last_view: 'С последнего просмотра',
      old_alerts_title: '📜 *Старые алерты:*',
      no_old_alerts: (sym) => sym ? `Нет старых алертов с тикером *${sym}* за выбранный период.` : 'Нет старых алертов за выбранный период.',
      back: '↩️ Назад',
      status_fired: '✅ Сработал',
      status_deleted: '🗑️ Удалён',
      status_info: 'ℹ️ Статус',
      time_label: 'Время',
      delete_reason: 'Причина удаления',
      fired_price: 'Цена при срабатывании'
    }
  };
  const L = isEn ? dict.en : dict.ru;
  const v = L[key];
  if (typeof v === 'function') return v(...vars);
  return v;
}

export function formatAlertEntry(a, idx, cur, last, lang = 'ru') {
  const isSL = a.type === 'sl';
  const titleRaw = t(lang, 'alert_title', idx, a.symbol, isSL);
  const title = `*${titleRaw}*`;
  const typeLabel = isSL ? t(lang, 'type_sl') : t(lang, 'type_alert');
  const conditionStr = a.condition === '>' ? t(lang, 'condition_above') : t(lang, 'condition_below');
  let percent = '';
  if (typeof cur === 'number' && typeof a.price === 'number' && a.price !== 0) {
    const diff = a.condition === '>' ? (a.price - cur) : (cur - a.price);
    percent = ` (${t(lang, 'left_label')} ${(diff / a.price * 100).toFixed(2)}% ${a.condition === '<' ? (t(lang, 'condition_below')) : (t(lang, 'condition_above'))})`;
  }
  let changeText = '';
  if (typeof last === 'number' && last > 0 && typeof cur === 'number') {
    changeText = `\n${t(lang, 'from_last_view')}: ${formatChangeWithIcons(((cur - last)/last)*100)}`;
  }
  const curStr = fmtNum(cur);
  const priceStr = fmtNum(a.price);
  return `${title}\n${t(lang, 'type_label')}: ${typeLabel}\n${t(lang, 'condition_label')}: ${conditionStr} *${priceStr}*\n${t(lang, 'current_label')}: *${curStr}*${percent}${changeText}\n\n`;
}

export function formatConditionShort(a, lang = 'ru') {
  const dir = a.condition === '>' ? t(lang, 'condition_above') : t(lang, 'condition_below');
  return `${dir} ${fmtNum(a.price)}`;
}

function sortAlertsByOrder(alerts, order) {
  if (!Array.isArray(alerts) || alerts.length === 0) return alerts;
  if (order === 'new_top') {
    return alerts.slice().sort((a, b) => {
      const ta = (a._id && typeof a._id.getTimestamp === 'function') ? a._id.getTimestamp().getTime() : 0;
      const tb = (b._id && typeof b._id.getTimestamp === 'function') ? b._id.getTimestamp().getTime() : 0;
      return tb - ta;
    });
  }
  return alerts.slice().sort((a, b) => {
    const ta = (a._id && typeof a._id.getTimestamp === 'function') ? a._id.getTimestamp().getTime() : 0;
    const tb = (b._id && typeof b._id.getTimestamp === 'function') ? b._id.getTimestamp().getTime() : 0;
    return ta - tb;
  });
}

export async function renderAlertsList(userId, options = { fast: false, lang: 'ru' }) {
  const lang = options.lang || 'ru';
  let alerts = await getUserAlertsCached(userId);
  if (!alerts || !alerts.length) return { pages: [{ text: t(lang, 'no_active_alerts'), buttons: [] }], pageCount: 1 };

  try {
    const order = await getUserAlertsOrder(userId);
    alerts = sortAlertsByOrder(alerts, order);
  } catch (e) {}

  const uniqueSymbols = [...new Set(alerts.map(a => a.symbol))];
  const priceMap = new Map();

  for (const s of uniqueSymbols) {
    const p = tickersCache.map.get(s);
    if (Number.isFinite(p)) priceMap.set(s, p);
    else {
      const p2 = await getPriceLevel1(s);
      if (Number.isFinite(p2)) priceMap.set(s, p2);
    }
  }

  const lastViews = await getUserLastViews(userId);

  const entries = alerts.map((a, idx) => {
    const cur = priceMap.get(a.symbol);
    const last = (typeof lastViews[a.symbol] === 'number') ? lastViews[a.symbol] : null;
    return { text: formatAlertEntry(a, idx, cur, last, lang), id: a._id.toString(), symbol: a.symbol, index: idx, alert: a };
  });

  const total = entries.length;
  if (total <= ENTRIES_PER_PAGE) {
    let text = `${t(lang, 'your_alerts_title')}\n\n`;
    for (const e of entries) text += e.text;
    const buttons = [];
    buttons.push([{ text: t(lang, 'delete_menu'), callback_data: `show_delete_menu_0` }]);
    const valid = {};
    for (const s of uniqueSymbols) { const v = priceMap.get(s); if (Number.isFinite(v)) valid[s] = v; }
    if (Object.keys(valid).length) try { await setUserLastViews(userId, valid); } catch (e) {}
    return { pages: [{ text, buttons }], pageCount: 1 };
  }

  const pages = [];
  for (let i = 0; i < entries.length; i += ENTRIES_PER_PAGE) {
    let text = `${t(lang, 'your_alerts_title')}\n\n`;
    const entryIndexes = [];
    for (let j = i; j < Math.min(i + ENTRIES_PER_PAGE, entries.length); j++) {
      text += entries[j].text;
      entryIndexes.push(j);
    }
    pages.push({ text, entryIndexes, buttons: [] });
  }

  for (let p = 0; p < pages.length; p++) {
    pages[p].text = pages[p].text + `${t(lang, 'page', p+1, pages.length)}\n\n`;
    const rows = [];
    const nav = [];
    if (p > 0) nav.push({ text: t(lang, 'prev'), callback_data: `alerts_page_${p-1}_view` });
    if (p < pages.length - 1) nav.push({ text: t(lang, 'next'), callback_data: `alerts_page_${p+1}_view` });
    if (nav.length) rows.push(nav);
    rows.push([{ text: padLabel(t(lang, 'delete_menu'), Math.max(DELETE_LABEL_TARGET_LEN || 30, 28)), callback_data: `show_delete_menu_${p}` }]);
    pages[p].buttons = rows;
  }

  const valid = {};
  for (const s of uniqueSymbols) { const v = priceMap.get(s); if (Number.isFinite(v)) valid[s] = v; }
  if (Object.keys(valid).length) try { await setUserLastViews(userId, valid); } catch (e) {}

  return { pages, pageCount: pages.length };
}

export async function buildDeleteInlineForUser(userId, opts = { fast: false, sourcePage: null, totalPages: null, lang: 'ru' }) {
  const lang = opts.lang || 'ru';
  let alerts = await getUserAlertsCached(userId);
  if (!alerts || !alerts.length) return [];

  try {
    const order = await getUserAlertsOrder(userId);
    alerts = sortAlertsByOrder(alerts, order);
  } catch (e) {}

  const uniqueSymbols = [...new Set(alerts.map(a => a.symbol))];
  const priceMap = new Map();

  for (const s of uniqueSymbols) {
    const p = tickersCache.map.get(s);
    if (Number.isFinite(p)) priceMap.set(s, p);
    else {
      const c = pricesCache.get(s);
      if (c) priceMap.set(s, c.price);
      else {
        const p2 = await getPriceLevel1(s);
        if (Number.isFinite(p2)) priceMap.set(s, p2);
      }
    }
  }

  const lastViews = await getUserLastViews(userId);

  const entries = alerts.map((a, idx) => {
    const cur = priceMap.get(a.symbol);
    const last = (typeof lastViews[a.symbol] === 'number') ? lastViews[a.symbol] : null;
    return { id: a._id.toString(), symbol: a.symbol, index: idx, alert: a, cur, last };
  });

  let pageEntries = entries;
  if (typeof opts.sourcePage === 'number' && Number.isFinite(opts.sourcePage)) {
    const startIdx = opts.sourcePage * ENTRIES_PER_PAGE;
    const endIdx = Math.min(startIdx + ENTRIES_PER_PAGE, entries.length);
    pageEntries = entries.slice(startIdx, endIdx);
  }

  const inline = [];

  for (const e of pageEntries) {
    const cond = formatConditionShort(e.alert, lang);
    const raw = `${t(lang, 'delete_menu').replace('№ ...', `${e.index+1}`)}: ${e.symbol} — ${cond}`;
    const pageToken = (opts.sourcePage === null) ? 'all' : String(opts.sourcePage);
    inline.push([{ text: padLabel(raw, Math.max(DELETE_LABEL_TARGET_LEN || 30, 28)), callback_data: `del_${e.id}_p${pageToken}` }]);
  }

  if (typeof opts.sourcePage === 'number' && typeof opts.totalPages === 'number' && opts.totalPages > 1) {
    const nav = [];
    const sp = opts.sourcePage;
    if (sp > 0) nav.push({ text: t(lang, 'prev'), callback_data: `alerts_page_${sp-1}_view` });
    if (sp < opts.totalPages - 1) nav.push({ text: t(lang, 'next'), callback_data: `alerts_page_${sp+1}_view` });
    if (nav.length) inline.push(nav);
  }

  if (typeof opts.sourcePage === 'number' && Number.isFinite(opts.sourcePage)) {
    inline.push([{ text: t(lang, 'collapse'), callback_data: `back_to_alerts_p${opts.sourcePage}` }]);
  } else {
    inline.push([{ text: t(lang, 'collapse'), callback_data: 'back_to_alerts' }]);
  }
  if (pageEntries.length < entries.length) inline.push([{ text: t(lang, 'show_all'), callback_data: 'show_delete_menu_all' }]);

  const valid = {};
  for (const s of uniqueSymbols) { const v = priceMap.get(s); if (Number.isFinite(v)) valid[s] = v; }
  if (Object.keys(valid).length) try { await setUserLastViews(userId, valid); } catch (e) {}

  return inline;
}

export async function renderOldAlertsList(userId, opts = { days: 30, symbol: null, token: 'd30_q', lang: 'ru' }) {
  const lang = opts.lang || 'ru';
  const days = (opts && Number.isFinite(opts.days)) ? Math.max(1, Math.floor(opts.days)) : 30;
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000);

  const q = { userId, $or: [ { firedAt: { $exists: true } }, { deletedAt: { $exists: true } } ] };
  q.$and = [{ createdAt: { $gte: since } }];

  if (opts && opts.symbol) {
    const sym = String(opts.symbol).toUpperCase();
    q.$and.push({ $or: [{ symbol: sym }, { symbol: `${sym}-USDT` }] });
  }

  let docs = [];
  try {
    docs = await alertsArchiveCollection.find(q, { sort: { firedAt: -1, deletedAt: -1, createdAt: -1 } }).toArray();
  } catch (e) {
    docs = [];
  }

  if (!docs || !docs.length) {
    const text = t(lang, 'no_old_alerts', opts && opts.symbol ? String(opts.symbol).toUpperCase() : null);
    const buttons = [[{ text: t(lang, 'back'), callback_data: 'back_to_main' }]];
    return { pages: [{ text, buttons }], pageCount: 1 };
  }

  const entries = docs.map((d, idx) => {
    const status = d.firedAt ? t(lang, 'status_fired') : (d.deletedAt ? t(lang, 'status_deleted') : t(lang, 'status_info'));
    const when = d.firedAt ? d.firedAt : (d.deletedAt ? d.deletedAt : d.createdAt);
    const priceStr = fmtNum(d.price);
    const symbol = d.symbol;
    const byType = d.type === 'sl' ? t(lang, 'type_sl') : t(lang, 'type_alert');
    const reason = d.deleteReason ? `\n${t(lang, 'delete_reason')}: ${d.deleteReason}` : '';
    const firedInfo = d.firedPrice ? `\n${t(lang, 'fired_price')}: *${fmtNum(d.firedPrice)}*` : '';
    const txt = `*${idx+1}. ${symbol}* — ${byType}\n${t(lang, 'condition_label')}: ${d.condition === '>' ? t(lang, 'condition_above') : t(lang, 'condition_below')} *${priceStr}*\nСтатус: ${status}\n${t(lang, 'time_label')}: ${new Date(when).toLocaleString() || ''}${firedInfo}${reason}\n\n`;
    return { text: txt, id: d._id?.toString?.() || `arch_${idx}` };
  });

  const pages = [];
  for (let i = 0; i < entries.length; i += ENTRIES_PER_PAGE) {
    let text = `${t(lang, 'old_alerts_title')}\n\n`;
    const entryIndexes = [];
    for (let j = i; j < Math.min(i + ENTRIES_PER_PAGE, entries.length); j++) {
      text += entries[j].text;
      entryIndexes.push(j);
    }
    pages.push({ text, entryIndexes, buttons: [] });
  }

  for (let p = 0; p < pages.length; p++) {
    pages[p].text = pages[p].text + `${t(lang, 'page', p+1, pages.length)}\n\n`;
    const rows = [];
    const nav = [];
    const token = opts && opts.token ? opts.token : `d${days}_q${opts && opts.symbol ? encodeURIComponent(String(opts.symbol)) : ''}`;
    if (p > 0) nav.push({ text: t(lang, 'prev'), callback_data: `old_alerts_page_${p-1}_view_${token}` });
    if (p < pages.length - 1) nav.push({ text: t(lang, 'next'), callback_data: `old_alerts_page_${p+1}_view_${token}` });
    if (nav.length) rows.push(nav);
    rows.push([{ text: t(lang, 'back'), callback_data: 'back_to_main' }]);
    pages[p].buttons = rows;
  }

  return { pages, pageCount: pages.length };
}

export function startAlertsChecker(bot) {
  setInterval(async () => {
    try {
      const all = await getAllAlertsCached();
      if (!all || !all.length) return;

      const unique = [...new Set(all.map(a => a.symbol))];
      const priceMap = new Map();
      const missing = [];
      for (const s of unique) {
        const p = tickersCache.map.get(s);
        if (Number.isFinite(p)) priceMap.set(s, p);
        else missing.push(s);
      }

      for (let i = 0; i < missing.length; i += 8) {
        const chunk = missing.slice(i, i+8);
        await Promise.all(chunk.map(async sym => {
          const p = await getPriceLevel1(sym);
          if (Number.isFinite(p)) priceMap.set(sym, p);
        }));
      }

      for (const a of all) {
        const cur = priceMap.get(a.symbol) ?? (pricesCache.get(a.symbol)?.price);
        if (!Number.isFinite(cur)) continue;
        if ((a.condition === '>' && cur > a.price) || (a.condition === '<' && cur < a.price)) {
          const isSL = a.type === 'sl';
          const text = `${isSL ? '🛑 *Сработал стоп-лосс!*' : '🔔 *Сработал алерт!*'}\nМонета: *${a.symbol}*\nЦена сейчас: *${fmtNum(cur)}*\nУсловие: ${a.condition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${fmtNum(a.price)}*`;
          try {
            await bot.telegram.sendMessage(a.userId, text, { parse_mode: 'Markdown' });
            try {
              await alertsArchiveCollection.insertOne({
                ...a,
                firedAt: new Date(),
                firedPrice: cur,
                archivedReason: 'fired',
                archivedAt: new Date()
              });
            } catch (e) {}
            await alertsCollection.deleteOne({ _id: a._id });
            invalidateUserAlertsCache(a.userId);
          } catch (err) {
            try {
              const code = err?.response?.error_code;
              const description = err?.response?.description || String(err?.message || err);
              if (code === 403 || /bot was blocked/i.test(description)) {
                try {
                  await usersCollection.updateOne({ userId: a.userId }, { $set: { botBlocked: true, botBlockedAt: new Date() } }, { upsert: true });
                } catch (e) {}
                try {
                  await alertsArchiveCollection.insertOne({
                    ...a,
                    archivedAt: new Date(),
                    archivedReason: 'bot_blocked',
                    sendError: description
                  });
                } catch (e) {}
                await alertsCollection.deleteOne({ _id: a._id }).catch(()=>{});
                invalidateUserAlertsCache(a.userId);
              } else {
                console.error('bg check error', err);
              }
            } catch (ee) { console.error('bg check error handling failed', ee); }
          }
        }
      }

      if (allAlertsCache && typeof allAlertsCache === 'object') allAlertsCache.time = 0;
    } catch (e) {
      console.error('bg check error', e);
    }
  }, BG_CHECK_INTERVAL);
}
