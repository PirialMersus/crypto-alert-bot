// path: crypto-bot/index.js
// Телеграм-бот-алерт: пагинация по 20, компактный режим удаления (кнопки) — удаление по текущей странице (или все, если выбрано).

import { Telegraf, session } from 'telegraf';
import axios from 'axios';
import dotenv from 'dotenv';
import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';

dotenv.config();

const BOT_TOKEN = process.env.BOT_TOKEN;
const MONGO_URI = process.env.MONGO_URI;
const CREATOR_ID = process.env.CREATOR_ID ? parseInt(process.env.CREATOR_ID, 10) : null;

if (!BOT_TOKEN) throw new Error('BOT_TOKEN не задан в окружении');
if (!MONGO_URI) throw new Error('MONGO_URI не задан в окружении');

// ---------- Константы ----------
const INACTIVE_DAYS = 30;
const DAY_MS = 24 * 60 * 60 * 1000;

const TICKERS_TTL = 10_000;   // ms — allTickers TTL
const CACHE_TTL = 20_000;     // ms — общий короткий кеш
const BG_CHECK_INTERVAL = 60_000; // ms — фон

const AXIOS_TIMEOUT = 7_000;
const AXIOS_RETRIES = 2;

const POPULAR_COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE'];

// Pagination
const ENTRIES_PER_PAGE = 20;    // 20 записей на страницу
const TELEGRAM_MAX_MESSAGE = 3800; // safety margin

// recentSymbols limits
const RECENT_SYMBOLS_MAX = 20;

// Button padding target: используем эту метку как эталон ширины
const DELETE_MENU_LABEL = '❌ Удалить пару № ...';
const DELETE_LABEL_TARGET_LEN = DELETE_MENU_LABEL.length;

// ---------- Инициализация ----------
const bot = new Telegraf(BOT_TOKEN);
bot.use(session());
bot.use((ctx, next) => { if (!ctx.session) ctx.session = {}; return next(); });

const app = express();
app.get('/', (_req, res) => res.send('Бот работает!'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`HTTP server on ${PORT}`));

// ---------- MongoDB ----------
const client = new MongoClient(MONGO_URI);
let alertsCollection, usersCollection, lastViewsCollection;

async function ensureIndexes(db) {
  try {
    await db.collection('alerts').createIndex({ userId: 1 });
    await db.collection('alerts').createIndex({ symbol: 1 });
    await db.collection('alerts').createIndex({ userId: 1, symbol: 1 });
    await db.collection('users').createIndex({ userId: 1 }, { unique: true });
    await db.collection('users').createIndex({ lastActive: 1 });
    await db.collection('last_alerts_view').createIndex({ userId: 1, symbol: 1 }, { unique: true });
  } catch (e) { console.error('ensureIndexes error', e); }
}

async function connectToMongo() {
  await client.connect();
  const db = client.db();
  alertsCollection = db.collection('alerts');
  usersCollection = db.collection('users');
  lastViewsCollection = db.collection('last_alerts_view');
  await ensureIndexes(db);
  console.log('Connected to MongoDB and indexes are ready');
}
await connectToMongo();

// ---------- Caches ----------
const tickersCache = { time: 0, map: new Map() };
const pricesCache = new Map();
const alertsCache = new Map();
const lastViewsCache = new Map();
let allAlertsCache = { alerts: null, time: 0 };
let statsCache = { count: null, time: 0 };

// ---------- HTTP client with retries ----------
const httpClient = axios.create({ timeout: AXIOS_TIMEOUT, headers: { 'User-Agent': 'crypto-alert-bot/1.0' } });

async function httpGetWithRetry(url, retries = AXIOS_RETRIES) {
  let attempt = 0;
  let lastErr = null;
  while (attempt <= retries) {
    try { return await httpClient.get(url); }
    catch (e) { lastErr = e; const delay = Math.min(500 * Math.pow(2, attempt), 2000); await new Promise(r => setTimeout(r, delay)); attempt++; }
  }
  throw lastErr;
}

// ---------- KuCoin helpers ----------
async function refreshAllTickers() {
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
    tickersCache.time = Date.now();
    tickersCache.map = map;
    return map;
  } catch (e) { console.error('refreshAllTickers error:', e?.message || e); return tickersCache.map; }
}

const pricePromises = new Map();
async function getPriceLevel1(symbol) {
  const cached = pricesCache.get(symbol);
  if (cached && (Date.now() - cached.time) < CACHE_TTL) return cached.price;
  if (pricePromises.has(symbol)) return await pricePromises.get(symbol);

  const p = httpGetWithRetry(`https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${symbol}`)
    .then(res => {
      const price = Number(res?.data?.data?.price);
      if (Number.isFinite(price)) { pricesCache.set(symbol, { price, time: Date.now() }); return price; }
      return null;
    })
    .catch(err => { console.error('getPriceLevel1 error for', symbol, err?.message || err); return null; })
    .finally(() => pricePromises.delete(symbol));

  pricePromises.set(symbol, p);
  return await p;
}

async function getPrice(symbol) {
  const map = await refreshAllTickers();
  if (map.has(symbol)) return map.get(symbol);
  return await getPriceLevel1(symbol);
}

async function getPriceFast(symbol) {
  if (tickersCache.map.has(symbol) && (Date.now() - tickersCache.time) < TICKERS_TTL * 2) {
    if (Date.now() - tickersCache.time >= TICKERS_TTL) refreshAllTickers().catch(()=>{});
    return tickersCache.map.get(symbol);
  }
  const lvl1 = await getPriceLevel1(symbol);
  refreshAllTickers().catch(()=>{});
  return lvl1;
}

// ---------- CRUD caches ----------
async function getUserAlertsCached(userId) {
  const now = Date.now();
  const c = alertsCache.get(userId);
  if (c && (now - c.time) < CACHE_TTL) return c.alerts;
  const alerts = await alertsCollection.find({ userId }).toArray();
  alertsCache.set(userId, { alerts, time: now });
  return alerts;
}
function invalidateUserAlertsCache(userId) { alertsCache.delete(userId); allAlertsCache.time = 0; }
async function getAllAlertsCached() {
  const now = Date.now();
  if (allAlertsCache.alerts && (now - allAlertsCache.time) < CACHE_TTL) return allAlertsCache.alerts;
  const all = await alertsCollection.find({}).toArray();
  allAlertsCache = { alerts: all, time: Date.now() };
  return all;
}

// last views
async function getUserLastViews(userId) {
  const now = Date.now();
  const cached = lastViewsCache.get(userId);
  if (cached && (now - cached.time) < CACHE_TTL) return cached.map;
  const rows = await lastViewsCollection.find({ userId }).toArray();
  const map = Object.fromEntries(rows.map(r => [r.symbol, (typeof r.lastPrice === 'number') ? r.lastPrice : null]));
  lastViewsCache.set(userId, { map, time: now });
  return map;
}
async function setUserLastViews(userId, updates) {
  if (!updates || !Object.keys(updates).length) return;
  const ops = Object.entries(updates).map(([symbol, lastPrice]) => ({
    updateOne: { filter: { userId, symbol }, update: { $set: { lastPrice } }, upsert: true }
  }));
  await lastViewsCollection.bulkWrite(ops);
  lastViewsCache.delete(userId);
}

// activity middleware
const usersActivityCache = new Map();
bot.use(async (ctx, next) => {
  try {
    const uid = ctx.from?.id;
    if (uid) {
      const now = Date.now();
      const last = usersActivityCache.get(uid) || 0;
      if ((now - last) >= CACHE_TTL) {
        await usersCollection.updateOne(
          { userId: uid },
          { $set: { userId: uid, username: ctx.from.username || null, lastActive: new Date() }, $setOnInsert: { createdAt: new Date(), recentSymbols: [] } },
          { upsert: true }
        );
        usersActivityCache.set(uid, now);
      }
    }
  } catch (e) { console.error('activity middleware error', e); }
  return next();
});

// ---------- Formatting ----------
function fmtNum(n) {
  if (!Number.isFinite(n)) return '—';
  if (n >= 1000 || n === Math.floor(n)) return String(Math.round(n));
  if (n >= 1) return n.toFixed(4).replace(/\.?0+$/, '');
  return n.toPrecision(6).replace(/\.?0+$/, '');
}
function formatChangeWithIcons(change) {
  const sign = change >= 0 ? '+' : '';
  const value = `${sign}${change.toFixed(2)}%`;
  if (change > 0) return `${value} 📈`;
  if (change < 0) return `${value} 📉`;
  return `${value}`;
}

function formatAlertEntry(a, idx, cur, last) {
  const isSL = a.type === 'sl';
  const title = isSL ? `*${idx+1}. ${a.symbol} — 🛑 SL*` : `*${idx+1}. ${a.symbol}*`;
  const conditionStr = a.condition === '>' ? '⬆️ выше' : '⬇️ ниже';
  let percent = '';
  if (typeof cur === 'number' && typeof a.price === 'number') {
    const diff = a.condition === '>' ? (a.price - cur) : (cur - a.price);
    percent = ` (осталось ${(diff / a.price * 100).toFixed(2)}% до срабатывания)`;
  }
  let changeText = '';
  if (typeof last === 'number' && last > 0 && typeof cur === 'number') {
    changeText = `\nС последнего просмотра: ${formatChangeWithIcons(((cur - last)/last)*100)}`;
  }
  const curStr = fmtNum(cur);
  const priceStr = fmtNum(a.price);
  return `${title}\nТип: ${isSL ? '🛑 Стоп-лосс' : '🔔 Уведомление'}\nУсловие: ${conditionStr} *${priceStr}*\nТекущая: *${curStr}*${percent}${changeText}\n\n`;
}
function formatConditionShort(a) {
  const dir = a.condition === '>' ? '⬆️ выше' : '⬇️ ниже';
  return `${dir} ${fmtNum(a.price)}`;
}

// ---------- Utility: pad labels to target length using non-breaking spaces ----------
function padLabel(text, targetLen = DELETE_LABEL_TARGET_LEN) {
  // простая оценка длины символов — длина строки
  const cur = String(text);
  if (cur.length >= targetLen) return cur;
  const needed = targetLen - cur.length;
  // NBSP to visually pad
  return cur + '\u00A0'.repeat(needed);
}

// ---------- renderAlertsList ----------
async function renderAlertsList(userId, options = { fast: false }) {
  const alerts = await getUserAlertsCached(userId);
  if (!alerts || !alerts.length) return { pages: [{ text: 'У тебя нет активных алертов.', buttons: [] }], pageCount: 1 };

  const uniqueSymbols = [...new Set(alerts.map(a => a.symbol))];
  const priceMap = new Map();

  if (!options.fast) {
    await refreshAllTickers();
    for (const s of uniqueSymbols) priceMap.set(s, await getPrice(s));
  } else {
    for (const a of alerts) {
      const p = tickersCache.map.get(a.symbol);
      if (Number.isFinite(p)) priceMap.set(a.symbol, p);
      else {
        const c = pricesCache.get(a.symbol);
        if (c) priceMap.set(a.symbol, c.price);
      }
    }
    if (Date.now() - tickersCache.time >= TICKERS_TTL) refreshAllTickers().catch(()=>{});
  }

  const lastViews = await getUserLastViews(userId);

  const entries = alerts.map((a, idx) => {
    const cur = priceMap.get(a.symbol);
    const last = (typeof lastViews[a.symbol] === 'number') ? lastViews[a.symbol] : null;
    return { text: formatAlertEntry(a, idx, cur, last), id: a._id.toString(), symbol: a.symbol, index: idx, alert: a };
  });

  const total = entries.length;

  // Single page (no pagination)
  if (total <= ENTRIES_PER_PAGE) {
    let text = '📋 *Твои алерты:*\n\n';
    for (const e of entries) text += e.text;
    const buttons = [];
    // keep the big button as-is (this is reference label)
    buttons.push([{ text: DELETE_MENU_LABEL, callback_data: `show_delete_menu_0` }]);
    // save last views
    const valid = {};
    for (const s of uniqueSymbols) { const v = priceMap.get(s); if (Number.isFinite(v)) valid[s] = v; }
    if (Object.keys(valid).length) try { await setUserLastViews(userId, valid); } catch (e) {}
    return { pages: [{ text, buttons }], pageCount: 1 };
  }

  // Paginated pages
  const pages = [];
  for (let i = 0; i < entries.length; i += ENTRIES_PER_PAGE) {
    let text = '📋 *Твои алерты:*\n\n';
    const entryIndexes = [];
    for (let j = i; j < Math.min(i + ENTRIES_PER_PAGE, entries.length); j++) {
      text += entries[j].text;
      entryIndexes.push(j);
    }
    pages.push({ text, entryIndexes, buttons: [] });
  }

  for (let p = 0; p < pages.length; p++) {
    pages[p].text = pages[p].text + `Страница *${p+1}*/${pages.length}\n\n`; // bold current number
    const rows = [];
    const nav = [];
    if (p > 0) nav.push({ text: '◀️ Предыдущая страница', callback_data: `alerts_page_${p-1}_view` });
    if (p < pages.length - 1) nav.push({ text: 'Следующая страница ▶️', callback_data: `alerts_page_${p+1}_view` });
    if (nav.length) rows.push(nav);
    // pad the menu-open button to target length (keeps visual width)
    rows.push([{ text: padLabel(DELETE_MENU_LABEL), callback_data: `show_delete_menu_${p}` }]);
    pages[p].buttons = rows;
  }

  // save last views
  const valid = {};
  for (const s of uniqueSymbols) { const v = priceMap.get(s); if (Number.isFinite(v)) valid[s] = v; }
  if (Object.keys(valid).length) try { await setUserLastViews(userId, valid); } catch (e) {}

  return { pages, pageCount: pages.length };
}

// ---------- Новый хелпер: строит inline-кнопки удаления для данной страницы, без изменения текста ----------
async function buildDeleteInlineForUser(userId, opts = { fast: false, sourcePage: null, totalPages: null }) {
  const alerts = await getUserAlertsCached(userId);
  if (!alerts || !alerts.length) return [];

  const uniqueSymbols = [...new Set(alerts.map(a => a.symbol))];
  const priceMap = new Map();

  if (!opts.fast) {
    await refreshAllTickers();
    for (const s of uniqueSymbols) priceMap.set(s, await getPrice(s));
  } else {
    for (const a of alerts) {
      const p = tickersCache.map.get(a.symbol);
      if (Number.isFinite(p)) priceMap.set(a.symbol, p);
      else {
        const c = pricesCache.get(a.symbol);
        if (c) priceMap.set(a.symbol, c.price);
      }
    }
    if (Date.now() - tickersCache.time >= TICKERS_TTL) refreshAllTickers().catch(()=>{});
  }

  const lastViews = await getUserLastViews(userId);

  const entries = alerts.map((a, idx) => {
    const cur = priceMap.get(a.symbol);
    const last = (typeof lastViews[a.symbol] === 'number') ? lastViews[a.symbol] : null;
    return { id: a._id.toString(), symbol: a.symbol, index: idx, alert: a, cur, last };
  });

  // page window
  let pageEntries = entries;
  let startIdx = 0;
  let endIdx = entries.length;
  if (typeof opts.sourcePage === 'number' && Number.isFinite(opts.sourcePage)) {
    startIdx = opts.sourcePage * ENTRIES_PER_PAGE;
    endIdx = Math.min(startIdx + ENTRIES_PER_PAGE, entries.length);
    pageEntries = entries.slice(startIdx, endIdx);
  }

  const inline = [];

  // per-entry short delete buttons (one per row) — keep labels short so clients don't truncate
  for (const e of pageEntries) {
    const cond = formatConditionShort(e.alert);
    const raw = `❌ ${e.index+1}: ${e.symbol} — ${cond}`;
    // use padLabel to try to nudge width, but row is short (one button)
    inline.push([{ text: padLabel(raw, Math.max(DELETE_LABEL_TARGET_LEN, 28)), callback_data: `del_${e.id}` }]);
  }

  // nav/top rows similar to original renderDeletePage (preserve behaviour)
  if (typeof opts.sourcePage === 'number' && typeof opts.totalPages === 'number' && opts.totalPages > 1) {
    const nav = [];
    const sp = opts.sourcePage;
    if (sp > 0) nav.push({ text: '◀️ Предыдущая страница', callback_data: `alerts_page_${sp-1}_view` });
    if (sp < opts.totalPages - 1) nav.push({ text: 'Следующая страница ▶️', callback_data: `alerts_page_${sp+1}_view` });
    if (nav.length) inline.push(nav);
  }

  // bottom controls: collapse / show all (removed "удалить страницу")
  if (typeof opts.sourcePage === 'number' && Number.isFinite(opts.sourcePage)) {
    inline.push([{ text: '⬆️ Свернуть', callback_data: `back_to_alerts_p${opts.sourcePage}` }]);
  } else {
    inline.push([{ text: '⬆️ Свернуть', callback_data: 'back_to_alerts' }]);
  }
  if (pageEntries.length < entries.length) inline.push([{ text: '📂 Показать все алерты', callback_data: 'show_delete_menu_all' }]);

  // save last views compactly
  const valid = {};
  for (const s of uniqueSymbols) { const v = priceMap.get(s); if (Number.isFinite(v)) valid[s] = v; }
  if (Object.keys(valid).length) try { await setUserLastViews(userId, valid); } catch (e) {}

  return inline;
}

// ---------- renderDeletePage (compact buttons list) — оставляем для случаев, когда нужен отдельный "полный" view ----------
async function renderDeletePage(userId, opts = { fast: false, sourcePage: null, totalPages: null }) {
  const alerts = await getUserAlertsCached(userId);
  if (!alerts || !alerts.length) return { text: 'У тебя нет активных алертов.', buttons: [] };

  const uniqueSymbols = [...new Set(alerts.map(a => a.symbol))];
  const priceMap = new Map();

  if (!opts.fast) {
    await refreshAllTickers();
    for (const s of uniqueSymbols) priceMap.set(s, await getPrice(s));
  } else {
    for (const a of alerts) {
      const p = tickersCache.map.get(a.symbol);
      if (Number.isFinite(p)) priceMap.set(a.symbol, p);
      else {
        const c = pricesCache.get(a.symbol);
        if (c) priceMap.set(a.symbol, c.price);
      }
    }
    if (Date.now() - tickersCache.time >= TICKERS_TTL) refreshAllTickers().catch(()=>{});
  }

  const lastViews = await getUserLastViews(userId);

  const entries = alerts.map((a, idx) => {
    const cur = priceMap.get(a.symbol);
    const last = (typeof lastViews[a.symbol] === 'number') ? lastViews[a.symbol] : null;
    return { id: a._id.toString(), symbol: a.symbol, index: idx, alert: a, cur, last };
  });

  // Пагинация в меню удаления: если указана страница — берём только её диапазон
  let pageEntries = entries;
  let startIdx = 0;
  let endIdx = entries.length;
  if (typeof opts.sourcePage === 'number' && Number.isFinite(opts.sourcePage)) {
    startIdx = opts.sourcePage * ENTRIES_PER_PAGE;
    endIdx = Math.min(startIdx + ENTRIES_PER_PAGE, entries.length);
    pageEntries = entries.slice(startIdx, endIdx);
  }

  // header
  let header = `📋 *Твои алерты (удаление):* — ${entries.length}\n\n`;
  if (typeof opts.sourcePage === 'number' && typeof opts.totalPages === 'number' && opts.totalPages > 1) {
    header += `Страница *${opts.sourcePage+1}*/${opts.totalPages}\n\n`;
  } else if (typeof opts.sourcePage === 'number') {
    header += `Показаны алerты ${startIdx+1}–${endIdx}\n\n`;
  } else {
    header += `Показаны все алерты\n\n`;
  }

  // top nav row (immediately under indicator)
  const inline = [];
  if (typeof opts.sourcePage === 'number' && typeof opts.totalPages === 'number' && opts.totalPages > 1) {
    const topNav = [];
    const sp = opts.sourcePage;
    if (sp > 0) topNav.push({ text: '◀️ Предыдущая страница', callback_data: `alerts_page_${sp-1}_view` });
    if (sp < opts.totalPages - 1) topNav.push({ text: 'Следующая страница ▶️', callback_data: `alerts_page_${sp+1}_view` });
    if (topNav.length) inline.push(topNav);
  }

  // если на этой странице ничего нет
  if (!pageEntries.length) {
    // свернуть возвращает на ту страницу, если она задана
    if (typeof opts.sourcePage === 'number' && Number.isFinite(opts.sourcePage)) {
      inline.push([{ text: '⬆️ Свернуть', callback_data: `back_to_alerts_p${opts.sourcePage}` }]);
    } else {
      inline.push([{ text: '⬆️ Свернуть', callback_data: 'back_to_alerts' }]);
    }
    inline.push([{ text: '📂 Показать все алерты', callback_data: 'show_delete_menu_all' }]);
    return { text: header + '_На этой странице нет алертов._', buttons: inline };
  }

  // buttons — по одной кнопке на алерт. label: ❌ {№}: SYMBOL — {условие}
  for (const e of pageEntries) {
    const cond = formatConditionShort(e.alert);
    const raw = `❌ ${e.index+1}: ${e.symbol} — ${cond}`;
    inline.push([{ text: padLabel(raw), callback_data: `del_${e.id}` }]);
  }

  // нижний ряд: свернуть / показать все (удаление страницы убрано)
  if (typeof opts.sourcePage === 'number' && Number.isFinite(opts.sourcePage)) {
    inline.push([{ text: '⬆️ Свернуть', callback_data: `back_to_alerts_p${opts.sourcePage}` }]);
  } else {
    inline.push([{ text: '⬆️ Свернуть', callback_data: 'back_to_alerts' }]);
  }
  if (pageEntries.length < entries.length) {
    inline.push([{ text: '📂 Показать все алерты', callback_data: 'show_delete_menu_all' }]);
  }

  // save last views compactly
  const valid = {};
  for (const s of uniqueSymbols) { const v = priceMap.get(s); if (Number.isFinite(v)) valid[s] = v; }
  if (Object.keys(valid).length) try { await setUserLastViews(userId, valid); } catch (e) {}

  return { text: header, buttons: inline };
}

// ---------- Helpers (recentSymbols) ----------
async function pushRecentSymbol(userId, symbol) {
  try {
    // удалить существующий элемент (если есть)
    await usersCollection.updateOne(
      { userId },
      { $pull: { recentSymbols: symbol } }
    );

    // затем пушим в конец с ограничением длины (slice: -RECENT_SYMBOLS_MAX)
    await usersCollection.updateOne(
      { userId },
      { $push: { recentSymbols: { $each: [symbol], $slice: -RECENT_SYMBOLS_MAX } } },
      { upsert: true }
    );
  } catch (err) {
    console.error('pushRecentSymbol error', err);
  }
}

// ---------- Menu/helpers ----------
function getMainMenu(userId) {
  const keyboard = [[{ text: '➕ Создать алерт' }, { text: '📋 Мои алерты' }]];
  if (CREATOR_ID && String(userId) === String(CREATOR_ID)) keyboard.push([{ text: '👥 Количество активных пользователей' }]);
  return { reply_markup: { keyboard, resize_keyboard: true } };
}

async function countDocumentsWithTimeout(filter, ms = 7000) {
  if (!usersCollection) throw new Error('usersCollection не инициализирована');
  return await Promise.race([
    usersCollection.countDocuments(filter),
    new Promise((_, reject) => setTimeout(() => reject(new Error('mongo_timeout')), ms))
  ]);
}

// ---------- Handlers ----------
bot.start(ctx => {
  ctx.session = {};
  ctx.reply('Привет! Я бот-алерт для крипты.', getMainMenu(ctx.from?.id));
});

bot.hears('➕ Создать алерт', async (ctx) => {
  ctx.session = { step: 'symbol' };
  refreshAllTickers().catch(()=>{});
  const recent = await getUserRecentSymbols(ctx.from.id);
  const suggest = [...new Set([...recent, ...POPULAR_COINS])].slice(0,6).map(s=>({ text: s }));
  const kb = suggest.length ? [suggest, [{ text: '↩️ Отмена' }]] : [[{ text: '↩️ Отмена' }]];
  ctx.reply('Введи символ (например BTC) или нажми кнопку:', { reply_markup: { keyboard: kb, resize_keyboard: true } });
});

bot.hears('↩️ Отмена', ctx => { ctx.session = {}; ctx.reply('Отмена ✅', getMainMenu(ctx.from.id)); });

bot.hears('📋 Мои алерты', async (ctx) => {
  try {
    try { await bot.telegram.sendChatAction(ctx.chat.id, 'typing'); } catch (_) {}
    const { pages } = await renderAlertsList(ctx.from.id, { fast: false });
    const first = pages[0];
    await ctx.reply(first.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons } });
  } catch (e) {
    console.error('Мои алерты error', e);
    ctx.reply('Ошибка при получении алертов.');
  }
});

bot.hears('👥 Количество активных пользователей', async (ctx) => {
  try {
    if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) {
      return ctx.reply('У вас нет доступа к этой команде.');
    }

    const now = Date.now();
    if (statsCache.count !== null && (now - statsCache.time) < CACHE_TTL) {
      return ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${statsCache.count}`);
    }

    const cutoff = new Date(Date.now() - INACTIVE_DAYS * DAY_MS);
    let activeCount;
    try { activeCount = await countDocumentsWithTimeout({ lastActive: { $gte: cutoff } }, 7000); }
    catch (err) { console.error('Ошибка/таймаут при подсчёте активных пользователей:', err); return ctx.reply('Ошибка получения статистики (таймаут или проблема с БД). Попробуйте позже.'); }

    statsCache = { count: activeCount, time: now };
    await ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${activeCount}`);
  } catch (e) { console.error('stats handler error', e); try { await ctx.reply('Ошибка получения статистики.'); } catch {} }
});

// show_delete_menu_{pageIndex} -> add inline deletion buttons under the same message text (do not replace text)
bot.action(/show_delete_menu_(\d+)/, async (ctx) => {
  try {
    await ctx.answerCbQuery();
    const sourcePage = parseInt(ctx.match[1], 10);

    // compute total pages for indicator quickly
    const viewQuick = await renderAlertsList(ctx.from.id, { fast: true });
    const totalPages = viewQuick.pageCount || 1;

    // Build inline keyboard (only) and set it on the existing message (so text stays)
    const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage, totalPages });
    try {
      // edit only reply_markup so text remains
      await ctx.editMessageReplyMarkup({ inline_keyboard: inline });
    } catch (err) {
      // fallback: if edit reply markup failed (some clients), send a new message preserving text + inline
      try {
        // try to reuse original text if available
        const originalText = ctx.update.callback_query.message?.text || 'Твои алерты';
        await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } });
      } catch (e) {
        console.error('fallback reply failed', e);
      }
    }

    // async precise refresh: re-build inline with current prices and update reply_markup
    (async () => {
      try {
        const freshInline = await buildDeleteInlineForUser(ctx.from.id, { fast: false, sourcePage, totalPages });
        try { await ctx.editMessageReplyMarkup({ inline_keyboard: freshInline }); } catch {}
      } catch (err) { console.error('async refresh delete menu err', err); }
    })();
  } catch (e) {
    console.error('show_delete_menu error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

// раскрыть компактное меню на все записи — добавляем кнопки удаления, не заменяя текст
bot.action('show_delete_menu_all', async (ctx) => {
  try {
    await ctx.answerCbQuery();
    const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage: null, totalPages: null });
    try {
      await ctx.editMessageReplyMarkup({ inline_keyboard: inline });
    } catch (err) {
      try {
        const originalText = ctx.update.callback_query.message?.text || 'Твои алерты';
        await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } });
      } catch (e) { console.error('fallback show_delete_menu_all reply failed', e); }
    }

    (async () => {
      try {
        const freshInline = await buildDeleteInlineForUser(ctx.from.id, { fast: false, sourcePage: null, totalPages: null });
        try { await ctx.editMessageReplyMarkup({ inline_keyboard: freshInline }); } catch {}
      } catch (err) { console.error('async refresh show_delete_menu_all', err); }
    })();
  } catch (e) {
    console.error('show_delete_menu_all error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

// collapse back to alerts view (supports optional page index back_to_alerts_p{N})
bot.action(/back_to_alerts(?:_p(\d+))?/, async (ctx) => {
  try {
    await ctx.answerCbQuery();
    // если есть захваченное число — вернуть на ту страницу, иначе первая
    const requestedPage = ctx.match && ctx.match[1] ? Math.max(0, parseInt(ctx.match[1], 10)) : 0;

    // Получаем уже быстрый рендер
    const { pages } = await renderAlertsList(ctx.from.id, { fast: true });
    const idx = Math.min(requestedPage, Math.max(0, pages.length - 1));
    const p = pages[idx] || pages[0];

    try {
      await ctx.editMessageText(p.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: p.buttons } });
    } catch {
      await ctx.reply(p.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: p.buttons } });
    }

    // async точная подгрузка для актуализации
    (async () => {
      try {
        const fresh = await renderAlertsList(ctx.from.id, { fast: false });
        const fp = fresh.pages[idx] || fresh.pages[0];
        try { await ctx.editMessageText(fp.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: fp.buttons } }); } catch {}
      } catch (err) { console.error('async refresh back_to_alerts err', err); }
    })();
  } catch (e) {
    console.error('back_to_alerts error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

// page navigation (view mode)
bot.action(/alerts_page_(\d+)_view/, async (ctx) => {
  try {
    await ctx.answerCbQuery();
    const pageIndex = parseInt(ctx.match[1], 10);
    const { pages } = await renderAlertsList(ctx.from.id, { fast: false });
    const idx = Math.max(0, Math.min(pageIndex, pages.length - 1));
    const p = pages[idx];
    try { await ctx.editMessageText(p.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: p.buttons } }); }
    catch { await ctx.reply(p.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: p.buttons } }); }
  } catch (e) { console.error('alerts_page action error', e); try { await ctx.answerCbQuery('Ошибка'); } catch {} }
});

// delete specific alert (del_<id>) — удаляем и обновляем только reply_markup текущего сообщения (сохраняем текст)
bot.on('callback_query', async (ctx) => {
  try {
    const data = ctx.callbackQuery?.data;
    if (!data) return ctx.answerCbQuery();
    if (data.startsWith('del_')) {
      const id = data.replace('del_', '');
      const doc = await alertsCollection.findOne({ _id: new ObjectId(id) });
      if (!doc) {
        await ctx.answerCbQuery('Алерт не найден');
        return;
      }

      await alertsCollection.deleteOne({ _id: new ObjectId(id) });
      invalidateUserAlertsCache(ctx.from.id);

      // attempt to preserve page indicator if present in message text
      let sourcePage = null, totalPages = null;
      try {
        const msgText = ctx.update.callback_query.message?.text || '';
        const m = msgText.match(/Страница \*(\d+)\*\/(\d+)/);
        if (m) { sourcePage = parseInt(m[1], 10) - 1; totalPages = parseInt(m[2], 10); }
      } catch (e) {}

      // rebuild only inline keyboard for the same message
      const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage: (typeof sourcePage === 'number' ? sourcePage : null), totalPages: (typeof totalPages === 'number' ? totalPages : null) });

      if (!inline || inline.length === 0) {
        // no more alerts — edit message to say so
        try { await ctx.editMessageText('У тебя больше нет активных алертов.', { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [] } }); } catch {}
        await ctx.answerCbQuery('Алерт удалён');
        return;
      }

      try {
        await ctx.editMessageReplyMarkup({ inline_keyboard: inline });
      } catch (err) {
        // fallback: send new message with same text + inline
        try {
          const originalText = ctx.update.callback_query.message?.text || 'Твои алерты';
          await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } });
        } catch (e) { console.error('fallback after delete failed', e); }
      }

      await ctx.answerCbQuery('Алерт удалён');
      return;
    }

    // default answer
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('callback_query error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

// ---------- Creating alerts (text flow) ----------
bot.on('text', async (ctx) => {
  try {
    const step = ctx.session.step;
    const text = (ctx.message.text || '').trim();

    if (!step && /^[A-Z0-9]{2,10}$/i.test(text)) ctx.session = { step: 'symbol' };
    if (!ctx.session.step) return;

    if (ctx.session.step === 'symbol') {
      const base = text.toUpperCase();
      const symbol = `${base}-USDT`;
      const price = await getPriceFast(symbol);
      if (Number.isFinite(price)) {
        try { await pushRecentSymbol(ctx.from.id, base); } catch (e) { console.error('update recentSymbols failed', e); }
        ctx.session.symbol = symbol;
        ctx.session.step = 'alert_condition';
        await ctx.reply(`✅ Монета: *${symbol}*\nТекущая цена: *${fmtNum(price)}*\nВыбери направление:`, {
          parse_mode: 'Markdown',
          reply_markup: { keyboard: [[{ text: '⬆️ Когда выше' }, { text: '⬇️ Когда ниже' }], [{ text: '↩️ Отмена' }]], resize_keyboard:true }
        });
      } else {
        await ctx.reply('Пара не найдена на KuCoin. Попробуй другой символ.');
        ctx.session = {};
      }
      return;
    }

    if (ctx.session.step === 'alert_condition') {
      if (text === '⬆️ Когда выше') ctx.session.alertCondition = '>';
      else if (text === '⬇️ Когда ниже') ctx.session.alertCondition = '<';
      else { await ctx.reply('Выбери ⬆️ или ⬇️'); return; }
      ctx.session.step = 'alert_price';
      await ctx.reply('Введи цену уведомления:', { reply_markup: { keyboard: [[{ text: '↩️ Отмена' }]], resize_keyboard:true } });
      return;
    }

    if (ctx.session.step === 'alert_price') {
      const v = parseFloat(text);
      if (!Number.isFinite(v)) { await ctx.reply('Введите корректное число'); return; }
      ctx.session.alertPrice = v;
      ctx.session.step = 'ask_sl';
      const hint = ctx.session.alertCondition === '>' ? 'SL будет выше (для шорта — логика обратная)' : 'SL будет ниже';
      await ctx.reply(`Добавить стоп-лосс? ${hint}`, { reply_markup: { keyboard: [[{ text: '🛑 Добавить SL' }, { text: '⏭️ Без SL' }], [{ text: '↩️ Отмена' }]], resize_keyboard:true } });
      return;
    }

    if (ctx.session.step === 'ask_sl') {
      if (text === '⏭️ Без SL') {
        await alertsCollection.insertOne({ userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: ctx.session.alertPrice, type: 'alert' });
        invalidateUserAlertsCache(ctx.from.id);
        const cp = await getPriceFast(ctx.session.symbol);
        await ctx.reply(`✅ Алерт создан: *${ctx.session.symbol}* ${ctx.session.alertCondition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${fmtNum(ctx.session.alertPrice)}*\nТекущая цена: *${fmtNum(cp) ?? '—'}*`, { parse_mode: 'Markdown', ...getMainMenu(ctx.from.id) });
        ctx.session = {};
        return;
      }
      if (text === '🛑 Добавить SL') {
        ctx.session.step = 'sl_price';
        await ctx.reply('Введи цену стоп-лосса:', { reply_markup: { keyboard: [[{ text: '↩️ Отмена' }]], resize_keyboard:true } });
        return;
      }
      await ctx.reply('Выбери опцию: 🛑 Добавить SL / ⏭️ Без SL');
      return;
    }

    if (ctx.session.step === 'sl_price') {
      const sl = parseFloat(text);
      if (!Number.isFinite(sl)) { await ctx.reply('Введите корректное число SL'); return; }
      const groupId = new ObjectId().toString();
      const slDir = ctx.session.alertCondition === '<' ? 'ниже' : 'выше';
      await alertsCollection.insertMany([
        { userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: ctx.session.alertPrice, type: 'alert', groupId },
        { userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: sl, type: 'sl', slDir, groupId }
      ]);
      invalidateUserAlertsCache(ctx.from.id);
      const cp = await getPriceFast(ctx.session.symbol);
      await ctx.reply(`✅ Создана связка:\n🔔 *${ctx.session.symbol}* ${ctx.session.alertCondition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${fmtNum(ctx.session.alertPrice)}*\n🛑 SL (${slDir}) *${fmtNum(sl)}*\nТекущая: *${fmtNum(cp) ?? '—'}*`, { parse_mode: 'Markdown', ...getMainMenu(ctx.from.id) });
      ctx.session = {};
      return;
    }
  } catch (e) {
    console.error('text handler error', e);
    try { await ctx.reply('Произошла ошибка, попробуй ещё раз.'); } catch {}
    ctx.session = {};
  }
});

// ---------- Background check ----------
setInterval(async () => {
  try {
    const all = await getAllAlertsCached();
    if (!all.length) return;
    await refreshAllTickers().catch(()=>{});
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
        await bot.telegram.sendMessage(a.userId,
          `${isSL ? '🛑 *Сработал стоп-лосс!*' : '🔔 *Сработал алерт!*'}\nМонета: *${a.symbol}*\nЦена сейчас: *${fmtNum(cur)}*\nУсловие: ${a.condition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${fmtNum(a.price)}*`,
          { parse_mode: 'Markdown' }
        );
        await alertsCollection.deleteOne({ _id: a._id });
        invalidateUserAlertsCache(a.userId);
      }
    }
    allAlertsCache.time = 0;
  } catch (e) { console.error('bg check error', e); }
}, BG_CHECK_INTERVAL);

// ---------- Remove inactive ----------
async function removeInactive() {
  try {
    const cutoff = new Date(Date.now() - INACTIVE_DAYS * DAY_MS);
    const inactive = await usersCollection.find({ lastActive: { $lt: cutoff } }).project({ userId:1 }).toArray();
    if (!inactive.length) return;
    const ids = inactive.map(u => u.userId);
    await alertsCollection.deleteMany({ userId: { $in: ids } });
    await lastViewsCollection.deleteMany({ userId: { $in: ids } });
    await usersCollection.deleteMany({ userId: { $in: ids } });
    ids.forEach(id => alertsCache.delete(id));
    console.log(`Removed ${ids.length} inactive users`);
  } catch (e) { console.error('removeInactive error', e); }
}
await removeInactive();
setInterval(removeInactive, DAY_MS);

// ---------- Start ----------
bot.launch().then(() => console.log('Bot started'));

// ---------- Helpers ----------
async function getUserRecentSymbols(userId) {
  try {
    const u = await usersCollection.findOne({ userId }, { projection: { recentSymbols: 1 } });
    return Array.isArray(u?.recentSymbols) ? u.recentSymbols.slice(-6).reverse() : [];
  } catch { return []; }
}
