// crypto-bot/index.js
// Упрощённая версия: телеграм-бот с алертами и опциональным стоп-лоссом.
// Поддержка массовых котировок (allTickers) + level1 fallback.
// Показ typing только для "📋 Мои алерты".

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

// По твоей просьбе: короткие TTL чтобы данные были более свежими
const TICKERS_TTL = 10_000;   // ms — allTickers TTL
const CACHE_TTL = 20_000;    // ms — общий короткий кеш для быстрых данных
const BG_CHECK_INTERVAL = 60_000; // ms — фоновая проверка алертов

const AXIOS_TIMEOUT = 7_000;
const AXIOS_RETRIES = 2;

const POPULAR_COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE'];

// Telegram limits & pagination policy
const TELEGRAM_MAX_MESSAGE = 3800; // safety margin (Telegram ~4096). Use 3800 to be safe.
const PAGINATE_THRESHOLD = 20;     // включать пагинацию только если алертов больше этой величины
const ENTRIES_PER_PAGE = 20;       // при пагинации — сколько записей на страницу (можно менять)
const MAX_ENTRIES_PER_PAGE = 50;   // защитный максимум — чтобы не создавать огромные страницы

// ---------- Инициализация ----------
const bot = new Telegraf(BOT_TOKEN);

bot.use(session());
bot.use((ctx, next) => {
  if (!ctx.session) ctx.session = {};
  return next();
});

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
  } catch (e) {
    console.error('ensureIndexes error', e);
  }
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

// ---------- Кэши ----------
const tickersCache = { time: 0, map: new Map() }; // allTickers cache
const pricesCache = new Map();                    // symbol -> { price, time }
const alertsCache = new Map();                    // userId -> { alerts, time }
const lastViewsCache = new Map();                 // userId -> { symbol: lastPrice }
let allAlertsCache = { alerts: null, time: 0 };

// simple stats cache
let statsCache = { count: null, time: 0 };

// ---------- HTTP client with retries ----------
const httpClient = axios.create({ timeout: AXIOS_TIMEOUT, headers: { 'User-Agent': 'crypto-alert-bot/1.0' } });

async function httpGetWithRetry(url, retries = AXIOS_RETRIES) {
  let attempt = 0;
  let lastErr = null;
  while (attempt <= retries) {
    try {
      return await httpClient.get(url);
    } catch (e) {
      lastErr = e;
      const delay = Math.min(500 * Math.pow(2, attempt), 2000);
      await new Promise(r => setTimeout(r, delay));
      attempt++;
    }
  }
  throw lastErr;
}

// ---------- KuCoin: allTickers + level1 ----------
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
  } catch (e) {
    console.error('refreshAllTickers error:', e?.message || e);
    return tickersCache.map;
  }
}

const pricePromises = new Map();
async function getPriceLevel1(symbol) {
  const cached = pricesCache.get(symbol);
  if (cached && (Date.now() - cached.time) < CACHE_TTL) return cached.price;
  if (pricePromises.has(symbol)) return await pricePromises.get(symbol);

  const p = httpGetWithRetry(`https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${symbol}`)
    .then(res => {
      const price = Number(res?.data?.data?.price);
      if (Number.isFinite(price)) {
        pricesCache.set(symbol, { price, time: Date.now() });
        return price;
      }
      return null;
    })
    .catch(err => {
      console.error('getPriceLevel1 error for', symbol, err?.message || err);
      return null;
    })
    .finally(() => pricePromises.delete(symbol));

  pricePromises.set(symbol, p);
  return await p;
}

async function getPrice(symbol) {
  const map = await refreshAllTickers();
  if (map.has(symbol)) return map.get(symbol);
  return await getPriceLevel1(symbol);
}

// Быстрый — использует текущие кэши и не ждёт долго
async function getPriceFast(symbol) {
  if (tickersCache.map.has(symbol) && (Date.now() - tickersCache.time) < TICKERS_TTL * 2) {
    if (Date.now() - tickersCache.time >= TICKERS_TTL) refreshAllTickers().catch(()=>{});
    return tickersCache.map.get(symbol);
  }
  const lvl1 = await getPriceLevel1(symbol);
  refreshAllTickers().catch(()=>{});
  return lvl1;
}

// ---------- Простейшие CRUD-кеш вспомогательные ----------
async function getUserAlertsCached(userId) {
  const now = Date.now();
  const c = alertsCache.get(userId);
  if (c && (now - c.time) < CACHE_TTL) return c.alerts;
  const alerts = await alertsCollection.find({ userId }).toArray();
  alertsCache.set(userId, { alerts, time: now });
  return alerts;
}
function invalidateUserAlertsCache(userId) {
  alertsCache.delete(userId);
  allAlertsCache.time = 0;
}
async function getAllAlertsCached() {
  const now = Date.now();
  if (allAlertsCache.alerts && (now - allAlertsCache.time) < CACHE_TTL) return allAlertsCache.alerts;
  const all = await alertsCollection.find({}).toArray();
  allAlertsCache = { alerts: all, time: Date.now() };
  return all;
}

// last views (упрощённо — читаем/пишем прямо)
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

// ---------- Middleware активности ----------
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
  } catch (e) {
    console.error('activity middleware error', e);
  }
  return next();
});

// ---------- Форматирование ----------
const safeBold = t => `*${t}*`;
function formatPercentRemaining(condition, targetPrice, currentPrice) {
  if (typeof currentPrice !== 'number' || typeof targetPrice !== 'number') return '';
  const diff = condition === '>' ? targetPrice - currentPrice : currentPrice - targetPrice;
  if ((condition === '>' && currentPrice < targetPrice) || (condition === '<' && currentPrice > targetPrice)) {
    return `(осталось ${(diff / targetPrice * 100).toFixed(2)}% до срабатывания)`;
  }
  return '';
}
function formatChangeWithIcons(change) {
  const sign = change >= 0 ? '+' : '';
  const value = `${sign}${change.toFixed(2)}%`;
  if (change > 0) return `${value} 📈`;
  if (change < 0) return `${value} 📉`;
  return `${value}`;
}

// компактное форматирование числа (убираем лишние нули)
function fmtNum(n) {
  if (!Number.isFinite(n)) return '—';
  if (n >= 1000 || n === Math.floor(n)) return String(Math.round(n));
  if (n >= 1) return n.toFixed(4).replace(/\.?0+$/, '');
  return n.toPrecision(6).replace(/\.?0+$/, '');
}

// ---------- Полезные функции для предложений ----------
async function getUserRecentSymbols(userId) {
  try {
    const u = await usersCollection.findOne({ userId }, { projection: { recentSymbols: 1 } });
    return Array.isArray(u?.recentSymbols) ? u.recentSymbols.slice(-6).reverse() : [];
  } catch {
    return [];
  }
}
async function pushUserRecentSymbol(userId, base) {
  try { await usersCollection.updateOne({ userId }, { $addToSet: { recentSymbols: base } }); } catch (e) { /* ignore */ }
}

// ---------- Форматирование одной записи алерта ----------
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

// ---------- Рендер списка алертов с условной пагинацией ----------
async function renderAlertsList(userId, options = { fast: false, includeDeleteButtons: false }) {
  const alerts = await getUserAlertsCached(userId);
  if (!alerts.length) return { pages: [{ text: 'У тебя нет активных алертов.', buttons: [] }], pageCount: 1 };

  const unique = [...new Set(alerts.map(a => a.symbol))];
  const priceMap = new Map();

  if (!options.fast) {
    await refreshAllTickers();
    for (const s of unique) priceMap.set(s, await getPrice(s));
  } else {
    for (const s of unique) {
      const p = tickersCache.map.get(s);
      if (Number.isFinite(p)) priceMap.set(s, p);
      else {
        const c = pricesCache.get(s);
        priceMap.set(s, c ? c.price : null);
      }
    }
    if (Date.now() - tickersCache.time >= TICKERS_TTL) refreshAllTickers().catch(()=>{});
  }

  const lastViews = await getUserLastViews(userId);

  // соберём все текстовые записи и метаданные
  const entries = alerts.map((a, idx) => {
    const cur = priceMap.get(a.symbol);
    const last = (typeof lastViews[a.symbol] === 'number') ? lastViews[a.symbol] : null;
    return { text: formatAlertEntry(a, idx, cur, last), id: a._id.toString(), symbol: a.symbol, index: idx };
  });

  // Если алертов меньше или равно порога — не делать "страницы" (оставляем одну страницу),
  // но всё равно следим за лимитом длины (на случай длинных записей) — если длина превышает, уменьшаем точность или отправляем в несколько частей.
  const pages = [];

  if (alerts.length <= PAGINATE_THRESHOLD) {
    // собираем одну страницу, но проверяем длину и при необходимости делаем несколько страниц
    let current = { text: '📋 *Твои алерты:*\n\n', entryIndexes: [], buttons: [] };
    for (let i = 0; i < entries.length; i++) {
      const e = entries[i];
      if ((current.text.length + e.text.length) > TELEGRAM_MAX_MESSAGE) {
        // если это случилось — завершаем текущую страницу и начинаем новую
        pages.push(current);
        current = { text: '📋 *Твои алерты:*\n\n', entryIndexes: [], buttons: [] };
      }
      current.text += e.text;
      current.entryIndexes.push(i);
    }
    pages.push(current);
  } else {
    // включаем постраничную разбивку по ENTRIES_PER_PAGE (и дополнительно следим за TELEGRAM_MAX_MESSAGE)
    let i = 0;
    while (i < entries.length) {
      let current = { text: '📋 *Твои алерты:*\n\n', entryIndexes: [], buttons: [] };
      let count = 0;
      while (i < entries.length && count < ENTRIES_PER_PAGE && (current.text.length + entries[i].text.length) <= TELEGRAM_MAX_MESSAGE) {
        current.text += entries[i].text;
        current.entryIndexes.push(i);
        i++; count++;
      }
      // если не добавилось ни одной записи из-за длины строки (очень длинная запись) — добавим хотя бы одну
      if (current.entryIndexes.length === 0 && i < entries.length) {
        current.text += entries[i].text;
        current.entryIndexes.push(i);
        i++;
      }
      pages.push(current);
    }
  }

  // Формируем кнопки и добавляем индикатор страницы в текст (перед кнопками)
  for (let p = 0; p < pages.length; p++) {
    // добавим индикатор "Страница X/Y" перед кнопками
    const pageNumberText = `Страница ${p+1}/${pages.length}\n\n`;
    pages[p].text = pages[p].text + pageNumberText;

    const rows = [];
    if (options.includeDeleteButtons) {
      // per-entry delete buttons
      for (const idx of pages[p].entryIndexes) {
        const e = entries[idx];
        rows.push([{ text: `❌ ${e.index+1}: ${e.symbol}`, callback_data: `del_${e.id}` }]);
      }
      // навигация (делим callback на del/view версии)
      const nav = [];
      if (p > 0) nav.push({ text: '◀️ Предыдущая страница', callback_data: `alerts_page_${p-1}_del` });
      if (p < pages.length - 1) nav.push({ text: 'Следующая страница ▶️', callback_data: `alerts_page_${p+1}_del` });
      if (nav.length) rows.push(nav);
      rows.push([{ text: '⬆️ Свернуть', callback_data: 'back_to_alerts' }]);
    } else {
      // режим просмотра
      const nav = [];
      if (p > 0) nav.push({ text: ' ◀️ Предыдущая страница', callback_data: `alerts_page_${p-1}_view` });
      if (p < pages.length - 1) nav.push({ text: 'Следующая страница ▶️', callback_data: `alerts_page_${p+1}_view` });
      if (nav.length) rows.push(nav);
      rows.push([{ text: '❌ Удалить пару № ...', callback_data: 'show_delete_menu' }]);
    }
    pages[p].buttons = rows;
  }

  // сохраним текущие цены как last views компактно
  const valid = {};
  for (const s of unique) {
    const v = priceMap.get(s);
    if (Number.isFinite(v)) valid[s] = v;
  }
  if (Object.keys(valid).length) {
    try { await setUserLastViews(userId, valid); } catch (e) { /* ignore */ }
  }

  return { pages, pageCount: pages.length };
}

// ---------- Main menu ----------
function getMainMenu(userId) {
  const keyboard = [[{ text: '➕ Создать алерт' }, { text: '📋 Мои алерты' }]];
  if (CREATOR_ID && String(userId) === String(CREATOR_ID)) keyboard.push([{ text: '👥 Количество активных пользователей' }]);
  return { reply_markup: { keyboard, resize_keyboard: true } };
}

// ---------- Helpers for stats ----------
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

// --- Мои алерты: показываем typing (только для этой команды) ---
bot.hears('📋 Мои алерты', async (ctx) => {
  try {
    try { await bot.telegram.sendChatAction(ctx.chat.id, 'typing'); } catch (_) {}
    const { pages } = await renderAlertsList(ctx.from.id, { includeDeleteButtons: false, fast: false });
    const first = pages[0];
    await ctx.replyWithMarkdown(first.text, { reply_markup: { inline_keyboard: first.buttons } });
  } catch (e) {
    console.error('Мои алерты error', e);
    ctx.reply('Ошибка при получении алертов.');
  }
});

// --- Количество активных пользователей (только создателю) ---
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
    try {
      activeCount = await countDocumentsWithTimeout({ lastActive: { $gte: cutoff } }, 7000);
    } catch (err) {
      console.error('Ошибка/таймаут при подсчёте активных пользователей:', err);
      return ctx.reply('Ошибка получения статистики (таймаут или проблема с БД). Попробуйте позже.');
    }

    statsCache = { count: activeCount, time: now };
    await ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${activeCount}`);
  } catch (e) {
    console.error('stats handler error', e);
    try { await ctx.reply('Ошибка получения статистики.'); } catch {}
  }
});

// Показать меню удаления (быстро, без typing)
bot.action('show_delete_menu', async (ctx) => {
  try {
    await ctx.answerCbQuery();
    const { pages } = await renderAlertsList(ctx.from.id, { includeDeleteButtons: true, fast: true });
    try {
      await ctx.editMessageText(pages[0].text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: pages[0].buttons } });
    } catch {
      await ctx.replyWithMarkdown(pages[0].text, { reply_markup: { inline_keyboard: pages[0].buttons } });
    }
    // обновим через асинхронный запрос более точно
    (async () => {
      try {
        const fresh = await renderAlertsList(ctx.from.id, { includeDeleteButtons: true, fast: false });
        try { await ctx.editMessageText(fresh.pages[0].text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: fresh.pages[0].buttons } }); } catch {}
      } catch {}
    })();
  } catch (e) {
    console.error('show_delete_menu error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

bot.action('back_to_alerts', async (ctx) => {
  try {
    await ctx.answerCbQuery();
    const { pages } = await renderAlertsList(ctx.from.id, { includeDeleteButtons: false, fast: true });
    try {
      await ctx.editMessageText(pages[0].text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: pages[0].buttons } });
    } catch {
      await ctx.replyWithMarkdown(pages[0].text, { reply_markup: { inline_keyboard: pages[0].buttons } });
    }
    (async () => {
      try {
        const fresh = await renderAlertsList(ctx.from.id, { includeDeleteButtons: false, fast: false });
        try { await ctx.editMessageText(fresh.pages[0].text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: fresh.pages[0].buttons } }); } catch {}
      } catch {}
    })();
  } catch (e) {
    console.error('back_to_alerts error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

// Навигация по страницам (view или del mode)
bot.action(/alerts_page_(\d+)_(view|del)/, async (ctx) => {
  try {
    await ctx.answerCbQuery();
    const pageIndex = parseInt(ctx.match[1], 10);
    const mode = ctx.match[2]; // 'view' or 'del'
    const includeDelete = mode === 'del';
    const { pages } = await renderAlertsList(ctx.from.id, { includeDeleteButtons: includeDelete, fast: false });
    const idx = Math.max(0, Math.min(pageIndex, pages.length - 1));
    const p = pages[idx];
    try {
      await ctx.editMessageText(p.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: p.buttons } });
    } catch (err) {
      await ctx.replyWithMarkdown(p.text, { reply_markup: { inline_keyboard: p.buttons } });
    }
  } catch (e) {
    console.error('alerts_page action error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

// callback delete (удаление конкретного алерта)
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

      // после удаления — покажем первую страницу меню удаления (без гарантии сохранения текущей позиции)
      const { pages } = await renderAlertsList(ctx.from.id, { includeDeleteButtons: true, fast: true });
      if (pages.length) {
        try { await ctx.editMessageText(pages[0].text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: pages[0].buttons } }); } catch { await ctx.replyWithMarkdown(pages[0].text, { reply_markup: { inline_keyboard: pages[0].buttons } }); }
      } else {
        try { await ctx.editMessageText('У тебя больше нет активных алертов.', { reply_markup: { inline_keyboard: [] } }); } catch {}
        await ctx.reply('Вы в главном меню', getMainMenu(ctx.from.id));
      }

      await ctx.answerCbQuery('Алерт удалён');
      return;
    }
    await ctx.answerCbQuery();
  } catch (e) {
    console.error('callback_query error', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

// ---------- Создание алертов (текстовый поток) ----------
bot.on('text', async (ctx) => {
  try {
    const step = ctx.session.step;
    const text = (ctx.message.text || '').trim();

    // если без шага и юзер прислал короткий тикер — начинает процесс
    if (!step && /^[A-Z0-9]{2,10}$/i.test(text)) {
      ctx.session = { step: 'symbol' };
    }
    if (!ctx.session.step) return;

    if (ctx.session.step === 'symbol') {
      const base = text.toUpperCase();
      const symbol = `${base}-USDT`;
      const price = await getPriceFast(symbol);
      if (Number.isFinite(price)) {
        await pushUserRecentSymbol(ctx.from.id, base);
        ctx.session.symbol = symbol;
        ctx.session.step = 'alert_condition';
        await ctx.replyWithMarkdown(`✅ Монета: *${symbol}*\nТекущая цена: *${fmtNum(price)}*\nВыбери направление:`, {
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
        await ctx.replyWithMarkdown(`✅ Алерт создан: *${ctx.session.symbol}* ${ctx.session.alertCondition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${fmtNum(ctx.session.alertPrice)}*\nТекущая цена: *${fmtNum(cp) ?? '—'}*`, getMainMenu(ctx.from.id));
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
      await ctx.replyWithMarkdown(`✅ Создана связка:\n🔔 *${ctx.session.symbol}* ${ctx.session.alertCondition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${fmtNum(ctx.session.alertPrice)}*\n🛑 SL (${slDir}) *${fmtNum(sl)}*\nТекущая: *${fmtNum(cp) ?? '—'}*`, getMainMenu(ctx.from.id));
      ctx.session = {};
      return;
    }
  } catch (e) {
    console.error('text handler error', e);
    try { await ctx.reply('Произошла ошибка, попробуй ещё раз.'); } catch {}
    ctx.session = {};
  }
});

// ---------- Фоновая проверка алертов ----------
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
  } catch (e) {
    console.error('bg check error', e);
  }
}, BG_CHECK_INTERVAL);

// ---------- Удаление неактивных пользователей ----------
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
  } catch (e) {
    console.error('removeInactive error', e);
  }
}
await removeInactive();
setInterval(removeInactive, DAY_MS);

// ---------- Старт ----------
bot.launch().then(() => console.log('Bot started'));
