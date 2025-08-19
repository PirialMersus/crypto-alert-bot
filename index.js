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

const INACTIVE_DAYS = 30;
const DAY_MS = 24 * 60 * 60 * 1000;

const TICKERS_TTL = 10_000;
const CACHE_TTL = 20_000;
const BG_CHECK_INTERVAL = 60_000;

const AXIOS_TIMEOUT = 7_000;
const AXIOS_RETRIES = 2;

const POPULAR_COINS = ['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE'];

const ENTRIES_PER_PAGE = 20;
const TELEGRAM_MAX_MESSAGE = 3800;

const RECENT_SYMBOLS_MAX = 20;

const DELETE_MENU_LABEL = '❌ Удалить пару № ...';
const DELETE_LABEL_TARGET_LEN = DELETE_MENU_LABEL.length;

const KYIV_TZ = 'Europe/Kyiv';
const IMAGE_FETCH_HOUR = 6;
const PREPARE_SEND_HOUR = 7;
const RETRY_INTERVAL_MS = 5 * 60 * 1000;
const MAX_RETRY_ATTEMPTS = 12;
const ONLINE_WINDOW_MS = 10 * 60 * 1000;
const QUOTE_CAPTION_MAX = 1024;
const MESSAGE_TEXT_MAX = 4000;

const bot = new Telegraf(BOT_TOKEN);
bot.use(session());
bot.use((ctx, next) => { if (!ctx.session) ctx.session = {}; return next(); });

const app = express();
app.get('/', (_req, res) => res.send('Бот работает!'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`HTTP server on ${PORT}`));

const client = new MongoClient(MONGO_URI);
let alertsCollection, usersCollection, lastViewsCollection, dailyMotivationCollection, dailyQuoteRetryCollection, pendingDailySendsCollection;

async function ensureIndexes(db) {
  try {
    await db.collection('alerts').createIndex({ userId: 1 });
    await db.collection('alerts').createIndex({ symbol: 1 });
    await db.collection('alerts').createIndex({ userId: 1, symbol: 1 });
    await db.collection('users').createIndex({ userId: 1 }, { unique: true });
    await db.collection('users').createIndex({ lastActive: 1 });
    await db.collection('last_alerts_view').createIndex({ userId: 1, symbol: 1 }, { unique: true });

    await db.collection('daily_motivation').createIndex({ date: 1 }, { unique: true });
    await db.collection('daily_quote_retry').createIndex({ date: 1 }, { unique: true });
    await db.collection('pending_daily_sends').createIndex({ userId: 1, date: 1 }, { unique: true });
  } catch (e) { console.error('ensureIndexes error', e); }
}

async function connectToMongo() {
  await client.connect();
  const db = client.db();
  alertsCollection = db.collection('alerts');
  usersCollection = db.collection('users');
  lastViewsCollection = db.collection('last_alerts_view');
  dailyMotivationCollection = db.collection('daily_motivation');
  dailyQuoteRetryCollection = db.collection('daily_quote_retry');
  pendingDailySendsCollection = db.collection('pending_daily_sends');
  await ensureIndexes(db);
  console.log('Connected to MongoDB and indexes are ready');
}
await connectToMongo();

const tickersCache = { time: 0, map: new Map() };
const pricesCache = new Map();
const alertsCache = new Map();
const lastViewsCache = new Map();
let allAlertsCache = { alerts: null, time: 0 };
let statsCache = { count: null, time: 0 };

const dailyCache = { date: null, doc: null, imageBuffer: null };

const httpClient = axios.create({ timeout: AXIOS_TIMEOUT, headers: { 'User-Agent': 'crypto-alert-bot/1.0' } });

async function httpGetWithRetry(url, retries = AXIOS_RETRIES, opts = {}) {
  let attempt = 0;
  let lastErr = null;
  while (attempt <= retries) {
    try { return await httpClient.get(url, opts); }
    catch (e) { lastErr = e; const delay = Math.min(500 * Math.pow(2, attempt), 2000); await new Promise(r => setTimeout(r, delay)); attempt++; }
  }
  throw lastErr;
}

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

async function getUserRecentSymbols(userId) {
  try {
    const u = await usersCollection.findOne({ userId }, { projection: { recentSymbols: 1 } });
    return Array.isArray(u?.recentSymbols) ? u.recentSymbols.slice(-6).reverse() : [];
  } catch { return []; }
}

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
          { $set: { userId: uid, username: ctx.from.username || null, lastActive: new Date(), language_code: ctx.from?.language_code || null }, $setOnInsert: { createdAt: new Date(), recentSymbols: [] } },
          { upsert: true }
        );
        usersActivityCache.set(uid, now);
      }
    }
  } catch (e) { console.error('activity middleware error', e); }
  return next();
});

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

function padLabel(text, targetLen = DELETE_LABEL_TARGET_LEN) {
  const cur = String(text);
  if (cur.length >= targetLen) return cur;
  const needed = targetLen - cur.length;
  return cur + '\u00A0'.repeat(needed);
}

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

  if (total <= ENTRIES_PER_PAGE) {
    let text = '📋 *Твои алерты:*\n\n';
    for (const e of entries) text += e.text;
    const buttons = [];
    buttons.push([{ text: DELETE_MENU_LABEL, callback_data: `show_delete_menu_0` }]);
    const valid = {};
    for (const s of uniqueSymbols) { const v = priceMap.get(s); if (Number.isFinite(v)) valid[s] = v; }
    if (Object.keys(valid).length) try { await setUserLastViews(userId, valid); } catch (e) {}
    return { pages: [{ text, buttons }], pageCount: 1 };
  }

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
    pages[p].text = pages[p].text + `Страница *${p+1}*/${pages.length}\n\n`;
    const rows = [];
    const nav = [];
    if (p > 0) nav.push({ text: '◀️ Предыдущая страница', callback_data: `alerts_page_${p-1}_view` });
    if (p < pages.length - 1) nav.push({ text: 'Следующая страница ▶️', callback_data: `alerts_page_${p+1}_view` });
    if (nav.length) rows.push(nav);
    rows.push([{ text: padLabel(DELETE_MENU_LABEL), callback_data: `show_delete_menu_${p}` }]);
    pages[p].buttons = rows;
  }
  const valid = {};
  for (const s of uniqueSymbols) { const v = priceMap.get(s); if (Number.isFinite(v)) valid[s] = v; }
  if (Object.keys(valid).length) try { await setUserLastViews(userId, valid); } catch (e) {}

  return { pages, pageCount: pages.length };
}

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

  let pageEntries = entries;
  if (typeof opts.sourcePage === 'number' && Number.isFinite(opts.sourcePage)) {
    const startIdx = opts.sourcePage * ENTRIES_PER_PAGE;
    const endIdx = Math.min(startIdx + ENTRIES_PER_PAGE, entries.length);
    pageEntries = entries.slice(startIdx, endIdx);
  }

  const inline = [];

  for (const e of pageEntries) {
    const cond = formatConditionShort(e.alert);
    const raw = `❌ ${e.index+1}: ${e.symbol} — ${cond}`;
    const pageToken = (opts.sourcePage === null) ? 'all' : String(opts.sourcePage);
    inline.push([{ text: padLabel(raw, Math.max(DELETE_LABEL_TARGET_LEN, 28)), callback_data: `del_${e.id}_p${pageToken}` }]);
  }

  if (typeof opts.sourcePage === 'number' && typeof opts.totalPages === 'number' && opts.totalPages > 1) {
    const nav = [];
    const sp = opts.sourcePage;
    if (sp > 0) nav.push({ text: '◀️ Предыдущая страница', callback_data: `alerts_page_${sp-1}_view` });
    if (sp < opts.totalPages - 1) nav.push({ text: 'Следующая страница ▶️', callback_data: `alerts_page_${sp+1}_view` });
    if (nav.length) inline.push(nav);
  }

  if (typeof opts.sourcePage === 'number' && Number.isFinite(opts.sourcePage)) {
    inline.push([{ text: '⬆️ Свернуть', callback_data: `back_to_alerts_p${opts.sourcePage}` }]);
  } else {
    inline.push([{ text: '⬆️ Свернуть', callback_data: 'back_to_alerts' }]);
  }
  if (pageEntries.length < entries.length) inline.push([{ text: '📂 Показать все алерты', callback_data: 'show_delete_menu_all' }]);

  const valid = {};
  for (const s of uniqueSymbols) { const v = priceMap.get(s); if (Number.isFinite(v)) valid[s] = v; }
  if (Object.keys(valid).length) try { await setUserLastViews(userId, valid); } catch (e) {}

  return inline;
}

async function pushRecentSymbol(userId, symbol) {
  try {
    await usersCollection.updateOne(
      { userId },
      { $pull: { recentSymbols: symbol } }
    );
    await usersCollection.updateOne(
      { userId },
      { $push: { recentSymbols: { $each: [symbol], $slice: -RECENT_SYMBOLS_MAX } } },
      { upsert: true }
    );
  } catch (err) {
    console.error('pushRecentSymbol error', err);
  }
}

function getMainMenu(userId) {
  const keyboard = [[{ text: '➕ Создать алерт' }, { text: '📋 Мои алерты' }], [{ text: '🌅 Прислать мотивацию' }]];
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

const QUOTABLE_RANDOM = 'https://api.quotable.io/random';
const ZEN_QUOTES = 'https://zenquotes.io/api/today';
const TYPEFIT_ALL = 'https://type.fit/api/quotes';
const UNSPLASH_RANDOM = 'https://source.unsplash.com/random/1200x800/?nature,landscape,forest,mountains,sea';
const PICSUM_RANDOM = 'https://picsum.photos/1200/800';

async function fetchQuoteQuotable() {
  try {
    const res = await httpGetWithRetry(QUOTABLE_RANDOM, 1);
    const d = res?.data;
    if (d?.content) return { text: d.content, author: d.author || '', source: 'quotable' };
  } catch (e) { console.warn('fetchQuoteQuotable failed', e?.message || e); }
  return null;
}
async function fetchQuoteZen() {
  try {
    const res = await httpGetWithRetry(ZEN_QUOTES, 1);
    const d = res?.data;
    if (Array.isArray(d) && d[0] && d[0].q) return { text: d[0].q, author: d[0].a || '', source: 'zen' };
  } catch (e) { console.warn('fetchQuoteZen failed', e?.message || e); }
  return null;
}
async function fetchQuoteTypefit() {
  try {
    const res = await httpGetWithRetry(TYPEFIT_ALL, 1);
    const arr = res?.data;
    if (Array.isArray(arr) && arr.length) {
      const cand = arr[Math.floor(Math.random() * arr.length)];
      if (cand && cand.text) return { text: cand.text, author: cand.author || '', source: 'typefit' };
    }
  } catch (e) { console.warn('fetchQuoteTypefit failed', e?.message || e); }
  return null;
}
async function fetchQuoteFromAny() {
  let q = null;
  q = await fetchQuoteQuotable().catch(()=>null);
  if (q) return q;
  q = await fetchQuoteZen().catch(()=>null);
  if (q) return q;
  q = await fetchQuoteTypefit().catch(()=>null);
  if (q) return q;
  return null;
}

async function fetchRandomImage() {
  const sources = [
    { name: 'unsplash', url: UNSPLASH_RANDOM },
    { name: 'picsum', url: PICSUM_RANDOM }
  ];
  for (const s of sources) {
    try {
      const res = await httpGetWithRetry(s.url, 1, { responseType: 'arraybuffer', maxRedirects: 10 });
      if (res && res.data) {
        const buf = Buffer.from(res.data);
        let finalUrl = null;
        try { finalUrl = res.request?.res?.responseUrl || null; } catch {}
        return { buffer: buf, url: finalUrl || s.url, source: s.name };
      }
    } catch (e) { console.warn('fetchRandomImage failed', s.name, e?.message || e); }
  }
  return null;
}

const LIBRE_ENDPOINTS = ['https://libretranslate.de/translate', 'https://libretranslate.com/translate'];
async function translateOrNull(text, targetLang) {
  if (!text) return null;
  if (!targetLang) return null;
  const t = String(targetLang).split('-')[0].toLowerCase();
  if (!t || t === 'en') return text;

  try {
    const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=${encodeURIComponent(t)}&dt=t&q=${encodeURIComponent(text)}`;
    const res = await httpGetWithRetry(url, 0);
    const data = res?.data;
    if (Array.isArray(data) && Array.isArray(data[0])) {
      const out = data[0].map(seg => (Array.isArray(seg) ? seg[0] : '')).join('');
      if (out && out.trim()) return out.trim();
    }
  } catch (e) { }

  for (const endpoint of LIBRE_ENDPOINTS) {
    try {
      const resp = await httpClient.post(endpoint, { q: text, source: 'auto', target: t, format: 'text' }, { headers: { 'Content-Type': 'application/json' }, timeout: 7000 });
      const d = resp?.data;
      const cand = d?.translatedText || d?.result || d?.translated_text || (typeof d === 'string' ? d : null);
      if (cand && String(cand).trim()) return String(cand).trim();
    } catch (e) { }
  }
  return null;
}

async function resolveUserLang(userId, ctxLang = null, ctxFromLang = null) {
  try {
    const u = await usersCollection.findOne({ userId }, { projection: { preferredLang: 1, language_code: 1 } });
    if (u?.preferredLang) return String(u.preferredLang).split('-')[0];
    if (ctxLang) return String(ctxLang).split('-')[0];
    if (ctxFromLang) return String(ctxFromLang).split('-')[0];
    if (u?.language_code) return String(u.language_code).split('-')[0];
  } catch (e) {}
  return 'ru';
}
async function fetchAndStoreDailyMotivation(dateStr) {
  try {
    const quote = await fetchQuoteFromAny().catch(()=>null);
    const img = await fetchRandomImage().catch(()=>null);
    const doc = {
      date: dateStr,
      quote: quote ? { text: quote.text, author: quote.author || '', source: quote.source || '' } : null,
      image: img ? { url: img.url, source: img.source } : null,
      createdAt: new Date()
    };
    await dailyMotivationCollection.updateOne({ date: dateStr }, { $setOnInsert: doc }, { upsert: true });
    const stored = await dailyMotivationCollection.findOne({ date: dateStr });
    dailyCache.date = dateStr;
    dailyCache.doc = stored;
    dailyCache.imageBuffer = null;

    if (!stored?.quote) {
      await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { date: dateStr, attempts: 0, nextAttemptAt: new Date(Date.now() + RETRY_INTERVAL_MS) } }, { upsert: true });
    } else {
      await dailyQuoteRetryCollection.deleteOne({ date: dateStr }).catch(()=>{});
    }

    console.log('fetchAndStoreDailyMotivation:', dateStr, 'quote:', stored?.quote ? 'yes' : 'no', 'image:', stored?.image ? 'yes':'no');
    return stored;
  } catch (e) {
    console.error('fetchAndStoreDailyMotivation error', e?.message || e);
    throw e;
  }
}

async function ensureDailyImageBuffer(dateStr) {
  if (dailyCache.date !== dateStr) {
    const doc = await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>null);
    if (!doc) return null;
    dailyCache.date = dateStr;
    dailyCache.doc = doc;
    dailyCache.imageBuffer = null;
  }
  if (dailyCache.imageBuffer) return dailyCache.imageBuffer;
  const doc = dailyCache.doc;
  if (doc?.image?.url) {
    try {
      const r = await httpGetWithRetry(doc.image.url, 1, { responseType: 'arraybuffer', maxRedirects: 10 });
      if (r && r.data) {
        dailyCache.imageBuffer = Buffer.from(r.data);
        return dailyCache.imageBuffer;
      }
    } catch (e) { console.warn('ensureDailyImageBuffer fetch stored url failed', e?.message || e); }
  }
  const got = await fetchRandomImage().catch(()=>null);
  if (got && got.buffer) {
    dailyCache.imageBuffer = got.buffer;
    try {
      if (got.url) {
        await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: { 'image.url': got.url, 'image.source': got.source } });
        dailyCache.doc.image = { url: got.url, source: got.source };
      }
    } catch (e) {}
    return dailyCache.imageBuffer;
  }
  return null;
}

async function buildWish() {
  return 'Хорошего дня!';
}
async function sendDailyToUser(userId, dateStr, opts = { disableNotification: false }) {
  try {
    let doc = dailyCache.date === dateStr ? dailyCache.doc : await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>null);
    if (!doc) {
      doc = await fetchAndStoreDailyMotivation(dateStr).catch(()=>null);
    }

    const retryDoc = await dailyQuoteRetryCollection.findOne({ date: dateStr }).catch(()=>null);
    const retriesExhausted = !!(retryDoc && retryDoc.exhausted);

    const buf = await ensureDailyImageBuffer(dateStr).catch(()=>null);

    let caption = '';
    if (doc?.quote?.text) {
      const lang = await resolveUserLang(userId);
      let final = doc.quote.text;
      if (lang && lang !== 'en') {
        const tr = await translateOrNull(final, lang).catch(()=>null);
        if (tr) final = tr;
      }
      caption = String(final).slice(0, QUOTE_CAPTION_MAX);
    } else if (retriesExhausted) {
      caption = String(await buildWish()).slice(0, QUOTE_CAPTION_MAX);
    } else {
      caption = '';
    }

    if (buf) {
      try {
        if (caption) await bot.telegram.sendPhoto(userId, { source: buf }, { caption, disable_notification: !!opts.disableNotification });
        else await bot.telegram.sendPhoto(userId, { source: buf }, { disable_notification: !!opts.disableNotification });
      } catch (e) {
        console.warn('sendDailyToUser sendPhoto failed', e?.message || e);
      }
    }

    if (doc?.quote?.text) {
      const lang = await resolveUserLang(userId);
      let final = doc.quote.text;
      if (lang && lang !== 'en') {
        const tr = await translateOrNull(final, lang).catch(()=>null);
        if (tr) final = tr;
      }
      if (!caption || caption !== String(final).slice(0, QUOTE_CAPTION_MAX)) {
        try { await bot.telegram.sendMessage(userId, (doc.quote.author ? `${final}\n— ${doc.quote.author}` : final).slice(0, MESSAGE_TEXT_MAX), { disable_notification: !!opts.disableNotification }); }
        catch (e) { console.warn('sendDailyToUser quote sendMessage failed', e?.message || e); }
      }
    } else if (retriesExhausted) {
      if (!caption) {
        const wish = await buildWish();
        try { await bot.telegram.sendMessage(userId, wish, { disable_notification: !!opts.disableNotification }); } catch (e) { }
      }
    } else {
    }

    return true;
  } catch (e) {
    console.error('sendDailyToUser error', e?.message || e);
    return false;
  }
}

async function processDailyQuoteRetry() {
  try {
    const now = new Date();
    const doc = await dailyQuoteRetryCollection.findOne({ nextAttemptAt: { $lte: now } });
    if (!doc) return;

    const dateStr = doc.date;
    const attempts = (doc.attempts || 0) + 1;
    console.log('processDailyQuoteRetry attempt', attempts, 'for', dateStr);

    const q = await fetchQuoteFromAny().catch(()=>null);
    if (q && q.text) {
      await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: { quote: { text: q.text, author: q.author || '', source: q.source || '' } } });
      await dailyQuoteRetryCollection.deleteOne({ date: dateStr });
      const stored = await dailyMotivationCollection.findOne({ date: dateStr });
      dailyCache.date = dateStr;
      dailyCache.doc = stored;
      dailyCache.imageBuffer = null;
      console.log('Quote fetched on retry for', dateStr);

      const cursor = pendingDailySendsCollection.find({ date: dateStr, sent: true, $or: [{ quoteSent: { $exists: false } }, { quoteSent: false }] });
      while (await cursor.hasNext()) {
        const p = await cursor.next();
        try {
          const uid = p.userId;
          const lang = await resolveUserLang(uid);
          let final = stored.quote.text;
          if (lang && lang !== 'en') {
            const tr = await translateOrNull(final, lang).catch(()=>null);
            if (tr) final = tr;
          }
          const out = stored.quote.author ? `${final}\n— ${stored.quote.author}` : final;
          await bot.telegram.sendMessage(uid, String(out).slice(0, MESSAGE_TEXT_MAX));
          await pendingDailySendsCollection.updateOne({ _id: p._id }, { $set: { quoteSent: true } });
        } catch (e) {
          console.warn('processDailyQuoteRetry: failed to send quote to', p.userId, e?.message || e);
        }
      }

      return;
    }

    if (attempts >= MAX_RETRY_ATTEMPTS) {
      await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { attempts, exhausted: true } });
      console.log('processDailyQuoteRetry exhausted for', dateStr);
    } else {
      await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { attempts, nextAttemptAt: new Date(Date.now() + RETRY_INTERVAL_MS) } });
      console.log('processDailyQuoteRetry rescheduled for', dateStr, 'attempts', attempts);
    }
  } catch (e) {
    console.error('processDailyQuoteRetry error', e?.message || e);
  }
}
setInterval(processDailyQuoteRetry, 60_000);

let lastSeenQuoteDate = null;
async function watchForNewQuotes() {
  try {
    const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ });
    const doc = await dailyMotivationCollection.findOne({ date: dateStr });
    if (!doc || !doc.quote || !doc.quote.text) return;
    if (lastSeenQuoteDate === dateStr) return;
    lastSeenQuoteDate = dateStr;
    const cursor = pendingDailySendsCollection.find({ date: dateStr, sent: true, $or: [{ quoteSent: { $exists: false } }, { quoteSent: false }] });
    while (await cursor.hasNext()) {
      const p = await cursor.next();
      try {
        const uid = p.userId;
        const lang = await resolveUserLang(uid);
        let final = doc.quote.text;
        if (lang && lang !== 'en') {
          const tr = await translateOrNull(final, lang).catch(()=>null);
          if (tr) final = tr;
        }
        const out = doc.quote.author ? `${final}\n— ${doc.quote.author}` : final;
        await bot.telegram.sendMessage(uid, String(out).slice(0, MESSAGE_TEXT_MAX));
        await pendingDailySendsCollection.updateOne({ _id: p._id }, { $set: { quoteSent: true } });
      } catch (e) {
        console.warn('watchForNewQuotes: failed to send quote to', p.userId, e?.message || e);
      }
    }
  } catch (e) { console.warn('watchForNewQuotes error', e?.message || e); }
}
setInterval(watchForNewQuotes, 30_000);

let lastFetchDate = null;
let lastPrepareDate = null;
let fetchRetryTimer = null;

async function sendDailyAllUsers(dateStr) {
  try {
    if (fetchRetryTimer) { clearInterval(fetchRetryTimer); fetchRetryTimer = null; }
    let doc = dailyCache.date === dateStr ? dailyCache.doc : await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>null);
    if (!doc) doc = await fetchAndStoreDailyMotivation(dateStr).catch(()=>null);
    const buf = await ensureDailyImageBuffer(dateStr).catch(()=>null);

    const cursor = usersCollection.find({}, { projection: { userId: 1, preferredLang: 1 } });
    while (await cursor.hasNext()) {
      const u = await cursor.next();
      if (!u || !u.userId) continue;
      try {
        const uid = u.userId;
        let caption = '';
        if (doc?.quote?.text) {
          const lang = await resolveUserLang(uid, u.preferredLang || null);
          let final = doc.quote.text;
          if (lang && lang !== 'en') {
            const tr = await translateOrNull(final, lang).catch(()=>null);
            if (tr) final = tr;
          }
          caption = String(final).slice(0, QUOTE_CAPTION_MAX);
        } else {
          caption = String(await buildWish()).slice(0, QUOTE_CAPTION_MAX);
        }

        if (buf) {
          if (caption) await bot.telegram.sendPhoto(uid, { source: buf }, { caption, disable_notification: true });
          else await bot.telegram.sendPhoto(uid, { source: buf }, { disable_notification: true });
        } else if (caption) {
          await bot.telegram.sendMessage(uid, caption, { disable_notification: true });
        }

        await pendingDailySendsCollection.updateOne({ userId: u.userId, date: dateStr }, { $set: { sent: true, sentAt: new Date(), quoteSent: !!doc?.quote?.text } }, { upsert: true });
      } catch (e) {
        try {
          await pendingDailySendsCollection.updateOne({ userId: u.userId, date: dateStr }, { $setOnInsert: { userId: u.userId, date: dateStr, sent: false, createdAt: new Date() } }, { upsert: true });
        } catch (err) {}
      }
      await new Promise(r => setTimeout(r, 40));
    }

    console.log('sendDailyAllUsers completed for', dateStr);
  } catch (e) { console.error('sendDailyAllUsers error', e); }
}

async function dailyScheduleChecker() {
  try {
    const now = new Date();
    const kyivNow = new Date(now.toLocaleString('en-US', { timeZone: KYIV_TZ }));
    const hour = kyivNow.getHours();
    const dateStr = kyivNow.toLocaleDateString('sv-SE', { timeZone: KYIV_TZ });

    if (hour === IMAGE_FETCH_HOUR && lastFetchDate !== dateStr) {
      try {
        await fetchAndStoreDailyMotivation(dateStr).catch(()=>null);
      } catch (e) { console.warn('dailyScheduleChecker fetch error', e?.message || e); }
      lastFetchDate = dateStr;

      const sendTime = new Date(kyivNow);
      sendTime.setHours(PREPARE_SEND_HOUR, 0, 0, 0);
      const nowTs = kyivNow.getTime();
      if (nowTs < sendTime.getTime()) {
        if (fetchRetryTimer) clearInterval(fetchRetryTimer);
        fetchRetryTimer = setInterval(async () => {
          try {
            const nowInner = new Date();
            const kyivNowInner = new Date(nowInner.toLocaleString('en-US', { timeZone: KYIV_TZ }));
            if (kyivNowInner.getTime() >= sendTime.getTime()) { clearInterval(fetchRetryTimer); fetchRetryTimer = null; return; }
            const stored = await fetchAndStoreDailyMotivation(dateStr).catch(()=>null);
            if (stored && stored.quote && stored.image) { clearInterval(fetchRetryTimer); fetchRetryTimer = null; }
          } catch (err) { }
        }, RETRY_INTERVAL_MS);
      }
    }

    if (hour === PREPARE_SEND_HOUR && lastPrepareDate !== dateStr) {
      lastPrepareDate = dateStr;
      try {
        await sendDailyAllUsers(dateStr);
      } catch (e) { console.error('dailyScheduleChecker send error', e); }
    }
  } catch (e) { console.error('dailyScheduleChecker main error', e?.message || e); }
}
setInterval(dailyScheduleChecker, 60_000);

bot.use(async (ctx, next) => {
  try {
    const uid = ctx.from?.id;
    if (!uid) return next();
    const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ });
    try {
      const pending = await pendingDailySendsCollection.findOne({ userId: uid, date: dateStr, sent: false });
      if (pending) {
        const ok = await sendDailyToUser(uid, dateStr, { disableNotification: false });
        if (ok) {
          await pendingDailySendsCollection.updateOne({ _id: pending._id }, { $set: { sent: true, sentAt: new Date() } });
        }
      }
    } catch (e) { }
  } catch (e) { }
  return next();
});

bot.hears('🌅 Прислать мотивацию', async (ctx) => {
  try {
    const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ });
    await sendDailyToUser(ctx.from.id, dateStr);
    await pendingDailySendsCollection.updateOne({ userId: ctx.from.id, date: dateStr }, { $set: { sent: true, sentAt: new Date(), quoteSent: true } }, { upsert: true });
  } catch (e) {
    console.error('motivation button error', e);
    try { await ctx.reply('Ошибка при отправке мотивации'); } catch {}
  }
});

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

bot.action(/show_delete_menu_(\d+)/, async (ctx) => {
  try {
    await ctx.answerCbQuery();
    const sourcePage = parseInt(ctx.match[1], 10);

    const viewQuick = await renderAlertsList(ctx.from.id, { fast: true });
    const totalPages = viewQuick.pageCount || 1;

    const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage, totalPages });
    try {
      await ctx.editMessageReplyMarkup({ inline_keyboard: inline });
    } catch (err) {
      try {
        const originalText = ctx.update.callback_query.message?.text || 'Твои алерты';
        await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } });
      } catch (e) {
        console.error('fallback reply failed', e);
      }
    }

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

bot.action(/back_to_alerts(?:_p(\d+))?/, async (ctx) => {
  try {
    await ctx.answerCbQuery();
    const requestedPage = ctx.match && ctx.match[1] ? Math.max(0, parseInt(ctx.match[1], 10)) : 0;

    const { pages } = await renderAlertsList(ctx.from.id, { fast: true });
    const idx = Math.min(requestedPage, Math.max(0, pages.length - 1));
    const p = pages[idx] || pages[0];

    try {
      await ctx.editMessageText(p.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: p.buttons } });
    } catch {
      await ctx.reply(p.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: p.buttons } });
    }

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

bot.on('callback_query', async (ctx) => {
  try {
    const data = ctx.callbackQuery?.data;
    if (!data) return ctx.answerCbQuery();

    const m = data.match(/^del_([0-9a-fA-F]{24})_p(all|\d+)$/);
    const mLegacy = !m && data.startsWith('del_') ? data.match(/^del_([0-9a-fA-F]{24})$/) : null;

    if (m || mLegacy) {
      const id = (m ? m[1] : mLegacy[1]);
      const token = m ? m[2] : null;

      const doc = await alertsCollection.findOne({ _id: new ObjectId(id) });
      if (!doc) {
        await ctx.answerCbQuery('Алерт не найден');
        return;
      }

      let sourcePage = null;
      if (token) {
        sourcePage = (token === 'all') ? null : Math.max(0, parseInt(token, 10));
      } else {
        try {
          const alertsBefore = await getUserAlertsCached(ctx.from.id);
          const idxBefore = alertsBefore.findIndex(a => String(a._id) === String(doc._id) || a._id?.toString() === id);
          if (idxBefore >= 0) sourcePage = Math.floor(idxBefore / ENTRIES_PER_PAGE);
          else sourcePage = 0;
        } catch (e) {
          sourcePage = 0;
        }
      }

      await alertsCollection.deleteOne({ _id: new ObjectId(id) });
      invalidateUserAlertsCache(ctx.from.id);

      const alertsAfter = await getUserAlertsCached(ctx.from.id);
      const computedTotalPages = Math.max(1, Math.ceil((alertsAfter?.length || 0) / ENTRIES_PER_PAGE));

      if (sourcePage !== null) {
        sourcePage = Math.max(0, Math.min(sourcePage, computedTotalPages - 1));
      }

      const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage, totalPages: (sourcePage === null ? null : computedTotalPages) });

      if (!inline || inline.length === 0) {
        try { await ctx.editMessageText('У тебя больше нет активных алертов.', { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [] } }); } catch {}
        await ctx.answerCbQuery('Алерт удалён');
        return;
      }

      try {
        await ctx.editMessageReplyMarkup({ inline_keyboard: inline });
      } catch (err) {
        try {
          const originalText = ctx.update.callback_query.message?.text || 'Твои алерты';
          await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } });
        } catch (e) { console.error('fallback after delete failed', e); }
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

bot.launch().then(() => console.log('Bot started'));
