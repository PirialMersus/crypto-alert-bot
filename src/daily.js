// src/daily.js
import { httpGetWithRetry, httpClient } from './httpClient.js';
import { dailyCache } from './cache.js';
import { dailyMotivationCollection, dailyQuoteRetryCollection, pendingDailySendsCollection } from './db.js';
import { RETRY_INTERVAL_MS, QUOTE_CAPTION_MAX, MESSAGE_TEXT_MAX, KYIV_TZ } from './constants.js';
import { translateOrNull } from './translate.js';

const QUOTABLE_RANDOM = 'https://api.quotable.io/random';
const ZEN_QUOTES = 'https://zenquotes.io/api/today';
const TYPEFIT_ALL = 'https://type.fit/api/quotes';
const FORISMATIC_RU = 'https://api.forismatic.com/api/1.0/?method=getQuote&format=json&lang=ru';
const UNSPLASH_RANDOM = 'https://source.unsplash.com/random/1200x800/?nature,landscape,forest,mountains,sea';
const PICSUM_RANDOM = 'https://picsum.photos/1200/800';
const PICSUM_SEED = (seed) => `https://picsum.photos/seed/${encodeURIComponent(seed)}/1200/800`;
const PLACEHOLD = 'https://placehold.co/1200x800.png?text=Motivation';
const LOREMFLICKR = 'https://loremflickr.com/1200/800/nature,landscape';

async function tryGetArrayBuffer(url, opts = {}) {
  try {
    const res = await httpClient.get(url, { responseType: 'arraybuffer', maxRedirects: 10, timeout: 8000, ...opts });
    if (res && res.data) {
      const buf = Buffer.from(res.data);
      let finalUrl = null;
      try { finalUrl = res.request?.res?.responseUrl || null; } catch {}
      return { buffer: buf, url: finalUrl || url };
    }
  } catch (e) {}
  return null;
}

function isLikelyHTML(s) {
  if (!s || typeof s !== 'string') return false;
  const snippet = s.slice(0, 500).toLowerCase();
  if (/<\s*html|<!doctype|<meta|<title|<script|<\/html>/.test(snippet)) return true;
  const lt = (s.match(/</g) || []).length;
  const gt = (s.match(/>/g) || []).length;
  if (lt > 4 && gt > 4) return true;
  return false;
}

function detectSimpleLang(s) {
  if (!s || typeof s !== 'string') return 'en';
  try {
    if (/\p{Script=Cyrillic}/u.test(s)) return 'ru';
  } catch (e) {
    if (/[А-Яа-яЁё]/.test(s)) return 'ru';
  }
  return 'en';
}

export async function fetchQuoteQuotable() {
  try {
    const res = await httpGetWithRetry(QUOTABLE_RANDOM, 1);
    const d = res?.data;
    if (d?.content && !isLikelyHTML(d.content)) return { text: d.content, author: d.author || '', source: 'quotable' };
  } catch (e) {}
  return null;
}

export async function fetchQuoteZen() {
  try {
    const res = await httpGetWithRetry(ZEN_QUOTES, 1);
    const d = res?.data;
    if (Array.isArray(d) && d[0] && d[0].q && !isLikelyHTML(d[0].q)) return { text: d[0].q, author: d[0].a || '', source: 'zen' };
  } catch (e) {}
  return null;
}

export async function fetchQuoteTypefit() {
  try {
    const res = await httpGetWithRetry(TYPEFIT_ALL, 1);
    const arr = res?.data;
    if (Array.isArray(arr) && arr.length) {
      const cand = arr[Math.floor(Math.random() * arr.length)];
      const text = cand?.text || cand?.quote || cand?.content;
      if (text && !isLikelyHTML(text)) return { text: text, author: cand.author || '', source: 'typefit' };
    }
  } catch (e) {}
  return null;
}

export async function fetchQuoteForismatic() {
  try {
    const res = await httpGetWithRetry(FORISMATIC_RU, 1, { timeout: 7000 });
    const d = res?.data;
    if (!d) return null;
    const text = d.quoteText || d.quote || '';
    const author = d.quoteAuthor || d.author || '';
    if (text && String(text).trim() && !isLikelyHTML(text)) return { text: String(text).trim(), author: String(author || '').trim(), source: 'forismatic' };
  } catch (e) {}
  return null;
}

export async function fetchQuoteFromAny(attempts = 2) {
  for (let a = 0; a < attempts; a++) {
    let q = null;
    q = await fetchQuoteForismatic().catch(()=>null);
    if (q && q.text) return q;
    q = await fetchQuoteTypefit().catch(()=>null);
    if (q && q.text) return q;
    q = await fetchQuoteQuotable().catch(()=>null);
    if (q && q.text) return q;
    q = await fetchQuoteZen().catch(()=>null);
    if (q && q.text) return q;
    await new Promise(r => setTimeout(r, 300 * (a+1)));
  }
  return null;
}

export async function fetchRandomImage() {
  const sources = [
    { name: 'picsum-seed', fn: async () => await tryGetArrayBuffer(PICSUM_SEED(String(Date.now()))) },
    { name: 'picsum', fn: async () => await tryGetArrayBuffer(PICSUM_RANDOM) },
    { name: 'unsplash', fn: async () => await tryGetArrayBuffer(UNSPLASH_RANDOM) },
    { name: 'loremflickr', fn: async () => await tryGetArrayBuffer(LOREMFLICKR) },
    { name: 'placehold', fn: async () => await tryGetArrayBuffer(PLACEHOLD) }
  ];
  for (const s of sources) {
    try {
      const got = await s.fn();
      if (got && got.buffer && got.buffer.length > 0) return { buffer: got.buffer, url: got.url || s.name, source: s.name };
    } catch (e) {}
  }
  return null;
}

export async function fetchAndStoreDailyMotivation(dateStr, opts = { force: false }) {
  try {
    const quoteAttempts = (opts && opts.force) ? 3 : 2;
    const imgAttempts = (opts && opts.force) ? 3 : 1;
    let quote = null;
    for (let i = 0; i < quoteAttempts && !quote; i++) {
      quote = await fetchQuoteFromAny(1).catch(()=>null);
      if (!quote) await new Promise(r => setTimeout(r, 300 * (i+1)));
    }
    let img = null;
    for (let i = 0; i < imgAttempts && !img; i++) {
      img = await fetchRandomImage().catch(()=>null);
      if (!img) await new Promise(r => setTimeout(r, 200 * (i+1)));
    }

    let originalLang = 'en';
    if (quote && quote.text) originalLang = detectSimpleLang(quote.text);

    let translations = { en: null, ru: null, uk: null };
    let canonicalEn = null;

    if (quote && quote.text) {
      if (originalLang === 'en') {
        canonicalEn = quote.text;
        translations.en = canonicalEn;
      } else {
        const translatedToEn = await translateOrNull(quote.text, 'en').catch(()=>null);
        if (translatedToEn) {
          canonicalEn = translatedToEn;
          translations.en = translatedToEn;
          if (originalLang === 'ru') translations.ru = quote.text;
        } else {
          canonicalEn = quote.text;
          translations.en = quote.text;
          if (originalLang === 'ru') translations.ru = quote.text;
        }
      }
    }

    const doc = {
      date: dateStr,
      quote: quote ? {
        original: canonicalEn,
        author: quote.author || '',
        source: quote.source || '',
        originalLang: 'en',
        translations
      } : null,
      image: img ? { url: img.url, source: img.source } : null,
      createdAt: new Date()
    };

    if (opts && opts.force) {
      if (!doc.quote && !doc.image) throw new Error('fetchAndStoreDailyMotivation: force requested but unable to fetch quote or image from sources (network or sources failure).');
      await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: doc }, { upsert: true });
    } else {
      await dailyMotivationCollection.updateOne({ date: dateStr }, { $setOnInsert: doc }, { upsert: true });
    }

    const stored = await dailyMotivationCollection.findOne({ date: dateStr });
    dailyCache.date = dateStr;
    dailyCache.doc = stored;
    dailyCache.imageBuffer = null;

    if (!stored?.quote) {
      await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { date: dateStr, attempts: 0, nextAttemptAt: new Date(Date.now() + RETRY_INTERVAL_MS) } }, { upsert: true });
    } else {
      await dailyQuoteRetryCollection.deleteOne({ date: dateStr }).catch(()=>{});
    }

    return stored;
  } catch (e) {
    console.error('fetchAndStoreDailyMotivation error', e?.message || e);
    throw e;
  }
}

export async function ensureDailyImageBuffer(dateStr) {
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
      const r = await httpClient.get(doc.image.url, { responseType: 'arraybuffer', maxRedirects: 10, timeout: 8000 });
      if (r && r.data) {
        dailyCache.imageBuffer = Buffer.from(r.data);
        return dailyCache.imageBuffer;
      }
    } catch (e) {}
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

export async function sendDailyToUser(bot, userId, dateStr, opts = { disableNotification: true, forceRefresh: false }) {
  try {
    if (opts && opts.forceRefresh) {
      try { await fetchAndStoreDailyMotivation(dateStr, { force: true }).catch(()=>{}); } catch (e) {}
    }

    let doc = dailyCache.date === dateStr ? dailyCache.doc : await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>null);
    if (!doc) doc = await fetchAndStoreDailyMotivation(dateStr).catch(()=>null);

    const buf = await ensureDailyImageBuffer(dateStr).catch(()=>null);
    const lang = await import('./cache.js').then(m => m.resolveUserLang(userId)).catch(()=> 'ru');

    let quoteText = null;
    const storedQuote = doc?.quote || null;

    if (!storedQuote || !storedQuote.original) {
      const fallback = String(await import('./utils.js').then(m=>m.buildWish())).slice(0, QUOTE_CAPTION_MAX);
      if (buf) {
        try { await bot.telegram.sendPhoto(userId, { source: buf }, { caption: fallback, disable_notification: !!opts.disableNotification }); } catch (e) { await pendingDailySendsCollection.updateOne({ userId, date: dateStr }, { $set: { sent: false, createdAt: new Date(), permanentFail: true } }, { upsert: true }).catch(()=>{}); return false; }
      } else {
        try { await bot.telegram.sendMessage(userId, fallback, { disable_notification: !!opts.disableNotification }); } catch (e) { await pendingDailySendsCollection.updateOne({ userId, date: dateStr }, { $set: { sent: false, createdAt: new Date(), permanentFail: true } }, { upsert: true }).catch(()=>{}); return false; }
      }
      await pendingDailySendsCollection.updateOne({ userId, date: dateStr }, { $set: { sent: true, sentAt: new Date(), quoteSent: false, permanentFail: false } }, { upsert: true }).catch(()=>{});
      return true;
    }

    if (lang === 'en') {
      if (storedQuote.translations && storedQuote.translations.en && !isLikelyHTML(storedQuote.translations.en)) quoteText = storedQuote.translations.en;
      else if (storedQuote.original && !isLikelyHTML(storedQuote.original)) quoteText = storedQuote.original;
    } else if (lang === 'ru') {
      if (storedQuote.translations && storedQuote.translations.ru && !isLikelyHTML(storedQuote.translations.ru)) quoteText = storedQuote.translations.ru;
      else if (storedQuote.original && !isLikelyHTML(storedQuote.original)) {
        const tr = await translateOrNull(storedQuote.original, 'ru').catch(()=>null);
        if (tr && !isLikelyHTML(tr)) {
          quoteText = tr;
          try { await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: { ['quote.translations.ru']: tr } }, { upsert: false }); dailyCache.doc = await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>dailyCache.doc); } catch (e) {}
        } else {
          quoteText = storedQuote.original;
        }
      }
    } else {
      if (storedQuote.translations && storedQuote.translations.en && !isLikelyHTML(storedQuote.translations.en)) quoteText = storedQuote.translations.en;
      else if (storedQuote.original && !isLikelyHTML(storedQuote.original)) quoteText = storedQuote.original;
    }

    async function recordSendStatus(ok, quoteWasSent) {
      try {
        if (!pendingDailySendsCollection) return;
        if (ok) {
          await pendingDailySendsCollection.updateOne({ userId, date: dateStr }, { $set: { sent: true, sentAt: new Date(), quoteSent: !!quoteWasSent, permanentFail: false } }, { upsert: true });
        } else {
          await pendingDailySendsCollection.updateOne({ userId, date: dateStr }, { $set: { sent: false, createdAt: new Date(), permanentFail: true } }, { upsert: true });
        }
      } catch (e) {}
    }

    if (!quoteText) {
      const fallback = String(await import('./utils.js').then(m=>m.buildWish())).slice(0, QUOTE_CAPTION_MAX);
      if (buf) {
        try { await bot.telegram.sendPhoto(userId, { source: buf }, { caption: fallback, disable_notification: !!opts.disableNotification }); } catch (e) { await recordSendStatus(false, false); return false; }
      } else {
        try { await bot.telegram.sendMessage(userId, fallback, { disable_notification: !!opts.disableNotification }); } catch (e) { await recordSendStatus(false, false); return false; }
      }
      await recordSendStatus(true, false);
      return true;
    }

    const caption = String(quoteText).slice(0, QUOTE_CAPTION_MAX);
    if (buf) {
      try { await bot.telegram.sendPhoto(userId, { source: buf }, { caption, disable_notification: !!opts.disableNotification }); } catch (e) { await recordSendStatus(false, !!quoteText); return false; }
    } else {
      try { await bot.telegram.sendMessage(userId, caption, { disable_notification: !!opts.disableNotification }); } catch (e) { await recordSendStatus(false, !!quoteText); return false; }
    }

    if (storedQuote.author) {
      try {
        if (!caption.includes(storedQuote.author)) await bot.telegram.sendMessage(userId, `— ${storedQuote.author}`.slice(0, MESSAGE_TEXT_MAX), { disable_notification: !!opts.disableNotification });
      } catch (e) {}
    }

    await recordSendStatus(true, !!quoteText);
    return true;
  } catch (e) {
    try {
      if (pendingDailySendsCollection) await pendingDailySendsCollection.updateOne({ userId, date: dateStr }, { $set: { sent: false, createdAt: new Date(), permanentFail: true } }, { upsert: true });
    } catch (err) {}
    return false;
  }
}

export async function processDailyQuoteRetry(bot) {
  try {
    const now = new Date();
    const doc = await dailyQuoteRetryCollection.findOne({ nextAttemptAt: { $lte: now } });
    if (!doc) return;
    const dateStr = doc.date;
    const attempts = (doc.attempts || 0) + 1;
    const q = await fetchQuoteFromAny(2).catch(()=>null);
    if (q && q.text && !isLikelyHTML(q.text)) {
      const origLang = detectSimpleLang(q.text);
      let translations = { en: null, ru: null, uk: null };
      let canonicalEn = null;
      if (origLang === 'en') {
        canonicalEn = q.text;
        translations.en = canonicalEn;
      } else {
        const enT = await translateOrNull(q.text, 'en').catch(()=>null);
        if (enT) {
          canonicalEn = enT;
          translations.en = enT;
          if (origLang === 'ru') translations.ru = q.text;
        } else {
          canonicalEn = q.text;
          translations.en = q.text;
          if (origLang === 'ru') translations.ru = q.text;
        }
      }
      await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: { 'quote.original': canonicalEn, 'quote.author': q.author || '', 'quote.source': q.source || '', 'quote.translations': translations, 'quote.originalLang': 'en' } }, { upsert: true });
      await dailyQuoteRetryCollection.deleteOne({ date: dateStr });
      const stored = await dailyMotivationCollection.findOne({ date: dateStr });
      dailyCache.date = dateStr;
      dailyCache.doc = stored;
      dailyCache.imageBuffer = null;
      const cursor = pendingDailySendsCollection.find({ date: dateStr, sent: true, $and: [ { $or: [{ quoteSent: { $exists: false } }, { quoteSent: false }] }, { $or: [{ permanentFail: { $exists: false } }, { permanentFail: false }] } ] });
      while (await cursor.hasNext()) {
        const p = await cursor.next();
        try {
          const uid = p.userId;
          const lang = await import('./cache.js').then(m => m.resolveUserLang(uid));
          let final = stored.quote.translations && stored.quote.translations[lang] ? stored.quote.translations[lang] : stored.quote.original;
          if (!final) final = stored.quote.original || '';
          const out = stored.quote.author ? `${final}\n— ${stored.quote.author}` : final;
          await bot.telegram.sendMessage(uid, String(out).slice(0, MESSAGE_TEXT_MAX));
          await pendingDailySendsCollection.updateOne({ _id: p._id }, { $set: { quoteSent: true } });
        } catch (e) {}
      }
      return;
    }
    if (attempts >= 12) {
      await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { attempts, exhausted: true } });
    } else {
      await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { attempts, nextAttemptAt: new Date(Date.now() + RETRY_INTERVAL_MS) } });
    }
  } catch (e) {}
}

export async function watchForNewQuotes(bot) {
  try {
    const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ || 'Europe/Kyiv' });
    const doc = await dailyMotivationCollection.findOne({ date: dateStr });
    if (!doc || !doc.quote || !doc.quote.original) return;
    if (watchForNewQuotes.lastSeen === dateStr) return;
    watchForNewQuotes.lastSeen = dateStr;
    const cursor = pendingDailySendsCollection.find({ date: dateStr, sent: true, $and: [ { $or: [{ quoteSent: { $exists: false } }, { quoteSent: false }] }, { $or: [{ permanentFail: { $exists: false } }, { permanentFail: false }] } ] });
    while (await cursor.hasNext()) {
      const p = await cursor.next();
      try {
        const uid = p.userId;
        const lang = await import('./cache.js').then(m => m.resolveUserLang(uid));
        let final = doc.quote.translations && doc.quote.translations[lang] ? doc.quote.translations[lang] : doc.quote.original;
        if (!final) final = doc.quote.original || '';
        const out = doc.quote.author ? `${final}\n— ${doc.quote.author}` : final;
        await bot.telegram.sendMessage(uid, String(out).slice(0, MESSAGE_TEXT_MAX));
        await pendingDailySendsCollection.updateOne({ _id: p._id }, { $set: { quoteSent: true } });
      } catch (e) {}
    }
  } catch (e) {}
}
watchForNewQuotes.lastSeen = null;
