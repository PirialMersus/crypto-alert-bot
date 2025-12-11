// src/daily.js
import { httpGetWithRetry, httpClient } from './httpClient.js';
import { dailyCache } from './cache.js';
import { dailyMotivationCollection, dailyQuoteRetryCollection, pendingDailySendsCollection, client } from './db/db.js';
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

function transliterateCyrillicToLatin(s) {
  if (!s || typeof s !== 'string') return s;
  const map = {
    'А':'A','Б':'B','В':'V','Г':'G','Д':'D','Е':'E','Ё':'E','Ж':'Zh','З':'Z','И':'I','Й':'Y','К':'K','Л':'L','М':'M','Н':'N','О':'O','П':'P','Р':'R','С':'S','Т':'T','У':'U','Ф':'F','Х':'Kh','Ц':'Ts','Ч':'Ch','Ш':'Sh','Щ':'Shch','Ъ':'','Ы':'Y','Ь':'','Э':'E','Ю':'Yu','Я':'Ya',
    'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'kh','ц':'ts','ч':'ch','ш':'sh','щ':'shch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya'
  };
  return s.split('').map(ch => map[ch] !== undefined ? map[ch] : ch).join('');
}

export async function fetchQuoteQuotable() {
  try {
    const res = await httpGetWithRetry(QUOTABLE_RANDOM, 1);
    const d = res?.data;
    if (d?.content && !isLikelyHTML(d.content)) return { text: d.content, author: d.author || '', source: 'quotable' };
  } catch (e) { console.warn('fetchQuoteQuotable failed', e?.message || e); }
  return null;
}

export async function fetchQuoteZen() {
  try {
    const res = await httpGetWithRetry(ZEN_QUOTES, 1);
    const d = res?.data;
    if (Array.isArray(d) && d[0] && d[0].q && !isLikelyHTML(d[0].q)) return { text: d[0].q, author: d[0].a || '', source: 'zen' };
  } catch (e) { console.warn('fetchQuoteZen failed', e?.message || e); }
  return null;
}

export async function fetchQuoteTypefit() {
  try {
    const res = await httpGetWithRetry(TYPEFIT_ALL, 1);
    const arr = res?.data;
    if (Array.isArray(arr) && arr.length) {
      const cand = arr[Math.floor(Math.random() * arr.length)];
      const text = cand?.text || cand?.quote || cand?.content;
      if (text && !isLikelyHTML(text)) {
        return { text: text, author: cand.author || '', source: 'typefit' };
      }
    }
  } catch (e) { console.warn('fetchQuoteTypefit failed', e?.message || e); }
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
  } catch (e) { console.warn('fetchQuoteForismatic failed', e?.message || e); }
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
    } catch (e) {
      console.warn('fetchRandomImage failed', s.name, e?.message || e);
    }
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
    if (quote && quote.text) {
      if (originalLang === 'ru') translations.ru = quote.text;
      else translations.en = quote.text;
    }

    const doc = {
      date: dateStr,
      quote: quote ? {
        original: quote.text,
        author: quote.author || '',
        source: quote.source || '',
        originalLang,
        translations
      } : null,
      image: img ? { url: img.url, source: img.source } : null,
      createdAt: new Date()
    };

    // If DB not available, either throw on force or return null gracefully
    if (!dailyMotivationCollection) {
      if (opts && opts.force) {
        throw new Error('fetchAndStoreDailyMotivation: mongo unavailable');
      } else {
        console.warn('fetchAndStoreDailyMotivation: mongo not connected — skipping DB write');
        dailyCache.date = dateStr;
        dailyCache.doc = doc;
        dailyCache.imageBuffer = img?.buffer || null;
        if (!doc.quote) {
          if (dailyQuoteRetryCollection) {
            await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { date: dateStr, attempts: 0, nextAttemptAt: new Date(Date.now() + RETRY_INTERVAL_MS) } }, { upsert: true }).catch(()=>{});
          }
        }
        return doc;
      }
    }

    if (opts && opts.force) {
      if (!doc.quote && !doc.image) {
        throw new Error('fetchAndStoreDailyMotivation: force requested but unable to fetch quote or image from sources (network or sources failure).');
      }
      await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: doc }, { upsert: true });
    } else {
      await dailyMotivationCollection.updateOne({ date: dateStr }, { $setOnInsert: doc }, { upsert: true });
    }

    const stored = await dailyMotivationCollection.findOne({ date: dateStr });

    dailyCache.date = dateStr;
    dailyCache.doc = stored;
    dailyCache.imageBuffer = null;

    if (!stored?.quote) {
      if (dailyQuoteRetryCollection) {
        await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { date: dateStr, attempts: 0, nextAttemptAt: new Date(Date.now() + RETRY_INTERVAL_MS) } }, { upsert: true });
      }
    } else {
      if (dailyQuoteRetryCollection) await dailyQuoteRetryCollection.deleteOne({ date: dateStr }).catch(()=>{});
    }

    return stored;
  } catch (e) {
    console.error('fetchAndStoreDailyMotivation error', e?.message || e);
    throw e;
  }
}

export async function sendDailyToUser(bot, userId, dateStr, opts = { disableNotification: true, forceRefresh: false }) {
  try {
    if (opts && opts.forceRefresh) {
      try {
        await fetchAndStoreDailyMotivation(dateStr, { force: true }).catch((e)=>{ console.warn('force fetch failed', e?.message || e); });
      } catch (e) {}
    }

    let doc = dailyCache.date === dateStr ? dailyCache.doc : (dailyMotivationCollection ? await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>null) : null);
    if (!doc) doc = await fetchAndStoreDailyMotivation(dateStr).catch(()=>null);

    const buf = await ensureDailyImageBuffer(dateStr).catch(()=>null);
    // resolveUserLang is in cache.js to avoid circular imports here
    const cacheMod = await import('./cache.js').catch(()=>null);
    const lang = cacheMod ? await cacheMod.resolveUserLang(userId).catch(()=> 'ru') : 'ru';

    let quoteText = null;
    const storedQuote = doc?.quote || null;
    const original = storedQuote?.original || (storedQuote?.translations && (storedQuote.translations.en || storedQuote.translations.ru)) || null;
    const originalLang = storedQuote?.originalLang || (storedQuote?.translations && storedQuote?.translations.ru ? 'ru' : 'en');

    if (original && isLikelyHTML(original)) {
      if (storedQuote) storedQuote.original = null;
    }

    if (storedQuote?.translations && storedQuote.translations[lang]) {
      if (!isLikelyHTML(storedQuote.translations[lang])) quoteText = storedQuote.translations[lang];
    }

    if (!quoteText && storedQuote && storedQuote.original && storedQuote.originalLang === lang) {
      if (!isLikelyHTML(storedQuote.original)) quoteText = storedQuote.original;
    }

    if (!quoteText && storedQuote?.translations) {
      if (lang === 'en' && storedQuote.translations.en && !isLikelyHTML(storedQuote.translations.en)) quoteText = storedQuote.translations.en;
      if (lang === 'ru' && storedQuote.translations.ru && !isLikelyHTML(storedQuote.translations.ru)) quoteText = storedQuote.translations.ru;
    }

    if (!quoteText && original) {
      try {
        const tr = await translateOrNull(original, lang).catch(()=>null);
        if (tr && !isLikelyHTML(tr)) {
          quoteText = tr;
          try {
            if (dailyMotivationCollection) {
              const upd = { ['quote.translations.'+lang]: tr };
              await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: upd }, { upsert: false });
              dailyCache.doc = await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>dailyCache.doc);
            }
          } catch (e) {}
        }
      } catch (e) {}
    }

    async function recordSendStatus(ok, quoteWasSent) {
      try {
        if (!pendingDailySendsCollection) return;
        if (ok) {
          await pendingDailySendsCollection.updateOne(
            { userId, date: dateStr },
            { $set: { sent: true, sentAt: new Date(), quoteSent: !!quoteWasSent, permanentFail: false } },
            { upsert: true }
          );
        } else {
          await pendingDailySendsCollection.updateOne(
            { userId, date: dateStr },
            { $set: { sent: false, createdAt: new Date(), permanentFail: true } },
            { upsert: true }
          );
        }
      } catch (e) {
        console.warn('recordSendStatus failed', e?.message || e);
      }
    }

    if (!quoteText) {
      const fallback = String((await import('./utils/utils.js').then(m=>m.buildWish()).catch(()=> 'Хорошего дня!'))).slice(0, QUOTE_CAPTION_MAX);
      if (buf) {
        try { await bot.telegram.sendPhoto(userId, { source: buf }, { caption: fallback, disable_notification: !!opts.disableNotification }); } catch (e) { console.warn('sendDailyToUser sendPhoto failed fallback', e); await recordSendStatus(false, false); return false; }
      } else {
        try { await bot.telegram.sendMessage(userId, fallback, { disable_notification: !!opts.disableNotification }); } catch (e) { console.warn('sendDailyToUser sendMessage failed fallback', e); await recordSendStatus(false, false); return false; }
      }
      await recordSendStatus(true, false);
      return true;
    }

    const caption = String(quoteText).slice(0, QUOTE_CAPTION_MAX);
    if (buf) {
      try { await bot.telegram.sendPhoto(userId, { source: buf }, { caption, disable_notification: !!opts.disableNotification }); } catch (e) { console.warn('sendDailyToUser sendPhoto failed', e); await recordSendStatus(false, !!quoteText); return false; }
    } else {
      try { await bot.telegram.sendMessage(userId, caption, { disable_notification: !!opts.disableNotification }); } catch (e) { console.warn('sendDailyToUser sendMessage failed', e); await recordSendStatus(false, !!quoteText); return false; }
    }

    const rawAuthor = storedQuote?.author || '';
    let authorToSend = rawAuthor;
    try {
      if (lang === 'en' && /[А-Яа-яЁё]/.test(String(rawAuthor))) {
        authorToSend = transliterateCyrillicToLatin(String(rawAuthor));
      }
    } catch (e) {}

    if (authorToSend) {
      try {
        if (!caption.includes(authorToSend)) {
          await bot.telegram.sendMessage(userId, `— ${authorToSend}`.slice(0, MESSAGE_TEXT_MAX), { disable_notification: !!opts.disableNotification });
        }
      } catch (e) {}
    }

    await recordSendStatus(true, !!quoteText);
    return true;
  } catch (e) {
    console.error('sendDailyToUser error', e?.message || e);
    try {
      if (pendingDailySendsCollection) {
        await pendingDailySendsCollection.updateOne(
          { userId, date: dateStr },
          { $set: { sent: false, createdAt: new Date(), permanentFail: true } },
          { upsert: true }
        );
      }
    } catch (err) {}
    return false;
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
    } catch (e) { /* ignore */ }
    return dailyCache.imageBuffer;
  }

  return null;
}

export async function processDailyQuoteRetry(bot) {
  try {
    if (!dailyQuoteRetryCollection) return;
    const now = new Date();
    const doc = await dailyQuoteRetryCollection.findOne({ nextAttemptAt: { $lte: now } });
    if (!doc) return;

    const dateStr = doc.date;
    const attempts = (doc.attempts || 0) + 1;

    const q = await fetchQuoteFromAny(2).catch(()=>null);
    if (q && q.text && !isLikelyHTML(q.text)) {
      let translations = { en: q.text, ru: null, uk: null };
      const origLang = detectSimpleLang(q.text);
      if (origLang === 'ru') { translations = { en: null, ru: q.text, uk: null }; }
      else { translations = { en: q.text, ru: null, uk: null }; }
      try { const enT = await translateOrNull(q.text, 'en').catch(()=>q.text); translations.en = enT || q.text; } catch {}
      try { const ruT = await translateOrNull(q.text, 'ru').catch(()=>translations.en); translations.ru = ruT || translations.en || q.text; } catch {}
      try { const ukT = await translateOrNull(q.text, 'uk').catch(()=>translations.en); translations.uk = ukT || translations.en || q.text; } catch {}

      if (dailyMotivationCollection) {
        await dailyMotivationCollection.updateOne(
          { date: dateStr },
          { $set: { 'quote.original': q.text, 'quote.author': q.author || '', 'quote.source': q.source || '', 'quote.translations': translations, 'quote.originalLang': origLang } },
          { upsert: true }
        );
        await dailyQuoteRetryCollection.deleteOne({ date: dateStr });
        const stored = await dailyMotivationCollection.findOne({ date: dateStr });
        dailyCache.date = dateStr;
        dailyCache.doc = stored;
        dailyCache.imageBuffer = null;

        if (pendingDailySendsCollection) {
          const cursor = pendingDailySendsCollection.find({ date: dateStr, sent: true, $and: [ { $or: [{ quoteSent: { $exists: false } }, { quoteSent: false }] }, { $or: [{ permanentFail: { $exists: false } }, { permanentFail: false }] } ] });
          while (await cursor.hasNext()) {
            const p = await cursor.next();
            try {
              const uid = p.userId;
              const cacheMod = await import('./cache.js').catch(()=>null);
              const lang = cacheMod ? await cacheMod.resolveUserLang(uid) : 'ru';
              let final = stored.quote.translations && stored.quote.translations[lang] ? stored.quote.translations[lang] : stored.quote.original;
              if (!final) final = stored.quote.original || '';
              const out = stored.quote.author ? `${final}\n— ${stored.quote.author}` : final;
              await bot.telegram.sendMessage(uid, String(out).slice(0, MESSAGE_TEXT_MAX));
              await pendingDailySendsCollection.updateOne({ _id: p._id }, { $set: { quoteSent: true } });
            } catch (e) { console.warn('processDailyQuoteRetry: failed to send', e); }
          }
        }
      }
      return;
    }

    if (attempts >= 12) {
      if (dailyQuoteRetryCollection) await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { attempts, exhausted: true } });
    } else {
      if (dailyQuoteRetryCollection) await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { attempts, nextAttemptAt: new Date(Date.now() + RETRY_INTERVAL_MS) } });
    }
  } catch (e) {
    console.error('processDailyQuoteRetry error', e?.message || e);
  }
}

export async function watchForNewQuotes(bot) {
  try {
    const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ || 'Europe/Kyiv' });
    const doc = (dailyMotivationCollection ? await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>null) : dailyCache.doc);
    if (!doc || !doc.quote || !doc.quote.original) return;
    if (watchForNewQuotes.lastSeen === dateStr) return;
    watchForNewQuotes.lastSeen = dateStr;

    if (!pendingDailySendsCollection) {
      console.warn('watchForNewQuotes: pendingDailySendsCollection not available, skipping');
      return;
    }

    const cursor = pendingDailySendsCollection.find({ date: dateStr, sent: true, $and: [ { $or: [{ quoteSent: { $exists: false } }, { quoteSent: false }] }, { $or: [{ permanentFail: { $exists: false } }, { permanentFail: false }] } ] });
    while (await cursor.hasNext()) {
      const p = await cursor.next();
      try {
        const uid = p.userId;
        const cacheMod = await import('./cache.js').then(m => m).catch(()=>null);
        const lang = cacheMod ? await cacheMod.resolveUserLang(uid) : 'ru';
        let final = doc.quote.translations && doc.quote.translations[lang] ? doc.quote.translations[lang] : doc.quote.original;
        if (!final) final = doc.quote.original || '';
        const out = doc.quote.author ? `${final}\n— ${doc.quote.author}` : final;
        await bot.telegram.sendMessage(uid, String(out).slice(0, MESSAGE_TEXT_MAX));
        await pendingDailySendsCollection.updateOne({ _id: p._id }, { $set: { quoteSent: true } });
      } catch (e) { console.warn('watchForNewQuotes: failed to send', e); }
    }
  } catch (e) { console.warn('watchForNewQuotes error', e?.message || e); }
}
watchForNewQuotes.lastSeen = null;
