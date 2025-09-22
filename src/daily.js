// src/daily.js
import { httpGetWithRetry, httpClient } from './httpClient.js';
import { dailyCache } from './cache.js';
import { dailyMotivationCollection, dailyQuoteRetryCollection, pendingDailySendsCollection } from './db.js';
import { RETRY_INTERVAL_MS, QUOTE_CAPTION_MAX, MESSAGE_TEXT_MAX, KYIV_TZ } from './constants.js';
import { translateOrNull } from './translate.js';
import { safeSendTelegram } from './utils.js';

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
  } catch (e) {
    // caller will log
  }
  return null;
}

export async function fetchQuoteQuotable() {
  try {
    const res = await httpGetWithRetry(QUOTABLE_RANDOM, 1);
    const d = res?.data;
    if (d?.content) return { text: d.content, author: d.author || '', source: 'quotable' };
  } catch (e) { console.warn('fetchQuoteQuotable failed', e?.message || e); }
  return null;
}

export async function fetchQuoteZen() {
  try {
    const res = await httpGetWithRetry(ZEN_QUOTES, 1);
    const d = res?.data;
    if (Array.isArray(d) && d[0] && d[0].q) return { text: d[0].q, author: d[0].a || '', source: 'zen' };
  } catch (e) { console.warn('fetchQuoteZen failed', e?.message || e); }
  return null;
}

export async function fetchQuoteTypefit() {
  try {
    const res = await httpGetWithRetry(TYPEFIT_ALL, 1);
    const arr = res?.data;
    if (Array.isArray(arr) && arr.length) {
      const cand = arr[Math.floor(Math.random() * arr.length)];
      if (cand && (cand.text || cand.quote || cand.content)) {
        const text = cand.text || cand.quote || cand.content;
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
    if (text && String(text).trim()) return { text: String(text).trim(), author: String(author || '').trim(), source: 'forismatic' };
  } catch (e) { console.warn('fetchQuoteForismatic failed', e?.message || e); }
  return null;
}

export async function fetchQuoteFromAny(attempts = 2) {
  // Try several sources, prefer forismatic/typefit (these are often more stable)
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
    // small delay between attempts
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
    // make several attempts for quote and image if force requested
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

    // Build doc — translations are set only when we have a quote
    let translations = null;
    if (quote && quote.text) translations = { en: quote.text, ru: null, uk: null };

    const doc = {
      date: dateStr,
      quote: quote ? {
        original: quote.text,
        author: quote.author || '',
        source: quote.source || '',
        translations
      } : null,
      image: img ? { url: img.url, source: img.source } : null,
      createdAt: new Date()
    };

    if (opts && opts.force) {
      // If force requested but we got nothing (no quote and no img), throw — so admin sees failure
      if (!doc.quote && !doc.image) {
        throw new Error('fetchAndStoreDailyMotivation: force requested but unable to fetch quote or image from sources (network or sources failure).');
      }
      await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: doc }, { upsert: true });
    } else {
      // Only insert-once behavior
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

export async function sendDailyToUser(bot, userId, dateStr, opts = { disableNotification: true, forceRefresh: false }) {
  try {
    if (opts && opts.forceRefresh) {
      try {
        await fetchAndStoreDailyMotivation(dateStr, { force: true }).catch((e)=>{ console.warn('force fetch failed', e?.message || e); });
      } catch (e) {}
    }

    let doc = dailyCache.date === dateStr ? dailyCache.doc : await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>null);
    if (!doc) doc = await fetchAndStoreDailyMotivation(dateStr).catch(()=>null);

    const buf = await ensureDailyImageBuffer(dateStr).catch(()=>null);
    const lang = 'ru';

    let quoteText = null;
    const original = doc?.quote?.original || (doc?.quote?.translations && doc.quote.translations.en) || null;

    if (doc?.quote) {
      if (doc.quote.translations && doc.quote.translations[lang]) {
        quoteText = doc.quote.translations[lang];
      } else if (original) {
        try {
          const tr = await translateOrNull(original, lang).catch(()=>null);
          if (tr) {
            quoteText = tr;
            try {
              const upd = { ['quote.translations.'+lang]: tr };
              await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: upd }, { upsert: false });
              dailyCache.doc = await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>dailyCache.doc);
            } catch (e) { /* ignore write errors */ }
          }
        } catch (e) { /* ignore translate errors */ }
      }
    }

    if (!quoteText) {
      const orig = doc?.quote?.original;
      if (orig) {
        try {
          const tr = await translateOrNull(orig, 'ru').catch(()=>null);
          if (tr) {
            quoteText = tr;
            try {
              await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: { 'quote.translations.ru': tr } }, { upsert: false });
              dailyCache.doc = await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>dailyCache.doc);
            } catch (e) { /* ignore write errors */ }
          }
        } catch (e) { /* ignore */ }
      }

      if (!quoteText) {
        try {
          const foris = await fetchQuoteForismatic().catch(()=>null);
          if (foris && foris.text) {
            const originalRu = String(foris.text);
            quoteText = originalRu;
            try {
              const enT = await translateOrNull(originalRu, 'en').catch(()=>originalRu);
              const ukT = await translateOrNull(originalRu, 'uk').catch(null);
              const updates = {
                'quote.original': originalRu,
                'quote.author': foris.author || (doc?.quote?.author || ''),
                'quote.source': foris.source || 'forismatic',
                'quote.translations.en': enT,
                'quote.translations.ru': originalRu
              };
              if (ukT) updates['quote.translations.uk'] = ukT;
              await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: updates }, { upsert: true });
              dailyCache.doc = await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>dailyCache.doc);
              doc = dailyCache.doc || doc;
            } catch (e) { /* ignore db write errors */ }
          }
        } catch (e) { /* ignore */ }
      }
    }

    if (!quoteText) {
      try {
        const q = await fetchQuoteFromAny().catch(()=>null);
        if (q && q.text) {
          const originalTxt = String(q.text);
          let enT = originalTxt;
          try { enT = await translateOrNull(originalTxt, 'en').catch(()=>originalTxt); } catch {}
          let tr = enT;
          try { tr = await translateOrNull(originalTxt, lang).catch(enT); } catch {}
          quoteText = tr || enT || originalTxt;
          try {
            await dailyMotivationCollection.updateOne(
              { date: dateStr },
              { $set: { 'quote.original': originalTxt, 'quote.author': q.author || '', 'quote.source': q.source || '', 'quote.translations.en': enT, ['quote.translations.'+lang]: tr } },
              { upsert: true }
            );
            dailyCache.doc = await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>null);
          } catch (e) { /* ignore */ }
        }
      } catch (e) { console.warn('Final fetch attempt failed', e); }
    }

    // Helper to record pendingDailySends status (best-effort)
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
        // swallow - this is best-effort to avoid breaking sends
        console.warn('recordSendStatus failed', e?.message || e);
      }
    }

    if (!quoteText) {
      const fallback = String(await import('./utils.js').then(m=>m.buildWish())).slice(0, QUOTE_CAPTION_MAX);
      if (buf) {
        try {
          await safeSendTelegram(bot, 'sendPhoto', [userId, { source: buf }, { caption: fallback, disable_notification: !!opts.disableNotification }]);
        } catch (e) { console.warn('sendDailyToUser sendPhoto failed fallback', e); await recordSendStatus(false, false); return false; }
      } else {
        try {
          await safeSendTelegram(bot, 'sendMessage', [userId, fallback, { disable_notification: !!opts.disableNotification }]);
        } catch (e) { console.warn('sendDailyToUser sendMessage failed fallback', e); await recordSendStatus(false, false); return false; }
      }
      await recordSendStatus(true, false);
      return true;
    }

    const caption = String(quoteText).slice(0, QUOTE_CAPTION_MAX);
    if (buf) {
      try {
        await safeSendTelegram(bot, 'sendPhoto', [userId, { source: buf }, { caption, disable_notification: !!opts.disableNotification }]);
      } catch (e) { console.warn('sendDailyToUser sendPhoto failed', e); await recordSendStatus(false, !!quoteText); return false; }
    } else {
      try {
        await safeSendTelegram(bot, 'sendMessage', [userId, caption, { disable_notification: !!opts.disableNotification }]);
      } catch (e) { console.warn('sendDailyToUser sendMessage failed', e); await recordSendStatus(false, !!quoteText); return false; }
    }

    if (doc?.quote?.author) {
      try {
        if (!caption.includes(doc.quote.author)) {
          try {
            await safeSendTelegram(bot, 'sendMessage', [userId, `— ${doc.quote.author}`.slice(0, MESSAGE_TEXT_MAX), { disable_notification: !!opts.disableNotification }]);
          } catch (e) { /* ignore */ }
        }
      } catch (e) { /* ignore */ }
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
    } catch (err) { /* ignore */ }
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
    if (q && q.text) {
      let translations = { en: q.text, ru: null, uk: null };
      try { const enT = await translateOrNull(q.text, 'en').catch(()=>q.text); translations.en = enT || q.text; } catch {}
      try { const ruT = await translateOrNull(q.text, 'ru').catch(()=>translations.en); translations.ru = ruT || translations.en || q.text; } catch {}
      try { const ukT = await translateOrNull(q.text, 'uk').catch(()=>translations.en); translations.uk = ukT || translations.en || q.text; } catch {}

      await dailyMotivationCollection.updateOne(
        { date: dateStr },
        { $set: { 'quote.original': q.text, 'quote.author': q.author || '', 'quote.source': q.source || '', 'quote.translations': translations } },
        { upsert: true }
      );
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
          try {
            await safeSendTelegram(bot, 'sendMessage', [uid, String(out).slice(0, MESSAGE_TEXT_MAX)]);
            await pendingDailySendsCollection.updateOne({ _id: p._id }, { $set: { quoteSent: true } });
          } catch (e) {
            console.warn('processDailyQuoteRetry: failed to send', e);
          }
        } catch (e) { console.warn('processDailyQuoteRetry: failed to process pending send', e); }
      }

      return;
    }

    if (attempts >= 12) {
      await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { attempts, exhausted: true } });
    } else {
      await dailyQuoteRetryCollection.updateOne({ date: dateStr }, { $set: { attempts, nextAttemptAt: new Date(Date.now() + RETRY_INTERVAL_MS) } });
    }
  } catch (e) {
    console.error('processDailyQuoteRetry error', e?.message || e);
  }
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
        try {
          await safeSendTelegram(bot, 'sendMessage', [uid, String(out).slice(0, MESSAGE_TEXT_MAX)]);
          await pendingDailySendsCollection.updateOne({ _id: p._id }, { $set: { quoteSent: true } });
        } catch (e) { console.warn('watchForNewQuotes: failed to send', e); }
      } catch (e) { console.warn('watchForNewQuotes: failed to process pending', e); }
    }
  } catch (e) { console.warn('watchForNewQuotes error', e); }
}
watchForNewQuotes.lastSeen = null;
