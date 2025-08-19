// src/motivation.js
import axios from 'axios';
import { getCollections } from './db.js';
import { escapeTextForTelegram } from './utils.js';

const http = axios.create({ timeout: 9000, headers: { 'User-Agent': 'crypto-alert-bot/1.0' } });

// Параметры
const KYIV_TZ = 'Europe/Kyiv';
const IMAGE_WIDTH = 1200;
const IMAGE_HEIGHT = 800;
const QUOTE_RETRY_DELAY_MS = 5 * 60 * 1000; // 5 минут между попытками цитаты
const RETRY_WORKER_INTERVAL_MS = 60 * 1000; // проверяем очередь каждую минуту
const SCHEDULER_TICK_MS = 30 * 1000; // тикер для проверки времени (каждые 30s)
const MAX_QUOTE_ATTEMPTS = 12; // ~1 час (12 * 5min)
const SEND_HOUR = 7;   // час отправки (7:00 Kyiv)
const PREPARE_HOUR = 6; // час подготовки (6:00 Kyiv)

/* ----------------- ВСПОМОГАТЕЛИ ------------------ */

function todayDateStrKyiv() {
  return new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ });
}

function kyivHourMinuteNow() {
  // formatToParts безопасно возвращает части локального времени в нужной таймзоне
  const fmt = new Intl.DateTimeFormat('en-GB', { timeZone: KYIV_TZ, hour12: false, hour: '2-digit', minute: '2-digit', year:'numeric', month:'2-digit', day:'2-digit' });
  const parts = fmt.formatToParts(new Date());
  const p = Object.fromEntries(parts.map(x => [x.type, x.value]));
  return {
    hour: parseInt(p.hour, 10),
    minute: parseInt(p.minute, 10),
    dateStr: `${p.year}-${p.month}-${p.day}`
  };
}

function buildRandomNatureImageUrl(w = IMAGE_WIDTH, h = IMAGE_HEIGHT) {
  // Используем picsum (стабильнее than unsplash без API); добавляем rnd query чтобы не кэшировалось
  return `https://picsum.photos/${w}/${h}?random=${Date.now()}&nature=1`;
}

async function fetchQuoteOnce() {
  // пробуем несколько общедоступных источников по порядку
  try {
    const r = await http.get('https://api.quotable.io/random');
    const d = r?.data;
    if (d?.content) return { text: d.content, author: d.author || null };
  } catch (e) {}

  try {
    const r = await http.get('https://zenquotes.io/api/random');
    const d = r?.data;
    if (Array.isArray(d) && d[0]?.q) return { text: d[0].q, author: d[0].a || null };
  } catch (e) {}

  try {
    const r = await http.get('https://type.fit/api/quotes');
    const arr = r?.data;
    if (Array.isArray(arr) && arr.length) {
      const pick = arr[Math.floor(Math.random() * arr.length)];
      if (pick?.text) return { text: pick.text, author: pick.author || null };
    }
  } catch (e) {}

  return null;
}

/* ----------------- Основные операции с БД ------------------ */

export async function ensureDailyMotivation(dateStr = todayDateStrKyiv()) {
  const { dailyMotivationCollection } = getCollections();
  const existing = await dailyMotivationCollection.findOne({ date: dateStr });
  if (existing) return existing;

  const doc = {
    date: dateStr,
    imageUrl: buildRandomNatureImageUrl(),
    quote: null,
    attempts: 0,
    createdAt: new Date()
  };

  try {
    await dailyMotivationCollection.insertOne(doc);
  } catch (e) {
    // возможный рейс-кондишн — читаем существующий
    const ex = await dailyMotivationCollection.findOne({ date: dateStr });
    if (ex) return ex;
    throw e;
  }

  // Асинхронно запустить первую попытку получить цитату (не блокируем)
  (async () => {
    try { await tryFetchAndStoreQuote(dateStr); } catch (err) { /* ignore */ }
  })();

  return doc;
}

export async function tryFetchAndStoreQuote(dateStr = todayDateStrKyiv()) {
  const { dailyMotivationCollection, dailyQuoteRetryCollection } = getCollections();
  const mot = await dailyMotivationCollection.findOne({ date: dateStr });
  if (!mot) return;

  if (mot.quote && mot.quote.text) return; // уже есть

  const attemptsBefore = mot.attempts || 0;
  const q = await fetchQuoteOnce();
  const attemptsNow = attemptsBefore + 1;

  if (q) {
    await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: { quote: q, quoteFetchedAt: new Date(), attempts: attemptsNow } });
    try { await dailyQuoteRetryCollection.deleteOne({ date: dateStr }); } catch (e) {}
    return;
  }

  // не получилось — обновим attempts и планируем retry
  await dailyMotivationCollection.updateOne({ date: dateStr }, { $set: { attempts: attemptsNow } });

  const nextTryAt = new Date(Date.now() + QUOTE_RETRY_DELAY_MS);
  await dailyQuoteRetryCollection.updateOne(
    { date: dateStr },
    { $set: { attempts: attemptsNow, lastTriedAt: new Date(), nextTryAt } },
    { upsert: true }
  );
}

/* ----------------- Отправка (одному и всем) ------------------ */

export async function sendDailyToUser(bot, userId, dateStr = todayDateStrKyiv(), options = { silent: false }) {
  const { dailyMotivationCollection, pendingDailySendsCollection } = getCollections();

  const mot = await dailyMotivationCollection.findOne({ date: dateStr });
  if (!mot) {
    // ещё нет мотивации — создаём и просим caller попробовать позже
    await ensureDailyMotivation(dateStr);
    const err = new Error('no_motivation_yet');
    err.code = 'no_motivation_yet';
    throw err;
  }

  // если цитата не получена и ещё есть попытки — просим retry (caller может повторить)
  if ((!mot.quote || !mot.quote.text) && (mot.attempts || 0) < MAX_QUOTE_ATTEMPTS) {
    const err = new Error('quote_pending');
    err.code = 'quote_pending';
    throw err;
  }

  // собираем подпись (цитата если есть, иначе null)
  const caption = mot.quote && mot.quote.text ? (mot.quote.author ? `${mot.quote.text}\n— ${mot.quote.author}` : mot.quote.text) : null;

  try {
    if (mot.imageUrl) {
      if (caption) {
        await bot.telegram.sendPhoto(userId, mot.imageUrl, { caption: escapeTextForTelegram(caption), disable_notification: !!options.silent });
      } else {
        // картинка есть, цитаты нет (после исчерпания попыток) — отправляем картинку + fallback wish
        await bot.telegram.sendPhoto(userId, mot.imageUrl, { disable_notification: !!options.silent });
        await bot.telegram.sendMessage(userId, 'Хорошего дня!', { disable_notification: !!options.silent });
      }
    } else if (caption) {
      await bot.telegram.sendMessage(userId, escapeTextForTelegram(caption), { disable_notification: !!options.silent });
    } else {
      await bot.telegram.sendMessage(userId, 'Хорошего дня!', { disable_notification: !!options.silent });
    }

    try {
      await pendingDailySendsCollection.updateOne({ userId, date: dateStr }, { $set: { sent: true, sentAt: new Date(), quoteSent: !!mot.quote } }, { upsert: true });
    } catch (e) { /** non fatal */ }

    return { sent: true };
  } catch (err) {
    // не смогли отправить (напр. bot blocked) — сохраняем запись как попытка, но не помечаем sent
    try {
      await pendingDailySendsCollection.updateOne({ userId, date: dateStr }, { $set: { sent: false, lastError: String(err) } }, { upsert: true });
    } catch (e) {}
    throw err;
  }
}

export async function sendDailyToAll(bot, dateStr = todayDateStrKyiv()) {
  const { usersCollection, pendingDailySendsCollection } = getCollections();
  const users = await usersCollection.find({}).project({ userId: 1 }).toArray();
  if (!Array.isArray(users) || !users.length) return { total: 0 };

  let sent = 0;
  let skipped = 0;
  for (const u of users) {
    const uid = u.userId;
    if (!uid) continue;

    try {
      const existing = await pendingDailySendsCollection.findOne({ userId: uid, date: dateStr });
      if (existing && existing.sent) { skipped++; continue; }

      try {
        await sendDailyToUser(bot, uid, dateStr, { silent: true });
        sent++;
      } catch (err) {
        if (err && (err.code === 'quote_pending' || err.code === 'no_motivation_yet')) {
          // пропускаем — мотивация ещё не готова
          skipped++;
        } else {
          // логируем, но не останавливаем рассылку
          console.error('sendDailyToUser failed for', uid, err?.message || err);
        }
      }
    } catch (e) {
      console.error('sendDailyToAll iteration error for', uid, e);
    }
  }

  return { total: users.length, sent, skipped };
}

/* ----------------- Планировщики/воркеры ------------------ */

let _ticker = null;
let _retryWorker = null;
let _lastPreparedDate = null;
let _lastSentDate = null;

export function startMotivationSchedulers(bot) {
  // Stop existing if any
  if (_ticker) clearInterval(_ticker);
  if (_retryWorker) clearInterval(_retryWorker);

  // Ensure today's doc on startup
  (async () => {
    try { await ensureDailyMotivation(); } catch (e) { console.error('ensureDailyMotivation startup error', e); }
  })();

  // Retry worker: каждые RETRY_WORKER_INTERVAL_MS проверяем очередь daily_quote_retry
  _retryWorker = setInterval(async () => {
    const { dailyQuoteRetryCollection } = getCollections();
    try {
      const now = new Date();
      const toTry = await dailyQuoteRetryCollection.find({ nextTryAt: { $lte: now } }).limit(20).toArray();
      for (const r of toTry) {
        if (!r || !r.date) continue;
        // если попыток уже много — удалим и пропустим
        if ((r.attempts || 0) >= MAX_QUOTE_ATTEMPTS) {
          try { await dailyQuoteRetryCollection.deleteOne({ date: r.date }); } catch (e) {}
          continue;
        }
        try {
          await tryFetchAndStoreQuote(r.date);
          const mot = (await getCollections().dailyMotivationCollection.findOne({ date: r.date }));
          if (mot && mot.quote && mot.quote.text) {
            try { await dailyQuoteRetryCollection.deleteOne({ date: r.date }); } catch (e) {}
          } else {
            await dailyQuoteRetryCollection.updateOne({ date: r.date }, { $set: { nextTryAt: new Date(Date.now() + QUOTE_RETRY_DELAY_MS) } });
          }
        } catch (err) {
          // ensure nextTryAt bumped
          try { await dailyQuoteRetryCollection.updateOne({ date: r.date }, { $set: { nextTryAt: new Date(Date.now() + QUOTE_RETRY_DELAY_MS) } }); } catch (e) {}
        }
      }
    } catch (err) {
      console.error('quote retry worker error', err);
    }
  }, RETRY_WORKER_INTERVAL_MS);

  // Scheduler tick: проверяем время в Kyiv каждую SCHEDULER_TICK_MS
  _ticker = setInterval(async () => {
    try {
      const nowParts = kyivHourMinuteNow();
      const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ });

      // подготовка в 06:00
      if (nowParts.hour === PREPARE_HOUR && nowParts.minute === 0) {
        if (_lastPreparedDate !== dateStr) {
          _lastPreparedDate = dateStr;
          try {
            await ensureDailyMotivation(dateStr);
            // first quote fetch is triggered inside ensureDailyMotivation
            console.log('ensureDailyMotivation executed for', dateStr);
          } catch (e) { console.error('prepare assurance error', e); }
        }
      }

      // отправка в 07:00 (тихая)
      if (nowParts.hour === SEND_HOUR && nowParts.minute === 0) {
        if (_lastSentDate !== dateStr) {
          _lastSentDate = dateStr;
          try {
            const res = await sendDailyToAll(bot, dateStr);
            console.log('sendDailyToAll result', res);
          } catch (e) {
            console.error('sendDailyToAll error', e);
          }
        }
      }
    } catch (err) {
      console.error('scheduler tick error', err);
    }
  }, SCHEDULER_TICK_MS);
}

export function stopMotivationSchedulers() {
  if (_ticker) { clearInterval(_ticker); _ticker = null; }
  if (_retryWorker) { clearInterval(_retryWorker); _retryWorker = null; }
}
