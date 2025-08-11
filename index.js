// crypto-bot/index.js
// Файл: crypto-bot/index.js — главный файл бота с логикой алертов и CRUD в MongoDB

import { Telegraf, session } from 'telegraf';
import axios from 'axios';
import dotenv from 'dotenv';
import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';

dotenv.config();

const BOT_TOKEN = process.env.BOT_TOKEN;
const MONGO_URI = process.env.MONGO_URI;
const CREATOR_ID = process.env.CREATOR_ID ? parseInt(process.env.CREATOR_ID, 10) : null;
const INACTIVE_DAYS = 30; // считать неактивным >30 дней
const CACHE_TTL = 70000; // 70 секунд

if (!BOT_TOKEN) throw new Error('BOT_TOKEN не задан в окружении');
if (!MONGO_URI) throw new Error('MONGO_URI не задан в окружении');

const bot = new Telegraf(BOT_TOKEN);

// ------------------ Сессии ------------------
bot.use(session());
bot.use((ctx, next) => {
  if (!ctx.session) ctx.session = {};
  return next();
});

// ------------------ Подключение к MongoDB ------------------
const client = new MongoClient(MONGO_URI);
let alertsCollection;
let lastViewsCollection;
let usersCollection;

async function connectToMongo() {
  try {
    await client.connect();
    const db = client.db();
    alertsCollection = db.collection('alerts');
    lastViewsCollection = db.collection('last_alerts_view');
    usersCollection = db.collection('users');
    console.log('✅ Подключено к MongoDB');
  } catch (err) {
    console.error('❌ Ошибка подключения к MongoDB:', err);
    throw err;
  }
}
await connectToMongo();

// ------------------ HTTP сервер ------------------
const app = express();
app.get('/', (req, res) => res.send('Бот работает с монгошкой! 🚀'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🌐 HTTP сервер запущен на порту ${PORT}`));

// ------------------ Обработчики ошибок ------------------
process.on('uncaughtException', err => console.error('Необработанная ошибка:', err));
process.on('unhandledRejection', reason => console.error('Необработанное обещание:', reason));

// ------------------ Главное меню (добавляем кнопку только для создателя) ------------------
function getMainMenu(userId) {
  const keyboard = [
    [{ text: '➕ Создать алерт' }, { text: '📋 Мои алерты' }]
  ];
  if (CREATOR_ID && String(userId) === String(CREATOR_ID)) {
    keyboard.push([{ text: '👥 Количество активных пользователей' }]);
  }
  return { reply_markup: { keyboard, resize_keyboard: true } };
}

// ------------------ КЭШИ ------------------
const pricesCache = new Map(); // symbol -> { price, time }
const alertsCache = new Map(); // userId -> { alerts, time }
const lastViewsCache = new Map(); // userId -> { map: {symbol:lastPrice}, time }
const usersActivityCache = new Map(); // userId -> lastWriteTs (ms)
let statsCache = { count: null, time: 0 }; // кеш для количества активных пользователей

// ------------------ Получение цены (KuCoin) с кешем ------------------
async function getPrice(symbol) {
  const now = Date.now();
  const cached = pricesCache.get(symbol);
  if (cached && (now - cached.time) < CACHE_TTL) return cached.price;

  try {
    const res = await axios.get(`https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${symbol}`);
    const price = parseFloat(res.data.data?.price);
    pricesCache.set(symbol, { price, time: now });
    return price;
  } catch (err) {
    console.error('getPrice error for', symbol, err?.message || err);
    return null;
  }
}

// ------------------ Кеширование списка алертов (по пользователю) ------------------
async function getUserAlertsCached(userId) {
  const now = Date.now();
  const cached = alertsCache.get(userId);
  if (cached && (now - cached.time) < CACHE_TTL) return cached.alerts;

  const alerts = await alertsCollection.find({ userId }).toArray();
  alertsCache.set(userId, { alerts, time: now });
  return alerts;
}
function invalidateUserAlertsCache(userId) {
  try {
    alertsCache.delete(userId);
  } catch (e) {
    console.error('Ошибка инвалидации кеша алертов для', userId, e);
  }
}

// ------------------ lastViews: чтение с кешем + буферизованная запись ------------------
async function getUserLastViewsCached(userId) {
  const now = Date.now();
  const cached = lastViewsCache.get(userId);
  if (cached && (now - cached.time) < CACHE_TTL) return cached.map;

  const rows = await lastViewsCollection.find({ userId }).toArray();
  const map = Object.fromEntries(rows.map(r => [r.symbol, r.lastPrice]));
  lastViewsCache.set(userId, { map, time: now });
  return map;
}

// updates: { SYMBOL: price, ... }
async function updateUserLastViews(userId, updates) {
  if (!userId) return;
  const now = Date.now();
  const cached = lastViewsCache.get(userId);

  if (cached && (now - cached.time) < CACHE_TTL) {
    Object.assign(cached.map, updates);
    return;
  }

  const combined = { ...(cached ? cached.map : {}), ...updates };
  const ops = Object.entries(combined).map(([symbol, lastPrice]) => ({
    updateOne: {
      filter: { userId, symbol },
      update: { $set: { lastPrice } },
      upsert: true
    }
  }));

  if (ops.length) {
    try {
      await lastViewsCollection.bulkWrite(ops);
    } catch (e) {
      console.error('Ошибка bulkWrite lastViews:', e);
    }
  }
  lastViewsCache.set(userId, { map: combined, time: now });
}

// ------------------ Middleware: обновление lastActive ------------------
bot.use(async (ctx, next) => {
  try {
    if (ctx.from && ctx.from.id) {
      const uid = ctx.from.id;
      const now = Date.now();
      const lastWrite = usersActivityCache.get(uid) || 0;
      if ((now - lastWrite) >= CACHE_TTL) {
        try {
          await usersCollection.updateOne(
            { userId: uid },
            { $set: { userId: uid, username: ctx.from.username || null, lastActive: new Date() } },
            { upsert: true }
          );
        } catch (e) {
          console.error('Ошибка записи lastActive:', e);
        }
        usersActivityCache.set(uid, now);
        statsCache.time = 0;
      }
    }
  } catch (e) {
    console.error('Ошибка в middleware активности:', e);
  }
  return next();
});

// ------------------ Хелперы форматирования ------------------
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
  if (change > 0) return `${value} ️🚀📈`;
  if (change < 0) return `${value} ️🛬📉`;
  return `${value}`;
}
const safeBold = text => `*${text}*`;

// ------------------ Отрисовка списка алертов ------------------
/**
 renderAlertsList(userId, options)
 options.includeDeleteButtons: boolean
 при includeDeleteButtons = false возвращает текст + одну кнопку "🧹 Удалить..."
 при includeDeleteButtons = true возвращает текст + набор кнопок удаления (по одному на алерт) + кнопку "◀️ Назад"
 Возвращает { text, buttons } где buttons — inline_keyboard (массив рядов).
 */
async function renderAlertsList(userId, options = { includeDeleteButtons: false }) {
  const alerts = await getUserAlertsCached(userId);
  if (!alerts.length) return { text: 'У тебя нет активных алертов.', buttons: [] };

  const uniqueSymbols = [...new Set(alerts.map(a => a.symbol))];
  const pricesArr = await Promise.all(uniqueSymbols.map(sym => getPrice(sym)));
  const priceMap = Object.fromEntries(uniqueSymbols.map((sym, i) => [sym, typeof pricesArr[i] === 'number' ? pricesArr[i] : null]));

  const lastViewMap = await getUserLastViewsCached(userId);

  let msg = '📋 *Твои алерты:*\n\n';
  const buttons = [];
  const updates = {};

  alerts.forEach((a, i) => {
    const currentPrice = priceMap[a.symbol];
    const titleLine = safeBold(`${i + 1}. ${a.symbol}`);

    if (currentPrice == null) {
      msg += `${titleLine}\n— нет данных о цене\n\n`;
    } else {
      const percentText = formatPercentRemaining(a.condition, a.price, currentPrice);
      let changeSinceLast = '';
      if (typeof lastViewMap[a.symbol] === 'number') {
        const change = ((currentPrice - lastViewMap[a.symbol]) / lastViewMap[a.symbol]) * 100;
        changeSinceLast = `\n📊 С последнего просмотра: ${formatChangeWithIcons(change)}`;
      }

      msg += `${titleLine}\n` +
        `Условие: ${a.condition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${a.price}*\n` +
        `Текущая: *${currentPrice}* ${percentText}${changeSinceLast}\n\n`;

      updates[a.symbol] = currentPrice;
    }
  });

  // сохраняем последние цены (буферизация внутри функции — безопасно)
  try {
    await updateUserLastViews(userId, updates);
  } catch (e) {
    console.error('Ошибка при updateUserLastViews:', e);
  }

  // формируем inline-кнопки исходя из опции
  if (options.includeDeleteButtons) {
    // кнопки удаления по одной на алерт + внизу кнопка "Назад"
    alerts.forEach((a, i) => {
      buttons.push([{ text: `❌ Удалить ${i + 1} ${a.symbol}`, callback_data: `del_${a._id.toString()}` }]);
    });
    buttons.push([{ text: '◀️ Назад', callback_data: 'back_to_alerts' }]);
  } else {
    // компактная версия: единственная кнопка "Удалить..."
    buttons.push([{ text: '❌ Удалить пару №...', callback_data: 'show_delete_menu' }]);
  }

  return { text: msg, buttons };
}

// ------------------ Telegram handlers ------------------
bot.start((ctx) => {
  ctx.session = {};
  ctx.reply('Привет! 🚀 Я бот для уведомлений о цене крипты (KuCoin API).', getMainMenu(ctx.from?.id));
});

bot.hears('➕ Создать алерт', (ctx) => {
  ctx.session = { step: 'symbol' };
  ctx.reply('Введи символ криптовалюты (например: BTC):', {
    reply_markup: { keyboard: [[{ text: '↩️ Отмена' }]], resize_keyboard: true }
  });
});

// ==== Мои алерты — компактный список + одна кнопка "Удалить..." ====
bot.hears('📋 Мои алерты', async (ctx) => {
  try {
    const { text, buttons } = await renderAlertsList(ctx.from.id, { includeDeleteButtons: false });
    await ctx.replyWithMarkdown(text, { reply_markup: { inline_keyboard: buttons } });
  } catch (err) {
    console.error('Ошибка в "Мои алерты":', err);
    ctx.reply('Произошла ошибка при получении алертов. Попробуй позже.');
  }
});

// --------- Показать меню удаления (нажатие "Удалить...") ----------
bot.action('show_delete_menu', async (ctx) => {
  try {
    await ctx.answerCbQuery();
    const userId = ctx.from.id;
    const { text, buttons } = await renderAlertsList(userId, { includeDeleteButtons: true });
    try {
      await ctx.editMessageText(text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: buttons } });
    } catch {
      await ctx.replyWithMarkdown(text, { reply_markup: { inline_keyboard: buttons } });
    }
  } catch (e) {
    console.error('Ошибка show_delete_menu:', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

// --------- Вернуться назад из меню удаления ----------
bot.action('back_to_alerts', async (ctx) => {
  try {
    await ctx.answerCbQuery();
    const userId = ctx.from.id;
    const { text, buttons } = await renderAlertsList(userId, { includeDeleteButtons: false });
    try {
      await ctx.editMessageText(text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: buttons } });
    } catch {
      await ctx.replyWithMarkdown(text, { reply_markup: { inline_keyboard: buttons } });
    }
  } catch (e) {
    console.error('Ошибка back_to_alerts:', e);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

// --------- Удаление алерта (callback del_<id>) ----------
bot.on('callback_query', async (ctx) => {
  try {
    const data = ctx.callbackQuery.data;
    if (!data) return ctx.answerCbQuery();

    // если это удаление конкретного алерта
    if (data.startsWith('del_')) {
      const idStr = data.replace('del_', '');
      await alertsCollection.deleteOne({ _id: new ObjectId(idStr) });

      // инвалидация кеша алертов для юзера
      invalidateUserAlertsCache(ctx.from.id);

      // после удаления показываем меню удаления заново
      const { text, buttons } = await renderAlertsList(ctx.from.id, { includeDeleteButtons: true });

      if (buttons.length) {
        try {
          await ctx.editMessageText(text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: buttons } });
        } catch {
          await ctx.replyWithMarkdown(text, { reply_markup: { inline_keyboard: buttons } });
        }
      } else {
        // нет алертов — сообщаем и отправляем главное меню
        try {
          await ctx.editMessageText('У тебя больше нет активных алертов.', { reply_markup: { inline_keyboard: [] } });
        } catch {}
        await ctx.reply('Вы в главном меню', getMainMenu(ctx.from.id));
      }

      await ctx.answerCbQuery('Алерт удалён');
      return;
    }

    // если data — наше кастомное действие (например show_delete_menu или back_to_alerts),
    // эти кейсы обрабатываются через bot.action выше — здесь просто гарантируем ответ.
    await ctx.answerCbQuery();
  } catch (err) {
    console.error('Ошибка в callback_query:', err);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

// ------------------ Остальная логика (создание алертов, статистика, проверка в фоне и т.д.) ------------------
// Здесь предполагается, что остальная логика (обработчики текста для создания алертов,
// статистика для создателя, периодическая проверка алертов и удаление неактивных пользователей)
// остаётся как в вашем рабочем коде — она совместима с новыми хендлерами кнопок выше.

// ------------------ Запуск бота ------------------
bot.launch().then(() => console.log('🚀 Бот запущен с кэшем и оптимизациями'));
