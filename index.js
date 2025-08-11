// crypto-bot/index.js
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
  // сравнение по строкам, чтобы избежать проблем с типами
  if (CREATOR_ID && String(userId) === String(CREATOR_ID)) {
    keyboard.push([{ text: '👥 Количество активных пользователей' }]);
  }
  return { reply_markup: { keyboard, resize_keyboard: true } };
}

// ------------------ КЭШИ ------------------
const pricesCache = new Map();        // symbol -> { price, time }
const alertsCache = new Map();        // userId -> { alerts, time }
const lastViewsCache = new Map();     // userId -> { map: {symbol:lastPrice}, time }
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
// Записываем в БД не чаще, чем раз в CACHE_TTL, иначе обновляем только память
async function updateUserLastViews(userId, updates) {
  if (!userId) return;
  const now = Date.now();
  const cached = lastViewsCache.get(userId);

  if (cached && (now - cached.time) < CACHE_TTL) {
    // обновляем только в памяти
    Object.assign(cached.map, updates);
    return;
  }

  // иначе — пишем в БД одним bulkWrite и обновляем кеш
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

// ------------------ Middleware: обновление lastActive (rate-limited, не чаще чем CACHE_TTL) ------------------
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
        // инвалидируем статистику, чтобы владелец видел свежие данные максимум через TTL
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
  if (change > 0) return `${value} 🟢⬆️`;
  if (change < 0) return `${value} 🔴⬇️`;
  return `${value}`;
}
const safeBold = text => `*${text}*`;

// ------------------ Отрисовка списка алертов ------------------
async function renderAlertsList(userId) {
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

    buttons.push([{ text: `❌ Удалить ${i + 1} пару`, callback_data: `del_${a._id.toString()}` }]);
  });

  try {
    await updateUserLastViews(userId, updates);
  } catch (e) {
    console.error('Ошибка при updateUserLastViews:', e);
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

bot.hears('📋 Мои алерты', async (ctx) => {
  try {
    const { text, buttons } = await renderAlertsList(ctx.from.id);
    await ctx.replyWithMarkdown(text, { reply_markup: { inline_keyboard: buttons } });
  } catch (err) {
    console.error('Ошибка в "Мои алерты":', err);
    ctx.reply('Произошла ошибка при получении алертов. Попробуй позже.');
  }
});

// --------- Кнопка статистики (только для создателя) ----------
bot.hears('👥 Количество активных пользователей', async (ctx) => {
  try {
    if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) {
      return ctx.reply('У вас нет доступа к этой команде.');
    }

    const now = Date.now();
    if (statsCache.count !== null && (now - statsCache.time) < CACHE_TTL) {
      return ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${statsCache.count}`);
    }

    const cutoff = new Date(Date.now() - INACTIVE_DAYS * 24 * 60 * 60 * 1000);
    const activeCount = await usersCollection.countDocuments({ lastActive: { $gte: cutoff } });
    statsCache = { count: activeCount, time: now };
    await ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${activeCount}`);
  } catch (e) {
    console.error('Ошибка получения количества активных пользователей:', e);
    ctx.reply('Ошибка получения статистики.');
  }
});

bot.hears('↩️ Отмена', (ctx) => {
  ctx.session = {};
  ctx.reply('Действие отменено ✅', getMainMenu(ctx.from.id));
});

bot.on('text', async (ctx) => {
  try {
    const step = ctx.session.step;
    const text = ctx.message.text.trim();

    if (!step) return;

    if (step === 'symbol') {
      const symbol = text.toUpperCase();
      const fullSymbol = `${symbol}-USDT`;
      const price = await getPrice(fullSymbol);

      if (price) {
        ctx.session.symbol = fullSymbol;
        ctx.session.step = 'condition';
        ctx.reply(`✅ Монета найдена: **${fullSymbol}**\nТекущая цена: *${price}*\nВыбери условие:`, {
          parse_mode: 'Markdown',
          reply_markup: {
            keyboard: [
              [{ text: '⬆️ Когда выше' }, { text: '⬇️ Когда ниже' }],
              [{ text: '↩️ Отмена' }]
            ],
            resize_keyboard: true
          }
        });
      } else {
        ctx.reply('❌ Такой пары нет на KuCoin. Попробуй другую монету.');
      }

    } else if (step === 'condition') {
      if (text === '⬆️ Когда выше') ctx.session.condition = '>';
      else if (text === '⬇️ Когда ниже') ctx.session.condition = '<';
      else return ctx.reply('Выбери из кнопок ⬆️ или ⬇️');

      ctx.session.step = 'price';
      ctx.reply('Введи цену:', {
        reply_markup: { keyboard: [[{ text: '↩️ Отмена' }]], resize_keyboard: true }
      });

    } else if (step === 'price') {
      const priceValue = parseFloat(text);
      if (isNaN(priceValue)) return ctx.reply('Введите корректное число цены');

      ctx.session.price = priceValue;
      const currentPrice = await getPrice(ctx.session.symbol);

      if (currentPrice) {
        await alertsCollection.insertOne({
          userId: ctx.from.id,
          symbol: ctx.session.symbol,
          condition: ctx.session.condition,
          price: ctx.session.price
        });

        // инвалидация кеша алертов для юзера (чтобы при следующем просмотре показать свежие данные)
        invalidateUserAlertsCache(ctx.from.id);

        ctx.reply(`✅ Алерт создан: **${ctx.session.symbol}** ${ctx.session.condition} *${ctx.session.price}*\nТекущая цена: *${currentPrice}*`, { parse_mode: 'Markdown', ...getMainMenu(ctx.from.id) });
      } else {
        ctx.reply('❌ Ошибка при получении цены. Попробуй позже.', getMainMenu(ctx.from.id));
      }
      ctx.session = {};
    }
  } catch (err) {
    console.error('Ошибка в on text:', err);
    ctx.reply('Произошла ошибка, повтори, пожалуйста.');
  }
});

// ------------------ Удаление алерта (callback) ------------------
bot.on('callback_query', async (ctx) => {
  try {
    const data = ctx.callbackQuery.data;
    if (!data?.startsWith('del_')) return ctx.answerCbQuery();

    const idStr = data.replace('del_', '');
    await alertsCollection.deleteOne({ _id: new ObjectId(idStr) });

    // инвалидация кеша алертов для юзера
    invalidateUserAlertsCache(ctx.from.id);

    const { text, buttons } = await renderAlertsList(ctx.from.id);

    if (buttons.length) {
      try {
        await ctx.editMessageText(text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: buttons } });
      } catch {
        await ctx.replyWithMarkdown(text, { reply_markup: { inline_keyboard: buttons } });
      }
    } else {
      try {
        await ctx.editMessageText('У тебя больше нет активных алертов.', { reply_markup: { inline_keyboard: [] } });
      } catch {}
      await ctx.reply('Вы в главном меню', getMainMenu(ctx.from.id));
    }

    await ctx.answerCbQuery('Алерт удалён');
  } catch (err) {
    console.error('Ошибка в callback_query:', err);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

// ------------------ Фоновая проверка алертов (каждые 120 секунд) ------------------
setInterval(async () => {
  try {
    const allAlerts = await alertsCollection.find({}).toArray();
    if (!allAlerts.length) return;

    const uniqueSymbols = [...new Set(allAlerts.map(a => a.symbol))];
    await Promise.all(uniqueSymbols.map(sym => getPrice(sym))); // getPrice сам кеширует

    for (const alert of allAlerts) {
      const currentPrice = pricesCache.get(alert.symbol)?.price;
      if (typeof currentPrice !== 'number') continue;

      if (
        (alert.condition === '>' && currentPrice > alert.price) ||
        (alert.condition === '<' && currentPrice < alert.price)
      ) {
        await bot.telegram.sendMessage(alert.userId,
          `🔔 *Сработал алерт!*\nМонета: **${alert.symbol}**\nЦена сейчас: *${currentPrice}*\nТвоё условие: ${alert.condition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${alert.price}*`,
          { parse_mode: 'Markdown' }
        );

        await alertsCollection.deleteOne({ _id: alert._id });

        // инвалидация кеша алертов для пользователя, у которого сработал алерт
        invalidateUserAlertsCache(alert.userId);
      }
    }
  } catch (err) {
    console.error('Ошибка в проверке алертов:', err);
  }
}, 120000);

// ------------------ Удаление неактивных пользователей ------------------
async function removeInactiveUsers() {
  try {
    const cutoff = new Date(Date.now() - INACTIVE_DAYS * 24 * 60 * 60 * 1000);
    const inactiveUsers = await usersCollection.find({ lastActive: { $lt: cutoff } }).toArray();

    if (!inactiveUsers.length) return;

    const ids = inactiveUsers.map(u => u.userId);
    await alertsCollection.deleteMany({ userId: { $in: ids } });
    await lastViewsCollection.deleteMany({ userId: { $in: ids } });
    await usersCollection.deleteMany({ userId: { $in: ids } });

    // очистка кэшей для удалённых пользователей
    ids.forEach(id => {
      invalidateUserAlertsCache(id);
      usersActivityCache.delete(id);
      lastViewsCache.delete(id);
    });

    // инвалидация статистики
    statsCache.time = 0;

    console.log(`Удалено ${ids.length} неактивных пользователей (>${INACTIVE_DAYS} дней) и их данные.`);
  } catch (e) {
    console.error('Ошибка при удалении неактивных пользователей:', e);
  }
}

// Запуск при старте и затем раз в сутки
await removeInactiveUsers();
setInterval(removeInactiveUsers, 24 * 60 * 60 * 1000);

// ------------------ Запуск бота ------------------
bot.launch().then(() => console.log('🚀 Бот запущен с кэшем и оптимизациями'));
