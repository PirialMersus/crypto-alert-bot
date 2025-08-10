// crypto-bot/index.js
import { Telegraf, session } from 'telegraf';
import axios from 'axios';
import dotenv from 'dotenv';
import express from 'express';
import { MongoClient, ObjectId } from 'mongodb';

dotenv.config();

const bot = new Telegraf(process.env.BOT_TOKEN);

bot.use(session());
bot.use((ctx, next) => {
  if (!ctx.session) ctx.session = {};
  return next();
});

// ------------------ Подключение к MongoDB ------------------
const client = new MongoClient(process.env.MONGO_URI);
let alertsCollection;
let lastViewsCollection;

async function connectToMongo() {
  try {
    await client.connect();
    const db = client.db();
    alertsCollection = db.collection('alerts');
    lastViewsCollection = db.collection('last_alerts_view');
    console.log('✅ Подключено к MongoDB');
  } catch (err) {
    console.error('❌ Ошибка подключения к MongoDB:', err);
  }
}
await connectToMongo();

// ------------------ HTTP сервер ------------------
const app = express();
app.get('/', (req, res) => res.send('Бот работает с монгошкой! 🚀🚀🚀'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🌐 HTTP сервер запущен на порту ${PORT}`));

// ------------------ Обработчики ошибок ------------------
process.on('uncaughtException', err => console.error('Необработанная ошибка:', err));
process.on('unhandledRejection', reason => console.error('Необработанное обещание:', reason));

const mainMenu = {
  reply_markup: {
    keyboard: [
      [{ text: '➕ Создать алерт' }, { text: '📋 Мои алерты' }]
    ],
    resize_keyboard: true
  }
};

// ------------------ Кэш цен ------------------
const pricesCache = new Map(); // symbol -> { price, time }
const CACHE_TTL = 15000; // 15 секунд

async function getPrice(symbol) {
  const now = Date.now();
  const cached = pricesCache.get(symbol);
  if (cached && (now - cached.time) < CACHE_TTL) {
    return cached.price;
  }
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

// ------------------ Helpers ------------------
function formatPercentRemaining(condition, targetPrice, currentPrice) {
  if (typeof currentPrice !== 'number' || typeof targetPrice !== 'number') return '';
  if (condition === '>' && currentPrice < targetPrice) {
    const pct = ((targetPrice - currentPrice) / targetPrice) * 100;
    return `(осталось ${pct.toFixed(2)}% до срабатывания)`;
  }
  if (condition === '<' && currentPrice > targetPrice) {
    const pct = ((currentPrice - targetPrice) / targetPrice) * 100;
    return `(осталось ${pct.toFixed(2)}% до срабатывания)`;
  }
  return '';
}

function formatChangeWithIcons(change) {
  // change — число в процентах
  const sign = change >= 0 ? '+' : '';
  const value = `${sign}${change.toFixed(2)}%`;
  if (change > 0) return `${value} 🟢⬆️`;
  if (change < 0) return `${value} 🔴⬇️`;
  return `${value}`;
}

function safeBold(text) {
  // Простая обёртка для жирного в Markdown (мы предполагаем что символы тикера безопасны)
  return `*${text}*`;
}

// ------------------ Telegram ------------------
bot.start((ctx) => {
  ctx.session = {};
  ctx.reply('Привет! 🚀 Я бот для уведомлений о цене крипты (KuCoin API).', mainMenu);
});

bot.hears('➕ Создать алерт', (ctx) => {
  ctx.session = { step: 'symbol' };
  ctx.reply('Введи символ криптовалюты (например: BTC):', {
    reply_markup: { keyboard: [[{ text: '↩️ Отмена' }]], resize_keyboard: true }
  });
});

// --- Главное: Мои алерты ---
bot.hears('📋 Мои алерты', async (ctx) => {
  try {
    const userId = ctx.from.id;
    const userAlerts = await alertsCollection.find({ userId }).toArray();

    if (!userAlerts.length) {
      return ctx.reply('У тебя нет активных алертов.', mainMenu);
    }

    // Уникальные символы (чтобы не дергать API лишний раз)
    const uniqueSymbols = [...new Set(userAlerts.map(a => a.symbol))];

    // Получаем цены параллельно
    const pricePromises = uniqueSymbols.map(sym => getPrice(sym));
    const priceResults = await Promise.all(pricePromises);
    const priceMap = {};
    uniqueSymbols.forEach((sym, i) => {
      priceMap[sym] = (typeof priceResults[i] === 'number') ? priceResults[i] : 'нет данных';
    });

    // Загружаем все lastViews одним запросом (для данного пользователя)
    const lastViews = await lastViewsCollection.find({ userId }).toArray();
    const hasPrevView = lastViews.length > 0;
    const lastViewMap = Object.fromEntries(lastViews.map(v => [v.symbol, v.lastPrice]));

    // Формируем сообщение: каждая запись — номер + пара жирным, затем условие/цена/проценты
    let msg = '📋 *Твои алерты:*\n\n';
    const buttons = []; // inline-кнопки: каждая кнопка соответствует одному алерту (текст: ❌ Удалить N пару)
    const updatePromises = [];

    for (let i = 0; i < userAlerts.length; i++) {
      const a = userAlerts[i];
      const currentPrice = priceMap[a.symbol];

      // Номер и пара (жирная строка)
      const titleLine = safeBold(`${i + 1} ${a.symbol}`);

      if (currentPrice === 'нет данных' || currentPrice === null) {
        msg += `${titleLine}\n— нет данных о цене\n\n`;
        buttons.push([{ text: `❌ Удалить ${i + 1} пару`, callback_data: `del_${a._id.toString()}` }]);
        continue;
      }

      const percentText = formatPercentRemaining(a.condition, a.price, currentPrice);

      // Показываем изменение с последнего просмотра только если ранее был просмотр
      let changeSinceLast = '';
      if (hasPrevView && lastViewMap[a.symbol] !== undefined && typeof lastViewMap[a.symbol] === 'number') {
        const change = ((currentPrice - lastViewMap[a.symbol]) / lastViewMap[a.symbol]) * 100;
        changeSinceLast = `\n📊 С последнего просмотра: ${formatChangeWithIcons(change)}`;
      }

      msg += `${titleLine}\n` +
        `Условие: ${a.condition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${a.price}*\n` +
        `Текущая: *${currentPrice}* ${percentText}${changeSinceLast}\n\n`;

      // кнопка удаления: ❌ Удалить N пару
      buttons.push([{ text: `❌ Удалить ${i + 1} пару`, callback_data: `del_${a._id.toString()}` }]);

      // Подготовим upsert lastPrice (собираем промисы)
      updatePromises.push(
        lastViewsCollection.updateOne(
          { userId, symbol: a.symbol },
          { $set: { lastPrice: currentPrice } },
          { upsert: true }
        ).catch(e => console.error('Ошибка сохранения lastPrice', e))
      );
    }

    // Выполняем все upsert параллельно
    await Promise.all(updatePromises);

    // Отправляем сообщение с inline-кнопками. Каждой строке соответствует своя кнопка.
    await ctx.replyWithMarkdown(msg, {
      reply_markup: { inline_keyboard: buttons }
    });
  } catch (err) {
    console.error('Ошибка в обработчике "Мои алерты":', err);
    ctx.reply('Произошла ошибка при получении алертов. Попробуй позже.');
  }
});

// --- Создание алерта ---
bot.hears('↩️ Отмена', (ctx) => {
  ctx.session = {};
  ctx.reply('Действие отменено ✅', mainMenu);
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
        ctx.reply(`✅ Алерт создан: **${ctx.session.symbol}** ${ctx.session.condition} *${ctx.session.price}*\nТекущая цена: *${currentPrice}*`, { parse_mode: 'Markdown', ...mainMenu });
      } else {
        ctx.reply('❌ Ошибка при получении цены. Попробуй позже.', mainMenu);
      }
      ctx.session = {};
    }
  } catch (err) {
    console.error('Ошибка в on text:', err);
    ctx.reply('Произошла ошибка, повтори, пожалуйста.');
  }
});

// --- Удаление алерта через inline-кнопку и мгновенная перерисовка ---
bot.on('callback_query', async (ctx) => {
  try {
    const data = ctx.callbackQuery.data;
    if (!data || !data.startsWith('del_')) {
      return ctx.answerCbQuery();
    }

    const id = data.replace('del_', '');
    // Удаляем алерт
    await alertsCollection.deleteOne({ _id: new ObjectId(id) });

    // Получаем userId и сообщение, чтобы перерисовать
    const userId = ctx.from.id;

    // Перезагружаем обновлённый список алертов
    const updatedAlerts = await alertsCollection.find({ userId }).toArray();

    if (!updatedAlerts.length) {
      // если пусто — редактируем сообщение и отправляем в меню
      try {
        await ctx.editMessageText('У тебя больше нет активных алертов.', { reply_markup: { inline_keyboard: [] } });
      } catch (e) {
        // иногда редактировать нельзя (старое сообщение) — просто игнорируем
      }
      await ctx.reply('Вы в главном меню', mainMenu);
      await ctx.answerCbQuery('Алерт удалён');
      return;
    }

    // Получаем цены для уникальных символов параллельно
    const uniqueSymbols = [...new Set(updatedAlerts.map(a => a.symbol))];
    const pricePromises = uniqueSymbols.map(sym => getPrice(sym));
    const priceResults = await Promise.all(pricePromises);
    const priceMap = {};
    uniqueSymbols.forEach((sym, i) => {
      priceMap[sym] = typeof priceResults[i] === 'number' ? priceResults[i] : 'нет данных';
    });

    // Получаем lastViews одной операцией
    const lastViews = await lastViewsCollection.find({ userId }).toArray();
    const hasPrevView = lastViews.length > 0;
    const lastViewMap = Object.fromEntries(lastViews.map(v => [v.symbol, v.lastPrice]));

    // Собираем текст
    let msg = '📋 *Твои алерты:*\n\n';
    const buttons = [];
    const updatePromises = [];

    for (let i = 0; i < updatedAlerts.length; i++) {
      const a = updatedAlerts[i];
      const currentPrice = priceMap[a.symbol];

      const titleLine = safeBold(`${i + 1} ${a.symbol}`);

      if (currentPrice === 'нет данных') {
        msg += `${titleLine}\n— нет данных о цене\n\n`;
        buttons.push([{ text: `❌ Удалить ${i + 1} пару`, callback_data: `del_${a._id.toString()}` }]);
        continue;
      }

      const percentText = formatPercentRemaining(a.condition, a.price, currentPrice);

      let changeSinceLast = '';
      if (hasPrevView && lastViewMap[a.symbol] !== undefined && typeof lastViewMap[a.symbol] === 'number') {
        const change = ((currentPrice - lastViewMap[a.symbol]) / lastViewMap[a.symbol]) * 100;
        changeSinceLast = `\n📊 С последнего просмотра: ${formatChangeWithIcons(change)}`;
      }

      msg += `${titleLine}\n` +
        `Условие: ${a.condition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${a.price}*\n` +
        `Текущая: *${currentPrice}* ${percentText}${changeSinceLast}\n\n`;

      buttons.push([{ text: `❌ Удалить ${i + 1} пару`, callback_data: `del_${a._id.toString()}` }]);

      // Обновляем last price для этой пары (собираем промисы)
      updatePromises.push(
        lastViewsCollection.updateOne(
          { userId, symbol: a.symbol },
          { $set: { lastPrice: currentPrice } },
          { upsert: true }
        ).catch(e => console.error('Ошибка сохранения lastPrice после удаления', e))
      );
    }

    await Promise.all(updatePromises);

    // Пытаемся отредактировать исходное сообщение (будет работать в большинстве случаев)
    try {
      await ctx.editMessageText(msg, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: buttons } });
    } catch (e) {
      // если редактировать нельзя (сообщение старое), отправляем новое
      await ctx.replyWithMarkdown(msg, { reply_markup: { inline_keyboard: buttons } });
    }

    await ctx.answerCbQuery('Алерт удалён');
  } catch (err) {
    console.error('Ошибка в callback_query:', err);
    try { await ctx.answerCbQuery('Ошибка'); } catch (e) {}
  }
});

// 🔁 Проверка алертов каждые 60 секунд
setInterval(async () => {
  try {
    const allAlerts = await alertsCollection.find({}).toArray();
    if (!allAlerts.length) return;

    const uniqueSymbols = [...new Set(allAlerts.map(a => a.symbol))];

    // Обновляем цены параллельно
    await Promise.all(uniqueSymbols.map(sym => getPrice(sym)));

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
      }
    }
  } catch (err) {
    console.error('Ошибка в проверке алертов:', err);
  }
}, 60000);

bot.launch().then(() => console.log('🚀 Бот запущен с MongoDB и оптимизированным кэшем'));
