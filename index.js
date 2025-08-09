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

async function connectToMongo() {
  try {
    await client.connect();
    const db = client.db();
    alertsCollection = db.collection('alerts');
    console.log('✅ Подключено к MongoDB');
  } catch (err) {
    console.error('❌ Ошибка подключения к MongoDB:', err);
  }
}
await connectToMongo();

// ------------------ HTTP сервер ------------------
const app = express();
app.get('/', (req, res) => res.send('Бот работает! 🚀'));

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

bot.hears('📋 Мои алерты', async (ctx) => {
  const userAlerts = await alertsCollection.find({ userId: ctx.from.id }).toArray();
  if (userAlerts.length === 0) {
    return ctx.reply('У тебя нет активных алертов.', mainMenu);
  }
  let msg = '📋 Твои алерты:\n\n';
  for (let i = 0; i < userAlerts.length; i++) {
    const a = userAlerts[i];
    let currentPrice = 'нет данных';
    try {
      const res = await axios.get(`https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${a.symbol}`);
      currentPrice = res.data.data?.price || 'нет данных';
    } catch {}
    msg += `${i + 1}. ${a.symbol} ${a.condition} ${a.price} (текущая: ${currentPrice})\n`;
  }

  ctx.reply(msg, {
    reply_markup: {
      inline_keyboard: userAlerts.map((a, idx) => [
        { text: `❌ Удалить ${idx + 1}`, callback_data: `del_${a._id}` }
      ])
    }
  });
});

bot.hears('↩️ Отмена', (ctx) => {
  ctx.session = {};
  ctx.reply('Действие отменено ✅', mainMenu);
});

bot.on('text', async (ctx) => {
  const step = ctx.session.step;
  const text = ctx.message.text.trim();

  if (!step) return;

  if (step === 'symbol') {
    const symbol = text.toUpperCase();
    const fullSymbol = `${symbol}-USDT`;

    try {
      const res = await axios.get(`https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${fullSymbol}`);
      if (!res.data.data) throw new Error();

      ctx.session.symbol = fullSymbol;
      ctx.session.step = 'condition';

      ctx.reply(`✅ Монета найдена: ${fullSymbol}\nТекущая цена: ${res.data.data.price}\nВыбери условие:`, {
        reply_markup: {
          keyboard: [
            [{ text: '⬆️ Когда выше' }, { text: '⬇️ Когда ниже' }],
            [{ text: '↩️ Отмена' }]
          ],
          resize_keyboard: true
        }
      });
    } catch {
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
    const price = parseFloat(text);
    if (isNaN(price)) return ctx.reply('Введите корректное число цены');

    ctx.session.price = price;

    try {
      const res = await axios.get(`https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${ctx.session.symbol}`);
      const currentPrice = parseFloat(res.data.data.price);

      await alertsCollection.insertOne({
        userId: ctx.from.id,
        symbol: ctx.session.symbol,
        condition: ctx.session.condition,
        price: ctx.session.price
      });

      ctx.reply(`✅ Алерт создан: ${ctx.session.symbol} ${ctx.session.condition} ${ctx.session.price}\nТекущая цена: ${currentPrice}`, mainMenu);
      ctx.session = {};
    } catch {
      ctx.reply('❌ Ошибка при получении цены. Попробуй позже.', mainMenu);
      ctx.session = {};
    }
  }
});

bot.on('callback_query', async (ctx) => {
  const data = ctx.callbackQuery.data;
  if (!data.startsWith('del_')) return;

  const _id = data.replace('del_', '');
  await alertsCollection.deleteOne({ _id: new ObjectId(_id) });

  const updatedAlerts = await alertsCollection.find({ userId: ctx.from.id }).toArray();

  if (updatedAlerts.length === 0) {
    await ctx.editMessageText('У тебя больше нет активных алертов.', {
      reply_markup: { inline_keyboard: [] }
    });
    await ctx.reply('Вы в главном меню', mainMenu);
    return;
  }

  let msg = '📋 Твои алерты:\n\n';
  for (let i = 0; i < updatedAlerts.length; i++) {
    const a = updatedAlerts[i];
    let currentPrice = 'нет данных';
    try {
      const res = await axios.get(`https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${a.symbol}`);
      currentPrice = res.data.data?.price || 'нет данных';
    } catch {}
    msg += `${i + 1}. ${a.symbol} ${a.condition} ${a.price} (текущая: ${currentPrice})\n`;
  }

  await ctx.editMessageText(msg, {
    reply_markup: {
      inline_keyboard: updatedAlerts.map((a, idx) => [
        { text: `❌ Удалить ${idx + 1}`, callback_data: `del_${a._id}` }
      ])
    }
  });

  await ctx.answerCbQuery();
});

// 🔁 Проверка алертов каждую минуту
setInterval(async () => {
  const allAlerts = await alertsCollection.find({}).toArray();
  const pricesCache = new Map();

  for (const alert of allAlerts) {
    try {
      if (!pricesCache.has(alert.symbol)) {
        const res = await axios.get(`https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${alert.symbol}`);
        pricesCache.set(alert.symbol, parseFloat(res.data.data.price));
      }
      const currentPrice = pricesCache.get(alert.symbol);

      if (
        (alert.condition === '>' && currentPrice > alert.price) ||
        (alert.condition === '<' && currentPrice < alert.price)
      ) {
        bot.telegram.sendMessage(alert.userId, `🔔 ${alert.symbol} сейчас ${currentPrice}`);
        await alertsCollection.deleteOne({ _id: alert._id });
      }
    } catch (err) {
      console.error(`Ошибка получения цены ${alert.symbol}`, err.message);
    }
  }
}, 60000);

bot.launch().then(() => console.log('🚀 Бот запущен с MongoDB'));
