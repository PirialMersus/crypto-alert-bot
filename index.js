import { Telegraf, session } from 'telegraf';
import axios from 'axios';
import dotenv from 'dotenv';
import express from 'express';
import fs from 'fs';

dotenv.config();

const bot = new Telegraf(process.env.BOT_TOKEN);

bot.use(session());
bot.use((ctx, next) => {
  if (!ctx.session) ctx.session = {};
  return next();
});

// ------------------ Работа с хранилищем ------------------
const ALERTS_FILE = 'alerts.json';

function loadAlerts() {
  try {
    if (fs.existsSync(ALERTS_FILE)) {
      return JSON.parse(fs.readFileSync(ALERTS_FILE, 'utf8'));
    }
    return [];
  } catch (err) {
    console.error('Ошибка загрузки alerts.json:', err);
    return [];
  }
}

function saveAlerts() {
  try {
    fs.writeFileSync(ALERTS_FILE, JSON.stringify(alerts, null, 2), 'utf8');
  } catch (err) {
    console.error('Ошибка сохранения alerts.json:', err);
  }
}

let alerts = loadAlerts();
// ---------------------------------------------------------

// ------------------ HTTP сервер для Render + UptimeRobot ------------------
const app = express();
app.get('/', (req, res) => res.send('Бот работает! 🚀'));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`🌐 HTTP сервер запущен на порту ${PORT}`));
// ---------------------------------------------------------

// ------------------ Обработчики ошибок ------------------
process.on('uncaughtException', err => {
  console.error('Необработанная ошибка:', err);
});
process.on('unhandledRejection', reason => {
  console.error('Необработанное обещание:', reason);
});
// ---------------------------------------------------------

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
  const userAlerts = alerts.filter(a => a.userId === ctx.from.id);
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
      inline_keyboard: userAlerts.map((_, idx) => [
        { text: `❌ Удалить ${idx + 1}`, callback_data: `del_${idx}` }
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

      alerts.push({
        userId: ctx.from.id,
        symbol: ctx.session.symbol,
        condition: ctx.session.condition,
        price: ctx.session.price
      });
      saveAlerts();

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

  const index = parseInt(data.split('_')[1], 10);
  const userAlerts = alerts.filter(a => a.userId === ctx.from.id);

  if (index >= 0 && index < userAlerts.length) {
    const alertToRemove = userAlerts[index];
    alerts = alerts.filter(a => a !== alertToRemove);
    saveAlerts();

    const updatedAlerts = alerts.filter(a => a.userId === ctx.from.id);

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
        inline_keyboard: updatedAlerts.map((_, idx) => [
          { text: `❌ Удалить ${idx + 1}`, callback_data: `del_${idx}` }
        ])
      }
    });
  }

  await ctx.answerCbQuery();
});

// Проверка алертов каждую минуту
setInterval(async () => {
  for (const alert of [...alerts]) {
    try {
      const res = await axios.get(`https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=${alert.symbol}`);
      const currentPrice = parseFloat(res.data.data.price);

      if (
        (alert.condition === '>' && currentPrice > alert.price) ||
        (alert.condition === '<' && currentPrice < alert.price)
      ) {
        bot.telegram.sendMessage(alert.userId, `🔔 ${alert.symbol} сейчас ${currentPrice}`);
        alerts = alerts.filter(a => a !== alert);
        saveAlerts();
      }
    } catch (err) {
      console.error(`Ошибка получения цены ${alert.symbol}`, err.message);
    }
  }
}, 60000);

bot.launch().then(() => console.log('🚀 Бот запущен с KuCoin API'));
