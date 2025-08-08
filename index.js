import { Telegraf, session } from 'telegraf';
import axios from 'axios';
import dotenv from 'dotenv';

dotenv.config();

const bot = new Telegraf(process.env.BOT_TOKEN);

// Подключаем сессии
bot.use(session());
bot.use((ctx, next) => {
  if (!ctx.session) ctx.session = {};
  return next();
});

// Хранилище алертов
let alerts = [];

// Стартовое меню
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
  ctx.reply('Привет! 🚀 Я бот для уведомлений о цене крипты.', mainMenu);
});

bot.hears('➕ Создать алерт', (ctx) => {
  ctx.session = {};
  ctx.session.step = 'symbol';
  ctx.reply('Введи символ криптовалюты (например: BTC):', {
    reply_markup: { keyboard: [[{ text: '↩️ Отмена' }]], resize_keyboard: true }
  });
});

bot.hears('📋 Мои алерты', (ctx) => {
  const userAlerts = alerts.filter(a => a.userId === ctx.from.id);
  if (userAlerts.length === 0) {
    return ctx.reply('У тебя нет активных алертов.', mainMenu);
  }
  let msg = 'Твои алерты:\n';
  userAlerts.forEach((a, i) => {
    msg += `${i + 1}. ${a.symbol} ${a.condition} ${a.price}\n`;
  });
  ctx.reply(msg, mainMenu);
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
    const fullSymbol = symbol + 'USDT';

    try {
      const res = await axios.get(`https://api.binance.com/api/v3/ticker/price?symbol=${fullSymbol}`);
      ctx.session.symbol = fullSymbol;
      ctx.session.step = 'condition';

      ctx.reply(
        `✅ Монета найдена: ${fullSymbol}\nТекущая цена: ${res.data.price}\nВыбери условие:`,
        {
          reply_markup: {
            keyboard: [
              [{ text: '⬆️ Когда выше' }, { text: '⬇️ Когда ниже' }],
              [{ text: '↩️ Отмена' }]
            ],
            resize_keyboard: true
          }
        }
      );
    } catch {
      ctx.reply('❌ Такой пары нет на Binance. Попробуй другую монету.');
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
      const res = await axios.get(`https://api.binance.com/api/v3/ticker/price?symbol=${ctx.session.symbol}`);
      const currentPrice = parseFloat(res.data.price);

      alerts.push({
        userId: ctx.from.id,
        symbol: ctx.session.symbol,
        condition: ctx.session.condition,
        price: ctx.session.price
      });

      ctx.reply(
        `✅ Алерт создан: ${ctx.session.symbol} ${ctx.session.condition} ${ctx.session.price}\nТекущая цена: ${currentPrice}`,
        mainMenu
      );
      ctx.session = {};
    } catch (err) {
      ctx.reply('❌ Ошибка при получении цены. Попробуй позже.', mainMenu);
      ctx.session = {};
    }
  }
});

// Проверка алертов каждую минуту
setInterval(async () => {
  for (const alert of alerts) {
    try {
      const res = await axios.get(`https://api.binance.com/api/v3/ticker/price?symbol=${alert.symbol}`);
      const currentPrice = parseFloat(res.data.price);

      if (
        (alert.condition === '>' && currentPrice > alert.price) ||
        (alert.condition === '<' && currentPrice < alert.price)
      ) {
        bot.telegram.sendMessage(alert.userId, `🔔 ${alert.symbol} сейчас ${currentPrice}`);
        alerts = alerts.filter(a => a !== alert);
      }
    } catch (err) {
      console.error(`Ошибка получения цены ${alert.symbol}`, err.message);
    }
  }
}, 60000);

bot.launch().then(() => console.log('🚀 Бот запущен'));
