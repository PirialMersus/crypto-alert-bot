// src/bot.js
import { Telegraf, session } from 'telegraf';
import dotenv from 'dotenv';
import { connectToMongo, ObjectId, countDocumentsWithTimeout } from './db.js';
import { createServer } from './server.js';
import { startTickersRefresher, refreshAllTickers, getCachedPrice } from './prices.js';
import { startAlertsChecker, renderAlertsList, buildDeleteInlineForUser, renderOldAlertsList } from './alerts.js';
import { removeInactive } from './cleanup.js';
import {
  getUserRecentSymbols,
  pushRecentSymbol,
  getUserAlertsOrder,
  setUserAlertsOrder,
  getUserAlertsCached,
  invalidateUserAlertsCache,
  statsCache,
  getUserAlertLimit,
  setUserAlertLimit,
  resolveUserLang
} from './cache.js';
import { fmtNum } from './utils.js';
import { sendDailyToUser, processDailyQuoteRetry, watchForNewQuotes, fetchAndStoreDailyMotivation } from './daily.js';
import { CACHE_TTL, INACTIVE_DAYS, DAY_MS, IMAGE_FETCH_HOUR, PREPARE_SEND_HOUR } from './constants.js';

dotenv.config();

const BOT_TOKEN = process.env.BOT_TOKEN;
const CREATOR_ID = process.env.CREATOR_ID ? parseInt(process.env.CREATOR_ID, 10) : null;
if (!BOT_TOKEN) throw new Error('BOT_TOKEN не задан в окружении');

export const bot = new Telegraf(BOT_TOKEN);

bot.use(session());
bot.use((ctx, next) => { if (!ctx.session) ctx.session = {}; return next(); });
bot.use(async (ctx, next) => {
  try {
    if (ctx.from && ctx.from.id) {
      const { usersCollection } = await import('./db.js');
      await usersCollection.updateOne(
        { userId: ctx.from.id },
        { $set: { userId: ctx.from.id, lastActive: new Date(), language_code: ctx.from.language_code || null } },
        { upsert: true }
      );
    }
  } catch (e) {
    console.warn('update lastActive failed', e?.message || e);
  }
  return next();
});

function getMainMenuSync(userId, lang = 'ru') {
  const isEn = String(lang).split('-')[0] === 'en';
  const create = isEn ? '➕ Create alert' : '➕ Создать алерт';
  const my = isEn ? '📋 My alerts' : '📋 Мои алерты';
  const settings = isEn ? '⚙️ Settings' : '⚙️ Настройки';
  const old = isEn ? '📜 Old alerts' : '📜 Старые алерты';
  const search = isEn ? '🔎 Search old alerts' : '🔎 Поиск старых алертов';
  const motivate = isEn ? '🌅 Send motivation' : '🌅 Прислать мотивацию';
  const stats = isEn ? '👥 Active users' : '👥 Количество активных пользователей';
  const support = isEn ? 'Wishes/Support' : 'Пожелания/техподдержка';
  const kb = [
    [{ text: create }, { text: my }],
    [{ text: old }, { text: search }],
    [{ text: support }, { text: settings }]
  ];
  if (CREATOR_ID && String(userId) === String(CREATOR_ID)) {
    kb.push([{ text: motivate }], [{ text: stats }]);
  }
  return { reply_markup: { keyboard: kb, resize_keyboard: true } };
}

async function buildSettingsInlineForUser(userId) {
  const order = await getUserAlertsOrder(userId).catch(()=> 'new_bottom');
  const lang = await resolveUserLang(userId).catch(()=> 'ru');
  const isTop = order === 'new_top';
  const isEn = String(lang).split('-')[0] === 'en';
  const orderLeft = isTop ? (isEn ? '✅ New on top' : '✅ Новые сверху') : (isEn ? 'New on top' : 'Новые сверху');
  const orderRight = !isTop ? (isEn ? '✅ New on bottom' : '✅ Новые снизу') : (isEn ? 'New on bottom' : 'Новые снизу');
  const langEn = (isEn ? '✅ English' : 'English');
  const langRu = (!isEn ? '✅ Русский' : 'Русский');
  return {
    inline_keyboard: [
      [
        { text: orderLeft, callback_data: 'set_order_new_top' },
        { text: orderRight, callback_data: 'set_order_new_bottom' }
      ],
      [
        { text: langEn, callback_data: 'set_lang_en' },
        { text: langRu, callback_data: 'set_lang_ru' }
      ],
      [{ text: isEn ? '↩️ Back' : '↩️ Назад', callback_data: 'back_to_main' }]
    ]
  };
}

function buildCancelButton(lang) {
  const isEn = String(lang).split('-')[0] === 'en';
  return isEn ? { text: '↩️ Cancel' } : { text: '↩️ Отмена' };
}

function buildDirectionKeyboard(lang) {
  const isEn = String(lang).split('-')[0] === 'en';
  if (isEn) {
    return { keyboard: [[{ text: '⬆️ When above' }, { text: '⬇️ When below' }], [buildCancelButton(lang)]], resize_keyboard: true };
  } else {
    return { keyboard: [[{ text: '⬆️ Когда выше' }, { text: '⬇️ Когда ниже' }], [buildCancelButton(lang)]], resize_keyboard: true };
  }
}

function buildAskSlKeyboard(lang) {
  const isEn = String(lang).split('-')[0] === 'en';
  if (isEn) {
    return { keyboard: [[{ text: '🛑 Add SL' }, { text: '⏭️ Skip SL' }], [buildCancelButton(lang)]], resize_keyboard: true };
  } else {
    return { keyboard: [[{ text: '🛑 Добавить SL' }, { text: '⏭️ Без SL' }], [buildCancelButton(lang)]], resize_keyboard: true };
  }
}

dotenv.config();

bot.start(async (ctx) => {
  ctx.session = {};
  const lang = await resolveUserLang(ctx.from?.id, null, ctx.from?.language_code);
  const isEn = String(lang).split('-')[0] === 'en';
  const greet = isEn ? 'Hello! I am a crypto alert bot.' : 'Привет! Я бот-алерт для крипты.';
  await ctx.reply(`${greet}\n${isEn ? '(Language: English)' : '(Язык: Русский)'}`, getMainMenuSync(ctx.from.id, lang));
});

bot.hears('⚙️ Настройки', async (ctx) => { const inline = await buildSettingsInlineForUser(ctx.from.id); await ctx.reply('Настройки отображения алертов:', { reply_markup: inline }); });
bot.hears('⚙️ Settings', async (ctx) => { const inline = await buildSettingsInlineForUser(ctx.from.id); await ctx.reply('Display settings:', { reply_markup: inline }); });

bot.hears('➕ Создать алерт', async (ctx) => {
  try {
    ctx.session = { step: 'symbol' };
    refreshAllTickers().catch(()=>{});
    const lang = await resolveUserLang(ctx.from.id);
    const recent = await getUserRecentSymbols(ctx.from.id);
    const suggest = [...new Set([...recent, ...['BTC','ETH','SOL','BNB','XRP','DOGE']])].slice(0,6).map(s=>({ text: s }));
    const cancelBtn = buildCancelButton(lang);
    const kb = suggest.length ? [suggest, [cancelBtn]] : [[cancelBtn]];
    await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? 'Enter symbol (e.g. BTC) or press a button:' : 'Введи символ (например BTC) или нажми кнопку:', { reply_markup: { keyboard: kb, resize_keyboard: true } });
  } catch (e) {
    ctx.session = {};
    await ctx.reply('Ошибка при запуске создания алерта');
  }
});
bot.hears('➕ Create alert', async (ctx) => {
  try {
    ctx.session = { step: 'symbol' };
    refreshAllTickers().catch(()=>{});
    const lang = await resolveUserLang(ctx.from.id);
    const recent = await getUserRecentSymbols(ctx.from.id);
    const suggest = [...new Set([...recent, ...['BTC','ETH','SOL','BNB','XRP','DOGE']])].slice(0,6).map(s=>({ text: s }));
    const cancelBtn = buildCancelButton(lang);
    const kb = suggest.length ? [suggest, [cancelBtn]] : [[cancelBtn]];
    await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? 'Enter symbol (e.g. BTC) or press a button:' : 'Введи символ (например BTC) или нажми кнопку:', { reply_markup: { keyboard: kb, resize_keyboard: true } });
  } catch (e) {
    ctx.session = {};
    await ctx.reply('Error starting alert creation.');
  }
});

bot.hears('↩️ Отмена', async (ctx) => { ctx.session = {}; const lang = await resolveUserLang(ctx.from.id); await ctx.reply('Отмена ✅', getMainMenuSync(ctx.from.id, lang)); });
bot.hears('↩️ Cancel', async (ctx) => { ctx.session = {}; const lang = await resolveUserLang(ctx.from.id); await ctx.reply('Cancelled ✅', getMainMenuSync(ctx.from.id, lang)); });

bot.hears('📋 Мои алерты', async (ctx) => {
  try {
    try { await bot.telegram.sendChatAction(ctx.chat.id, 'typing'); } catch (_){ }
    const lang = await resolveUserLang(ctx.from.id);
    const { pages } = await renderAlertsList(ctx.from.id, { fast: false, lang });
    const first = pages[0];
    await ctx.reply(first.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons } });
  } catch (e) {
    await ctx.reply('Ошибка при получении алертов.');
  }
});
bot.hears('📋 My alerts', async (ctx) => {
  try {
    const lang = await resolveUserLang(ctx.from.id);
    const { pages } = await renderAlertsList(ctx.from.id, { fast: false, lang });
    const first = pages[0];
    await ctx.reply(first.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons } });
  } catch (e) {
    await ctx.reply('Error fetching alerts.');
  }
});

bot.hears('📜 Старые алерты', async (ctx) => {
  ctx.session = { step: 'old_alerts_select_days' };
  const lang = await resolveUserLang(ctx.from.id);
  const isEn = String(lang).split('-')[0] === 'en';
  const kb = [[{ text: isEn ? '7 days' : '7 дней' }, { text: isEn ? '30 days' : '30 дней' }, { text: isEn ? '90 days' : '90 дней' }], [buildCancelButton(lang)]];
  await ctx.reply(isEn ? 'Choose a period to view old alerts:' : 'Выбери период для просмотра старых алертов:', { reply_markup: { keyboard: kb, resize_keyboard: true } });
});
bot.hears('📜 Old alerts', async (ctx) => {
  ctx.session = { step: 'old_alerts_select_days' };
  const kb = [[{ text: '7 days' }, { text: '30 days' }, { text: '90 days' }], [buildCancelButton('en')]];
  await ctx.reply('Choose a period to view old alerts:', { reply_markup: { keyboard: kb, resize_keyboard: true } });
});

bot.hears('🔎 Поиск старых алертов', async (ctx) => {
  ctx.session = { step: 'old_alerts_search' };
  const lang = await resolveUserLang(ctx.from.id);
  await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? 'Enter query in format: SYMBOL [DAYS]\nExamples: "BTC", "BTC 30". Default DAYS=30.' : 'Введи запрос в формате: SYMBOL [DAYS]\nПримеры: "BTC", "BTC 30". По умолчанию DAYS=30.', { reply_markup: { keyboard: [[buildCancelButton(lang)]], resize_keyboard: true } });
});
bot.hears('🔎 Search old alerts', async (ctx) => {
  ctx.session = { step: 'old_alerts_search' };
  await ctx.reply('Enter query in format: SYMBOL [DAYS]\nExamples: "BTC", "BTC 30". Default DAYS=30.', { reply_markup: { keyboard: [[buildCancelButton('en')]], resize_keyboard: true } });
});

bot.hears('🌅 Прислать мотивацию', async (ctx) => {
  try {
    if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) return ctx.reply('У вас нет доступа к этой команде.');
    const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Kyiv' });
    const ok = await sendDailyToUser(bot, ctx.from.id, dateStr, { disableNotification: false });
    if (ok) {
      const { pendingDailySendsCollection } = await import('./db.js');
      await pendingDailySendsCollection.updateOne({ userId: ctx.from.id, date: dateStr }, { $set: { sent: true, sentAt: new Date(), quoteSent: true, permanentFail: false } }, { upsert: true });
    } else {
      const { pendingDailySendsCollection } = await import('./db.js');
      await pendingDailySendsCollection.updateOne({ userId: ctx.from.id, date: dateStr }, { $set: { sent: false, createdAt: new Date(), permanentFail: true } }, { upsert: true });
    }
  } catch (e) {
    await ctx.reply('Ошибка при отправке мотивации');
  }
});
bot.hears('🌅 Send motivation', async (ctx) => {
  try {
    if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) return ctx.reply("You don't have access to this command.");
    const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Kyiv' });
    const ok = await sendDailyToUser(bot, ctx.from.id, dateStr, { disableNotification: false });
    if (ok) {
      const { pendingDailySendsCollection } = await import('./db.js');
      await pendingDailySendsCollection.updateOne({ userId: ctx.from.id, date: dateStr }, { $set: { sent: true, sentAt: new Date(), quoteSent: true, permanentFail: false } }, { upsert: true });
    } else {
      const { pendingDailySendsCollection } = await import('./db.js');
      await pendingDailySendsCollection.updateOne({ userId: ctx.from.id, date: dateStr }, { $set: { sent: false, createdAt: new Date(), permanentFail: true } }, { upsert: true });
      await ctx.reply('Failed to send motivation (check logs).');
    }
  } catch (e) {
    await ctx.reply('Error sending motivation.');
  }
});

bot.hears('Пожелания/техподдержка', async (ctx) => {
  const lang = await resolveUserLang(ctx.from.id);
  const msg = lang && String(lang).split('-')[0] === 'en'
    ? "The bot is completely free and has no restrictions. If you have suggestions to improve functionality, want to add something, or would like to thank me with a cup of coffee — write to me in private @pirial_mersus"
    : "Бот полностью бесплатен и в нем нет никаких ограничений. Если у вас есть какие то предложения по улучшению функциональности. Или вам хочется чтото добавить. Или вы хотите отблагодарить меня чашечкой кофе - напишите в личку @pirial_mersus";
  await ctx.reply(msg, getMainMenuSync(ctx.from.id, lang));
});
bot.hears('Wishes/Support', async (ctx) => {
  const lang = await resolveUserLang(ctx.from.id);
  const msg = lang && String(lang).split('-')[0] === 'en'
    ? "The bot is completely free and has no restrictions. If you have suggestions to improve functionality, want to add something, or would like to thank me with a cup of coffee — write to me in private @pirial_mersus"
    : "Бот полностью бесплатен и в нем нет никаких ограничений. Если у вас есть какие то предложения по улучшению функциональности. Или вам хочется чтото добавить. Или вы хотите отблагодарить меня чашечкой кофе - напишите в личку @pirial_mersus";
  await ctx.reply(msg, getMainMenuSync(ctx.from.id, lang));
});

async function handleActiveUsers(ctx) {
  try {
    if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) { return ctx.reply('У вас нет доступа к этой команде.'); }
    const now = Date.now();
    if (statsCache.count !== null && (now - statsCache.time) < CACHE_TTL) { return ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${statsCache.count}`); }
    const cutoff = new Date(Date.now() - INACTIVE_DAYS * DAY_MS);
    let activeCount;
    try {
      activeCount = await countDocumentsWithTimeout('users', { lastActive: { $gte: cutoff }, $or: [{ botBlocked: { $exists: false } }, { botBlocked: false }] }, 7000);
    }
    catch (err) {
      return ctx.reply('Ошибка получения статистики (таймаут или проблема с БД). Попробуйте позже.');
    }
    statsCache.count = activeCount;
    statsCache.time = now;
    await ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${activeCount}`);
  } catch (e) {
    await ctx.reply('Ошибка получения статистики.');
  }
}

bot.hears('👥 Количество активных пользователей', async (ctx) => { await handleActiveUsers(ctx); });
bot.hears('👥 Active users', async (ctx) => { await handleActiveUsers(ctx); });

bot.on('callback_query', async (ctx) => {
  try {
    const data = ctx.callbackQuery?.data;
    if (!data) return ctx.answerCbQuery();

    const lang = await resolveUserLang(ctx.from.id);

    // back to main (settings) handler
    if (data === 'back_to_main') {
      try {
        await ctx.editMessageReplyMarkup({ inline_keyboard: [] });
      } catch (e) {}
      try { await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? 'Back to main' : 'Назад в меню', getMainMenuSync(ctx.from.id, lang)); } catch {}
      await ctx.answerCbQuery();
      return;
    }

    const mPage = data.match(/^alerts_page_(\d+)_view$/);
    if (mPage) {
      const pageIdx = parseInt(mPage[1], 10);
      const { pages } = await renderAlertsList(ctx.from.id, { fast: true, lang });
      const page = pages[Math.max(0, Math.min(pageIdx, pages.length - 1))] || pages[0];
      try {
        await ctx.editMessageText(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } });
      } catch (e) { try { await ctx.reply(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } }); } catch {} }
      await ctx.answerCbQuery();
      return;
    }

    const mShow = data.match(/^show_delete_menu_(all|\d+)$/);
    if (mShow) {
      const token = mShow[1];
      const { pages } = await renderAlertsList(ctx.from.id, { fast: true, lang });
      const totalPages = pages.length;
      let sourcePage = null;
      if (token !== 'all') sourcePage = Math.max(0, Math.min(parseInt(token, 10), totalPages - 1));
      const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage, totalPages: (sourcePage === null ? null : totalPages), lang });
      try { await ctx.editMessageReplyMarkup({ inline_keyboard: inline }); }
      catch (err) {
        try { const originalText = ctx.update.callback_query.message?.text || 'Your alerts'; await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } }); } catch {}
      }
      await ctx.answerCbQuery();
      return;
    }

    const mBack = data.match(/^back_to_alerts(?:_p(\d+))?$/);
    if (mBack) {
      const p = mBack[1] ? parseInt(mBack[1], 10) : 0;
      const { pages } = await renderAlertsList(ctx.from.id, { fast: true, lang });
      const page = pages[Math.max(0, Math.min(p, pages.length - 1))] || pages[0];
      try { await ctx.editMessageText(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } }); }
      catch (e) { try { await ctx.reply(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } }); } catch {} }
      await ctx.answerCbQuery();
      return;
    }

    const mSet = data.match(/^set_order_(new_top|new_bottom)$/);
    if (mSet) {
      const order = mSet[1];
      await setUserAlertsOrder(ctx.from.id, order).catch(()=>{});
      const inline = await buildSettingsInlineForUser(ctx.from.id);
      try {
        await ctx.editMessageReplyMarkup(inline);
      } catch (e) { try { await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? 'Order set' : 'Порядок установлен', { reply_markup: inline }); } catch {} }
      await ctx.answerCbQuery(lang && String(lang).split('-')[0] === 'en' ? 'Order set' : 'Порядок установлен');
      return;
    }

    const mDel = data.match(/^del_([0-9a-fA-F]{24})_p(all|\d+)$/);
    const mLegacy = !mDel && data.startsWith('del_') ? data.match(/^del_([0-9a-fA-F]{24})$/) : null;

    if (mDel || mLegacy) {
      const id = (mDel ? mDel[1] : mLegacy[1]);
      const token = mDel ? mDel[2] : null;
      const { alertsCollection } = await import('./db.js');
      const doc = await alertsCollection.findOne({ _id: new ObjectId(id) });
      if (!doc) { await ctx.answerCbQuery('Алерт не найден'); return; }

      let sourcePage = null;
      if (token) { sourcePage = (token === 'all') ? null : Math.max(0, parseInt(token, 10)); }
      else {
        try {
          const alertsBefore = await getUserAlertsCached(ctx.from.id);
          const idxBefore = alertsBefore.findIndex(a => String(a._id) === String(doc._id) || a._id?.toString() === id);
          if (idxBefore >= 0) sourcePage = Math.floor(idxBefore / 20); else sourcePage = 0;
        } catch (e) { sourcePage = 0; }
      }

      try {
        const { alertsArchiveCollection } = await import('./db.js');
        await alertsArchiveCollection.insertOne({
          ...doc,
          deletedAt: new Date(),
          deleteReason: 'user_deleted',
          archivedAt: new Date()
        });
      } catch (e) {}

      const { alertsCollection: ac } = await import('./db.js');
      await ac.deleteOne({ _id: new ObjectId(id) });
      invalidateUserAlertsCache(ctx.from.id);

      const alertsAfter = await getUserAlertsCached(ctx.from.id);
      const computedTotalPages = Math.max(1, Math.ceil((alertsAfter?.length || 0) / 20));
      if (sourcePage !== null) { sourcePage = Math.max(0, Math.min(sourcePage, computedTotalPages - 1)); }

      const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage, totalPages: (sourcePage === null ? null : computedTotalPages), lang });

      if (!inline || inline.length === 0) {
        try { await ctx.editMessageText(lang && String(lang).split('-')[0] === 'en' ? 'You have no active alerts.' : 'У тебя больше нет активных алертов.', { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [] } }); } catch {}
        await ctx.answerCbQuery(lang && String(lang).split('-')[0] === 'en' ? 'Alert deleted' : 'Алерт удалён');
        return;
      }

      try { await ctx.editMessageReplyMarkup({ inline_keyboard: inline }); } catch (err) {
        try {
          const originalText = ctx.update.callback_query.message?.text || (lang && String(lang).split('-')[0] === 'en' ? 'Your alerts' : 'Твои алерты');
          await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } });
        } catch (e) {}
      }

      await ctx.answerCbQuery(lang && String(lang).split('-')[0] === 'en' ? 'Alert deleted' : 'Алерт удалён');
      return;
    }

    const mOldPage = data.match(/^old_alerts_page_(\d+)_view_(d(\d+)_q(.*))$/);
    if (mOldPage) {
      const pageIdx = parseInt(mOldPage[1], 10);
      const token = mOldPage[2];
      const mToken = token.match(/^d(\d+)_q(.*)$/);
      const days = mToken ? parseInt(mToken[1], 10) : 30;
      const q = mToken ? decodeURIComponent(mToken[2]) : '';
      const opts = { days, symbol: q || null, token, lang };
      const { pages } = await renderOldAlertsList(ctx.from.id, opts);
      const page = pages[Math.max(0, Math.min(pageIdx, pages.length - 1))] || pages[0];
      try {
        await ctx.editMessageText(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } });
      } catch (e) {
        try { await ctx.reply(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } }); } catch {}
      }
      await ctx.answerCbQuery();
      return;
    }

    const mSetLang = data.match(/^set_lang_(ru|en)$/);
    if (mSetLang) {
      const newLang = mSetLang[1];
      try {
        const { usersCollection } = await import('./db.js');
        await usersCollection.updateOne({ userId: ctx.from.id }, { $set: { preferredLang: newLang } }, { upsert: true });
        await ctx.reply(newLang === 'en' ? 'Language switched to English.' : 'Я переключился на русский.', getMainMenuSync(ctx.from.id, newLang));
        const inline = await buildSettingsInlineForUser(ctx.from.id);
        try { await ctx.editMessageReplyMarkup(inline); } catch {}
      } catch (e) {}
      await ctx.answerCbQuery();
      return;
    }

    // confirmation for clearing all old alerts
    if (data === 'clear_old_alerts_confirm') {
      const isEn = String(lang).split('-')[0] === 'en';
      const text = isEn ? 'Are you sure?' : 'Вы уверены?';
      const inline = { inline_keyboard: [[
          { text: isEn ? 'Yes' : 'Да', callback_data: 'clear_old_alerts_yes' },
          { text: isEn ? 'No' : 'Нет', callback_data: 'clear_old_alerts_no' }
        ]]};
      try { await ctx.editMessageText(text, { reply_markup: inline }); } catch (e) { try { await ctx.reply(text, { reply_markup: inline }); } catch {} }
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'clear_old_alerts_no') {
      try { await ctx.editMessageReplyMarkup({ inline_keyboard: [] }); } catch (e) {}
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'clear_old_alerts_yes') {
      const isEn = String(lang).split('-')[0] === 'en';
      try {
        const alertsMod = await import('./alerts.js');
        const res = await alertsMod.clearUserOldAlerts(ctx.from.id, { forceAll: true });
        const deleted = res?.deletedCount || 0;
        const msg = deleted ? (isEn ? `Deleted ${deleted} items.` : `Удалено ${deleted} записей.`) : (isEn ? 'No old alerts to delete.' : 'Нет старых алертов для удаления.');
        try { await ctx.editMessageText(msg, { reply_markup: { inline_keyboard: [] } }); } catch (e) { try { await ctx.reply(msg); } catch {} }
      } catch (e) {
        try { await ctx.answerCbQuery('Error'); } catch {}
      }
      await ctx.answerCbQuery();
      return;
    }

    await ctx.answerCbQuery();
  } catch (e) {
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

bot.on('text', async (ctx) => {
  try {
    const step = ctx.session.step;
    const textRaw = (ctx.message.text || '').trim();
    const text = textRaw;

    const daysMap = {
      '7 дней': 7, '30 дней': 30, '90 дней': 90,
      '7 days': 7, '30 days': 30, '90 days': 90
    };
    const numeric = parseInt(text.replace(/\D/g, ''), 10);
    const isNumericDay = Number.isFinite(numeric) && [7, 30, 90].includes(numeric);
    const normalized = text.toLowerCase();
    const isDaysPhrase = daysMap[text] || daysMap[normalized] || isNumericDay || /^\d+\s*дн/i.test(text) || /^\d+\s*day/i.test(text);
    if (( !step || step === 'old_alerts_select_days' ) && isDaysPhrase) {
      const days = daysMap[text] || daysMap[normalized] || (isNumericDay ? numeric : 30);
      const token = `d${days}_q`;
      const lang = await resolveUserLang(ctx.from.id);
      const { pages } = await renderOldAlertsList(ctx.from.id, { days, symbol: null, token, lang });
      const first = pages[0];
      ctx.session = {};
      if (first.buttons && first.buttons.length) {
        await ctx.reply(first.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons } });
      } else {
        await ctx.reply(first.text, getMainMenuSync(ctx.from.id, lang));
      }
      return;
    }

    if (!step && /^[A-Z0-9]{2,10}$/i.test(text)) ctx.session = { step: 'symbol' };
    if (!ctx.session.step) return;

    if (ctx.session.step === 'symbol') {
      const base = text.toUpperCase();
      const symbol = `${base}-USDT`;
      const price = await getCachedPrice(symbol);
      if (Number.isFinite(price)) {
        try { await pushRecentSymbol(ctx.from.id, base); } catch (e) {}
        ctx.session.symbol = symbol;
        ctx.session.step = 'alert_condition';
        const lang = await resolveUserLang(ctx.from.id);
        await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? `✅ Coin: *${symbol}* Current price: *${fmtNum(price)}* Choose direction:` : `✅ Монета: *${symbol}* Текущая цена: *${fmtNum(price)}* Выбери направление:`, {
          parse_mode: 'Markdown',
          reply_markup: buildDirectionKeyboard(lang)
        });
      } else {
        await ctx.reply('Пара не найдена на KuCoin. Попробуй другой символ.');
        ctx.session = {};
      }
      return;
    }

    if (ctx.session.step === 'alert_condition') {
      const lang = await resolveUserLang(ctx.from.id);
      if (text === '⬆️ Когда выше' || text === '⬆️ When above') ctx.session.alertCondition = '>';
      else if (text === '⬇️ Когда ниже' || text === '⬇️ When below') ctx.session.alertCondition = '<';
      else { await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? 'Choose ⬆️ or ⬇️' : 'Выбери ⬆️ или ⬇️'); return; }
      ctx.session.step = 'alert_price';
      await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? 'Enter alert price:' : 'Введи цену уведомления:', { reply_markup: { keyboard: [[buildCancelButton(lang)]], resize_keyboard:true } });
      return;
    }

    if (ctx.session.step === 'alert_price') {
      const v = parseFloat(text);
      if (!Number.isFinite(v)) { await ctx.reply('Введите корректное число'); return; }
      ctx.session.alertPrice = v;
      ctx.session.step = 'ask_sl';
      const lang = await resolveUserLang(ctx.from.id);
      const hint = ctx.session.alertCondition === '>' ? (lang && String(lang).split('-')[0] === 'en' ? 'SL will be higher (for short — reverse)' : 'SL будет выше (для шорта — логика обратная)') : (lang && String(lang).split('-')[0] === 'en' ? 'SL will be lower' : 'SL будет ниже');
      await ctx.reply((lang && String(lang).split('-')[0] === 'en' ? 'Add stop-loss?' : 'Добавить стоп-лосс?') + ` ${hint}`, { reply_markup: buildAskSlKeyboard(lang) });
      return;
    }

    if (ctx.session.step === 'ask_sl') {
      const { alertsCollection } = await import('./db.js');
      const limit = await getUserAlertLimit(ctx.from.id).catch(()=>1000000000);
      let currentCount = 0;
      try {
        currentCount = await alertsCollection.countDocuments({ userId: ctx.from.id });
      } catch (e) {
        const currentAlerts = await getUserAlertsCached(ctx.from.id).catch(()=>[]);
        currentCount = (currentAlerts?.length || 0);
      }

      if (currentCount >= limit) {
        const lang = await resolveUserLang(ctx.from.id);
        await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? `You already have ${currentCount} alerts — limit ${limit}. Contact @pirial_gena to increase.` : `У тебя уже ${currentCount} алертов — достигнут лимит ${limit}. Если нужно увеличить лимит, напиши мне: @pirial_gena`, getMainMenuSync(ctx.from.id, lang));
        ctx.session = {};
        return;
      }

      const lang = await resolveUserLang(ctx.from.id);
      if (text === (lang && String(lang).split('-')[0] === 'en' ? '⏭️ Skip SL' : '⏭️ Без SL')) {
        try {
          const { alertsCollection: ac } = await import('./db.js');
          const beforeInsertCount = await ac.countDocuments({ userId: ctx.from.id }).catch(()=>currentCount);
          if (beforeInsertCount >= limit) {
            await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? `You already have ${beforeInsertCount} alerts — limit ${limit}.` : `У тебя уже ${beforeInsertCount} алертов — достигнут лимит ${limit}. Если нужно увеличить лимит, напиши мне: @pirial_gena`, getMainMenuSync(ctx.from.id, lang));
            ctx.session = {};
            return;
          }

          await ac.insertOne({ userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: ctx.session.alertPrice, type: 'alert', createdAt: new Date() });
          invalidateUserAlertsCache(ctx.from.id);
          const cp = await getCachedPrice(ctx.session.symbol);
          const isEn = String(lang).split('-')[0] === 'en';
          const currentBold = `*${fmtNum(cp) ?? '—'}*`;
          const conditionLine = ctx.session.alertCondition === '>' ? (isEn ? '⬆️ when above' : '⬆️ выше') : (isEn ? '⬇️ when below' : '⬇️ ниже');
          const msg = isEn
            ? `✅ Alert created:\n🔔 ${ctx.session.symbol}\n${conditionLine} ${fmtNum(ctx.session.alertPrice)}\nCurrent: ${currentBold}`
            : `✅ Алерт создан:\n🔔 ${ctx.session.symbol}\n${conditionLine} ${fmtNum(ctx.session.alertPrice)}\nТекущая: ${currentBold}`;
          await ctx.reply(msg, { parse_mode: 'Markdown', ...getMainMenuSync(ctx.from.id, lang) });
        } catch (e) { await ctx.reply('Ошибка при создании алерта'); }
        ctx.session = {};
        return;
      }
      if (text === (lang && String(lang).split('-')[0] === 'en' ? '🛑 Add SL' : '🛑 Добавить SL')) {
        ctx.session.step = 'sl_price';
        await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? 'Enter stop-loss price:' : 'Введи цену стоп-лосса:', { reply_markup: { keyboard: [[buildCancelButton(lang)]], resize_keyboard:true } });
        return;
      }
      await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? 'Choose: 🛑 Add SL / ⏭️ Skip SL' : 'Выбери опцию: 🛑 Добавить SL / ⏭️ Без SL');
      return;
    }

    if (ctx.session.step === 'sl_price') {
      const sl = parseFloat(text);
      if (!Number.isFinite(sl)) { await ctx.reply('Введите корректное число SL'); return; }

      const { alertsCollection } = await import('./db.js');
      const limit = await getUserAlertLimit(ctx.from.id).catch(()=>1000000000);
      let currentCount = 0;
      try {
        currentCount = await alertsCollection.countDocuments({ userId: ctx.from.id });
      } catch (e) {
        const currentAlerts = await getUserAlertsCached(ctx.from.id).catch(()=>[]);
        currentCount = (currentAlerts?.length || 0);
      }

      if (currentCount + 2 > limit) {
        const lang = await resolveUserLang(ctx.from.id);
        await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? `Can't create pair (alert + SL). You have ${currentCount} alerts, limit ${limit}.` : `Нельзя создать связку (уведомление + SL). У тебя сейчас ${currentCount} алертов, лимит ${limit}. Чтобы увеличить лимит напиши: @pirial_gena`, getMainMenuSync(ctx.from.id, lang));
        ctx.session = {};
        return;
      }

      try {
        const groupId = new ObjectId().toString();
        const beforeInsertCount = await alertsCollection.countDocuments({ userId: ctx.from.id }).catch(()=>currentCount);
        if (beforeInsertCount + 2 > limit) {
          const lang = await resolveUserLang(ctx.from.id);
          await ctx.reply(lang && String(lang).split('-')[0] === 'en' ? `Can't create pair (alert + SL). You have ${beforeInsertCount} alerts, limit ${limit}.` : `Нельзя создать связку (уведомление + SL). У тебя сейчас ${beforeInsertCount} алертов, лимит ${limit}. Чтобы увеличить лимит напиши: @pirial_gena`, getMainMenuSync(ctx.from.id, lang));
          ctx.session = {};
          return;
        }

        const slDir = ctx.session.alertCondition === '<' ? (await resolveUserLang(ctx.from.id)) === 'en' ? 'lower' : 'ниже' : (await resolveUserLang(ctx.from.id)) === 'en' ? 'higher' : 'выше';
        const { alertsCollection: ac } = await import('./db.js');
        await ac.insertMany([
          { userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: ctx.session.alertPrice, type: 'alert', groupId, createdAt: new Date() },
          { userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: sl, type: 'sl', slDir, groupId, createdAt: new Date() }
        ]);
        invalidateUserAlertsCache(ctx.from.id);
        const cp = await getCachedPrice(ctx.session.symbol);
        const lang = await resolveUserLang(ctx.from.id);
        const isEn = String(lang).split('-')[0] === 'en';
        const currentBold = `*${fmtNum(cp) ?? '—'}*`;
        const conditionLine = ctx.session.alertCondition === '>' ? (isEn ? '⬆️ when above' : '⬆️ выше') : (isEn ? '⬇️ when below' : '⬇️ ниже');
        const slLine = isEn ? `🛑 SL (${slDir}) ${fmtNum(sl)}` : `🛑 SL (${slDir}) ${fmtNum(sl)}`;
        const msg = isEn
          ? `✅ Pair created:\n🔔 ${ctx.session.symbol}\n${conditionLine} ${fmtNum(ctx.session.alertPrice)}\n🛑 SL (${slDir}) ${fmtNum(sl)}\nCurrent: ${currentBold}`
          : `✅ Создана связка:\n🔔 ${ctx.session.symbol}\n${conditionLine} ${fmtNum(ctx.session.alertPrice)}\n🛑 SL (${slDir}) ${fmtNum(sl)}\nТекущая: ${currentBold}`;
        await ctx.reply(msg, { parse_mode: 'Markdown', ...getMainMenuSync(ctx.from.id, lang) });
      } catch (e) { await ctx.reply('Ошибка при создании связки'); }
      ctx.session = {};
      return;
    }

    if (ctx.session.step === 'old_alerts_select_days') {
      if (text === '↩️ Отмена' || text === '↩️ Cancel') { ctx.session = {}; const lang = await resolveUserLang(ctx.from.id); await ctx.reply('Отмена', getMainMenuSync(ctx.from.id, lang)); return; }
      const daysMapLocal = { '7 дней': 7, '30 дней': 30, '90 дней': 90, '7 days': 7, '30 days': 30, '90 days': 90 };
      const days = daysMapLocal[text] || parseInt(text, 10) || 30;
      const token = `d${days}_q`;
      const lang = await resolveUserLang(ctx.from.id);
      const { pages } = await renderOldAlertsList(ctx.from.id, { days, symbol: null, token, lang });
      const first = pages[0];
      ctx.session = {};
      if (first.buttons && first.buttons.length) {
        await ctx.reply(first.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons } });
      } else {
        await ctx.reply(first.text, getMainMenuSync(ctx.from.id, lang));
      }
      return;
    }

    if (ctx.session.step === 'old_alerts_search') {
      if (text === '↩️ Отмена' || text === '↩️ Cancel') { ctx.session = {}; const lang = await resolveUserLang(ctx.from.id); await ctx.reply('Отмена', getMainMenuSync(ctx.from.id, lang)); return; }
      const parts = text.split(/\s+/).filter(Boolean);
      const symbol = parts[0] || null;
      const days = parts[1] ? Math.max(1, parseInt(parts[1], 10)) : 30;
      const token = `d${days}_q${encodeURIComponent(String(symbol || ''))}`;
      const lang = await resolveUserLang(ctx.from.id);
      const { pages } = await renderOldAlertsList(ctx.from.id, { days, symbol, token, lang });
      const first = pages[0];
      ctx.session = {};
      if (first.buttons && first.buttons.length) {
        await ctx.reply(first.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons } });
      } else {
        await ctx.reply(first.text, getMainMenuSync(ctx.from.id, lang));
      }
      return;
    }

  } catch (e) {
    await ctx.reply('Произошла ошибка, попробуй ещё раз.');
    ctx.session = {};
  }
});

bot.command('set_alert_limit', async (ctx) => {
  try {
    if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) { return ctx.reply('У вас нет доступа к этой команде.'); }
    const parts = (ctx.message.text || '').trim().split(/\s+/).slice(1);
    if (parts.length < 2) return ctx.reply('Использование: /set_alert_limit <userId|@username> <limit>');
    const ident = parts[0];
    const lim = parseInt(parts[1], 10);
    if (!Number.isFinite(lim) || lim < 0) return ctx.reply('Лимит должен быть неотрицательным числом.');

    let targetId = parseInt(ident, 10);
    if (!Number.isFinite(targetId)) {
      let name = ident;
      if (!name.startsWith('@')) name = `@${name}`;
      try {
        const chat = await bot.telegram.getChat(name);
        targetId = chat.id;
      } catch (e) {
        return ctx.reply('Не удалось найти пользователя по идентификатору/имени.');
      }
    }

    const newLim = await setUserAlertLimit(targetId, lim);
    if (newLim === null) return ctx.reply('Ошибка при установке лимита.');
    await ctx.reply(`Лимит для пользователя ${targetId} установлен: ${newLim}`);
    try { await bot.telegram.sendMessage(targetId, `Тебе установлен лимит алертов: ${newLim} (вручную от администратора)`); } catch {}
  } catch (e) {
    await ctx.reply('Ошибка при выполнении команды');
  }
});

bot.command('get_alert_limit', async (ctx) => {
  try {
    if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) { return ctx.reply('У вас нет доступа к этой команде.'); }
    const parts = (ctx.message.text || '').trim().split(/\s+/).slice(1);
    if (parts.length < 1) return ctx.reply('Использование: /get_alert_limit <userId|@username>');
    const ident = parts[0];
    let targetId = parseInt(ident, 10);
    if (!Number.isFinite(targetId)) {
      let name = ident;
      if (!name.startsWith('@')) name = `@${name}`;
      try {
        const chat = await bot.telegram.getChat(name);
        targetId = chat.id;
      } catch (e) {
        return ctx.reply('Не удалось найти пользователя по идентификатору/имени.');
      }
    }
    const lim = await getUserAlertLimit(targetId);
    await ctx.reply(`Лимит для пользователя ${targetId}: ${lim}`);
  } catch (e) {
    await ctx.reply('Ошибка при выполнении команды');
  }
});

bot.command('refresh_daily', async (ctx) => {
  try {
    if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) return ctx.reply('У вас нет доступа.');

    const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Kyiv' });
    await ctx.reply(`⏳ Начинаю принудительное обновление мотивации на ${dateStr}...`);

    try {
      const cacheMod = await import('./cache.js');
      if (cacheMod && cacheMod.dailyCache) {
        cacheMod.dailyCache.date = null;
        cacheMod.dailyCache.doc = null;
        cacheMod.dailyCache.imageBuffer = null;
        await ctx.reply('Кэш dailyCache очищен.');
      }
    } catch (e) {
      await ctx.reply('⚠️ Не удалось очистить кэш в памяти (см логи). Продолжаю.');
    }

    const daily = await import('./daily.js');
    const { dailyMotivationCollection } = await import('./db.js');

    let previewQuote = null;
    try {
      previewQuote = await daily.fetchQuoteFromAny();
      if (previewQuote && previewQuote.text) {
        await ctx.reply(`Превью цитаты:\n${previewQuote.text}${previewQuote.author ? `\n— ${previewQuote.author}` : ''}`);
      } else {
        await ctx.reply('Превью цитаты: не удалось загрузить новую цитату (источники вернули пусто).');
      }
    } catch (e) {
      await ctx.reply(`Ошибка при получении превью цитаты: ${String(e?.message || e)}`);
    }

    let previewImgInfo = null;
    try {
      if (typeof daily.fetchRandomImage === 'function') {
        const img = await daily.fetchRandomImage();
        if (img && img.url) {
          previewImgInfo = img;
          await ctx.reply(`Превью картинки: ${img.url} (${img.source || 'unknown'})`);
        } else {
          await ctx.reply('Превью картинки: не удалось получить картинку из источников.');
        }
      } else {
        await ctx.reply('Превью картинки: функция fetchRandomImage недоступна.');
      }
    } catch (e) {
      await ctx.reply(`Ошибка при получении превью картинки: ${String(e?.message || e)}`);
    }

    let stored = null;
    try {
      stored = await daily.fetchAndStoreDailyMotivation(dateStr, { force: true });
      if (stored) {
        await ctx.reply('✅ Цитата и метаданные сохранены в БД (force).');
      } else {
        await ctx.reply('⚠️ fetchAndStoreDailyMotivation вернул null/undefined (в БД мог остаться старый документ).');
      }
    } catch (e) {
      await ctx.reply(`Ошибка при сохранении мотивации: ${String(e?.message || e)}`);
    }

    try {
      const doc = await dailyMotivationCollection.findOne({ date: dateStr });
      if (doc) {
        const q = doc.quote?.original || (doc.quote?.translations && doc.quote.translations.ru) || null;
        await ctx.reply(`Текущий документ в БД:\nЦитата: ${q ? q : '—'}\nАвтор: ${doc.quote?.author || '—'}\nImage URL: ${doc.image?.url || '—'}`);
      } else {
        await ctx.reply('В БД нет документа для сегодняшней даты после сохранения.');
      }
    } catch (e) {
      await ctx.reply(`Ошибка при чтении doc из БД: ${String(e?.message || e)}`);
    }

    try {
      const buf = await daily.ensureDailyImageBuffer(dateStr);
      if (buf && buf.length) {
        await ctx.reply(`Картинка загружена в память, размер ${buf.length} байт.`);
      } else {
        await ctx.reply('Картинка не загружена (будет отправлён текст без изображения).');
      }
    } catch (e) {
      await ctx.reply(`Ошибка при загрузке изображения: ${String(e?.message || e)}`);
    }

    try {
      const ok = await daily.sendDailyToUser(bot, ctx.from.id, dateStr, { disableNotification: false, forceRefresh: true });
      if (ok) await ctx.reply('Готово — мотивация обновлена и отправлена тебе.');
      else await ctx.reply('Мотивация сохранена, но отправка не удалась (см логи).');
    } catch (e) {
      await ctx.reply(`Ошибка при отправке мотивации: ${String(e?.message || e)}`);
    }

  } catch (e) {
    await ctx.reply('Внутренняя ошибка: ' + String(e?.message || e));
  }
});

export async function startBot() {
  await connectToMongo();
  startTickersRefresher();
  startAlertsChecker(bot);
  await removeInactive();
  const app = createServer();
  const PORT = process.env.PORT || 3000;
  const server = app.listen(PORT, () => console.log(`HTTP server on ${PORT}`));
  setInterval(() => processDailyQuoteRetry(bot), 60_000);
  setInterval(() => watchForNewQuotes(bot), 30_000);

  const dateStrNow = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Kyiv' });
  try { await fetchAndStoreDailyMotivation(dateStrNow).catch(()=>{}); } catch (e) {}

  let lastFetchDay = null;
  let lastPrepareDay = null;

  setInterval(async () => {
    try {
      const kyivNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'Europe/Kyiv' }));
      const day = kyivNow.toLocaleDateString('sv-SE');
      const hour = kyivNow.getHours();

      if (day !== lastFetchDay && hour === IMAGE_FETCH_HOUR) {
        try { await fetchAndStoreDailyMotivation(day, { force: true }); } catch (e) {}
        lastFetchDay = day;
      }

      if (day !== lastPrepareDay && hour === PREPARE_SEND_HOUR) {
        try { await fetchAndStoreDailyMotivation(day, { force: false }); } catch (e) {}
        lastPrepareDay = day;

        try {
          const dateStr = day;
          const { usersCollection, pendingDailySendsCollection } = await import('./db.js');
          const already = await pendingDailySendsCollection.find({ date: dateStr, sent: true }, { projection: { userId: 1 } }).toArray();
          const sentSet = new Set((already || []).map(r => r.userId));
          const cursor = usersCollection.find({}, { projection: { userId: 1 } });
          const BATCH = 20;
          let batch = [];
          while (await cursor.hasNext()) {
            const u = await cursor.next();
            if (!u || !u.userId) continue;
            const uid = u.userId;
            if (sentSet.has(uid)) continue;
            batch.push(uid);
            if (batch.length >= BATCH) {
              await Promise.all(batch.map(async (targetId) => {
                try {
                  const ok = await sendDailyToUser(bot, targetId, dateStr, { disableNotification: false, forceRefresh: false }).catch(()=>false);
                  await pendingDailySendsCollection.updateOne({ userId: targetId, date: dateStr }, { $set: { sent: !!ok, sentAt: ok ? new Date() : null, quoteSent: !!ok, permanentFail: !ok } }, { upsert: true });
                } catch (e) {}
              }));
              batch = [];
            }
          }
          if (batch.length) {
            await Promise.all(batch.map(async (targetId) => {
              try {
                const ok = await sendDailyToUser(bot, targetId, dateStr, { disableNotification: false, forceRefresh: false }).catch(()=>false);
                await pendingDailySendsCollection.updateOne({ userId: targetId, date: dateStr }, { $set: { sent: !!ok, sentAt: ok ? new Date() : null, quoteSent: !!ok, permanentFail: !ok } }, { upsert: true });
              } catch (e) {}
            }));
          }
        } catch (e) {}
      }
    } catch (e) {}
  }, 60_000);

  setInterval(async () => {
    try {
      await removeInactive();
    } catch (e) {}
  }, 7 * DAY_MS);

  await bot.launch();
  return { server };
}
