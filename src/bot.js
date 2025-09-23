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
  setUserAlertLimit
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

function getMainMenu(userId) {
  const keyboard = [
    [{ text: '➕ Создать алерт' }, { text: '📋 Мои алерты' }],
    [{ text: '⚙️ Настройки' }],
    [{ text: '📜 Старые алерты' }, { text: '🔎 Поиск старых алертов' }]
  ];
  if (CREATOR_ID && String(userId) === String(CREATOR_ID)) {
    keyboard.push([{ text: '🌅 Прислать мотивацию' }]);
    keyboard.push([{ text: '👥 Количество активных пользователей' }]);
  }
  return { reply_markup: { keyboard, resize_keyboard: true } };
}

bot.start(ctx => { ctx.session = {}; ctx.reply('Привет! Я бот-алерт для крипты.', getMainMenu(ctx.from?.id)); });

bot.hears('⚙️ Настройки', async (ctx) => {
  try {
    const uid = ctx.from.id;
    const order = await getUserAlertsOrder(uid).catch(()=> 'new_bottom');
    const isTop = order === 'new_top';
    const inline = {
      inline_keyboard: [
        [
          { text: isTop ? '✅ Новые сверху' : 'Новые сверху', callback_data: 'set_order_new_top' },
          { text: !isTop ? '✅ Новые снизу' : 'Новые снизу', callback_data: 'set_order_new_bottom' }
        ],
        [{ text: '↩️ Назад', callback_data: 'back_to_main' }]
      ]
    };
    await ctx.reply('Настройки отображения алертов:', { reply_markup: inline });
  } catch (e) {
    console.error(e);
    try { await ctx.reply('Ошибка при открытии настроек'); } catch {}
  }
});

bot.hears('Новые сверху', async (ctx) => {
  try {
    await setUserAlertsOrder(ctx.from.id, 'new_top');
    await ctx.reply('Порядок установлен: новые сверху', getMainMenu(ctx.from.id));
  } catch (e) {
    console.error(e);
    try { await ctx.reply('Ошибка при установке порядка'); } catch {}
  }
});

bot.hears('Новые снизу (по умолчанию)', async (ctx) => {
  try {
    await setUserAlertsOrder(ctx.from.id, 'new_bottom');
    await ctx.reply('Порядок установлен: новые снизу', getMainMenu(ctx.from.id));
  } catch (e) {
    console.error(e);
    try { await ctx.reply('Ошибка при установке порядка'); } catch {}
  }
});

bot.hears('➕ Создать алерт', async (ctx) => {
  // Сразу проверяем лимит перед началом процесса создания алерта
  try {
    const { alertsCollection } = await import('./db.js');
    const limit = await getUserAlertLimit(ctx.from.id).catch(()=>10);
    let currentCount = 0;
    try {
      currentCount = await alertsCollection.countDocuments({ userId: ctx.from.id });
    } catch (e) {
      console.warn('countDocuments failed during create-start check', e?.message || e);
      const recent = await getUserAlertsCached(ctx.from.id).catch(()=>[]);
      currentCount = (recent?.length || 0);
    }
    if (currentCount >= limit) {
      await ctx.reply(`У тебя уже ${currentCount} алертов — достигнут лимит ${limit}. Если нужно увеличить лимит, напиши мне: @pirial_gena`, getMainMenu(ctx.from.id));
      return;
    }
  } catch (e) {
    console.warn('pre-create limit check failed, allowing create flow', e?.message || e);
    // если проверка упала — не мешаем пользователю, продолжаем
  }

  ctx.session = { step: 'symbol' };
  refreshAllTickers().catch(()=>{});
  const recent = await getUserRecentSymbols(ctx.from.id);
  const suggest = [...new Set([...recent, ...['BTC','ETH','SOL','BNB','XRP','DOGE']])].slice(0,6).map(s=>({ text: s }));
  const kb = suggest.length ? [suggest, [{ text: '↩️ Отмена' }]] : [[{ text: '↩️ Отмена' }]];
  ctx.reply('Введи символ (например BTC) или нажми кнопку:', { reply_markup: { keyboard: kb, resize_keyboard: true } });
});

bot.hears('↩️ Отмена', ctx => { ctx.session = {}; ctx.reply('Отмена ✅', getMainMenu(ctx.from.id)); });

bot.hears('📋 Мои алерты', async (ctx) => {
  try {
    try { await bot.telegram.sendChatAction(ctx.chat.id, 'typing'); } catch (_){ }
  } catch (e) { console.error(e); }
  try {
    const { pages } = await renderAlertsList(ctx.from.id, { fast: false });
    const first = pages[0];
    await ctx.reply(first.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons } });
  } catch (e) {
    console.error(e);
    ctx.reply('Ошибка при получении алертов.');
  }
});

bot.hears('📜 Старые алерты', async (ctx) => {
  // Start flow to select days; we'll hide keyboard after a selection (handled in text handler)
  ctx.session = { step: 'old_alerts_select_days' };
  const kb = [[{ text: '7 дней' }, { text: '30 дней' }, { text: '90 дней' }], [{ text: '↩️ Отмена' }]];
  await ctx.reply('Выбери период для просмотра старых алертов:', { reply_markup: { keyboard: kb, resize_keyboard: true } });
});

bot.hears('🔎 Поиск старых алертов', async (ctx) => {
  ctx.session = { step: 'old_alerts_search' };
  await ctx.reply('Введи запрос в формате: SYMBOL [DAYS]\nПримеры: "BTC", "BTC 30". По умолчанию DAYS=30.', { reply_markup: { keyboard: [[{ text: '↩️ Отмена' }]], resize_keyboard: true } });
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
    console.error(e);
    try { await ctx.reply('Ошибка при отправке мотивации'); } catch {}
  }
});

bot.hears('👥 Количество активных пользователей', async (ctx) => {
  try {
    if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) { return ctx.reply('У вас нет доступа к этой команде.'); }

    const now = Date.now();
    if (statsCache.count !== null && (now - statsCache.time) < CACHE_TTL) { return ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${statsCache.count}`); }

    const cutoff = new Date(Date.now() - INACTIVE_DAYS * DAY_MS);
    let activeCount;
    try {
      // exclude users that we flagged as botBlocked
      activeCount = await countDocumentsWithTimeout('users', { lastActive: { $gte: cutoff }, $or: [{ botBlocked: { $exists: false } }, { botBlocked: false }] }, 7000);
    }
    catch (err) {
      console.error('Ошибка/таймаут при подсчёте активных пользователей:', err);
      return ctx.reply('Ошибка получения статистики (таймаут или проблема с БД). Попробуйте позже.');
    }

    statsCache.count = activeCount;
    statsCache.time = now;
    await ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${activeCount}`);
  } catch (e) { console.error('stats handler error', e); try { await ctx.reply('Ошибка получения статистики.'); } catch {} }
});

bot.on('callback_query', async (ctx) => {
  try {
    const data = ctx.callbackQuery?.data;
    if (!data) return ctx.answerCbQuery();

    const mPage = data.match(/^alerts_page_(\d+)_view$/);
    if (mPage) {
      const pageIdx = parseInt(mPage[1], 10);
      const { pages } = await renderAlertsList(ctx.from.id, { fast: true });
      const page = pages[Math.max(0, Math.min(pageIdx, pages.length - 1))] || pages[0];
      try {
        await ctx.editMessageText(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } });
      } catch (e) {
        try { await ctx.reply(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } }); } catch (e2) { console.error('alerts_page edit/reply failed', e2); }
      }
      await ctx.answerCbQuery();
      return;
    }

    const mShow = data.match(/^show_delete_menu_(all|\\d+)$/);
    if (mShow) {
      const token = mShow[1];
      const { pages } = await renderAlertsList(ctx.from.id, { fast: true });
      const totalPages = pages.length;
      let sourcePage = null;
      if (token !== 'all') sourcePage = Math.max(0, Math.min(parseInt(token, 10), totalPages - 1));
      const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage, totalPages: (sourcePage === null ? null : totalPages) });
      try { await ctx.editMessageReplyMarkup({ inline_keyboard: inline }); }
      catch (err) {
        try { const originalText = ctx.update.callback_query.message?.text || 'Твои алерты'; await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } }); } catch (e) { console.error('show_delete_menu reply failed', e); }
      }
      await ctx.answerCbQuery();
      return;
    }

    const mBack = data.match(/^back_to_alerts(?:_p(\d+))?$/);
    if (mBack) {
      const p = mBack[1] ? parseInt(mBack[1], 10) : 0;
      const { pages } = await renderAlertsList(ctx.from.id, { fast: true });
      const page = pages[Math.max(0, Math.min(p, pages.length - 1))] || pages[0];
      try { await ctx.editMessageText(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } }); }
      catch (e) { try { await ctx.reply(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } }); } catch (e2) { console.error('back_to_alerts failed', e2); } }
      await ctx.answerCbQuery();
      return;
    }

    const mSet = data.match(/^set_order_(new_top|new_bottom)$/);
    if (mSet) {
      const order = mSet[1];
      await setUserAlertsOrder(ctx.from.id, order).catch(()=>{});
      const orderNow = await getUserAlertsOrder(ctx.from.id).catch(()=> 'new_bottom');
      const isTop = orderNow === 'new_top';
      const inline = {
        inline_keyboard: [
          [
            { text: isTop ? '✅ Новые сверху' : 'Новые сверху', callback_data: 'set_order_new_top' },
            { text: !isTop ? '✅ Новые снизу' : 'Новые снизу', callback_data: 'set_order_new_bottom' }
          ],
          [{ text: '↩️ Назад', callback_data: 'back_to_main' }]
        ]
      };
      try {
        await ctx.editMessageReplyMarkup(inline);
      } catch (e) {
        try { await ctx.reply('Порядок установлен', { reply_markup: inline }); } catch (e2) { console.error('set_order fallback failed', e2); }
      }
      await ctx.answerCbQuery('Порядок установлен');
      return;
    }

    if (data === 'back_to_main') {
      try { await ctx.reply('Возврат в меню', getMainMenu(ctx.from.id)); } catch (e) { console.error('back_to_main failed', e); }
      await ctx.answerCbQuery();
      return;
    }

    const m = data.match(/^del_([0-9a-fA-F]{24})_p(all|\d+)$/);
    const mLegacy = !m && data.startsWith('del_') ? data.match(/^del_([0-9a-fA-F]{24})$/) : null;

    if (m || mLegacy) {
      const id = (m ? m[1] : mLegacy[1]);
      const token = m ? m[2] : null;
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

      // archive before deleting
      try {
        const { alertsArchiveCollection } = await import('./db.js');
        await alertsArchiveCollection.insertOne({
          ...doc,
          deletedAt: new Date(),
          deleteReason: 'user_deleted',
          archivedAt: new Date()
        });
      } catch (e) { console.warn('archive insert failed on user delete', e?.message || e); }

      const { alertsCollection: ac } = await import('./db.js');
      await ac.deleteOne({ _id: new ObjectId(id) });
      invalidateUserAlertsCache(ctx.from.id);

      const alertsAfter = await getUserAlertsCached(ctx.from.id);
      const computedTotalPages = Math.max(1, Math.ceil((alertsAfter?.length || 0) / 20));
      if (sourcePage !== null) { sourcePage = Math.max(0, Math.min(sourcePage, computedTotalPages - 1)); }

      const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage, totalPages: (sourcePage === null ? null : computedTotalPages) });

      if (!inline || inline.length === 0) {
        try { await ctx.editMessageText('У тебя больше нет активных алертов.', { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [] } }); } catch {}
        await ctx.answerCbQuery('Алерт удалён');
        return;
      }

      try { await ctx.editMessageReplyMarkup({ inline_keyboard: inline }); } catch (err) {
        try {
          const originalText = ctx.update.callback_query.message?.text || 'Твои алерты';
          await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } });
        } catch (e) { console.error('delete fallback failed', e); }
      }

      await ctx.answerCbQuery('Алерт удалён');
      return;
    }

    const mOldPage = data.match(/^old_alerts_page_(\d+)_view_(d(\d+)_q(.+))$/);
    if (mOldPage) {
      const pageIdx = parseInt(mOldPage[1], 10);
      const token = mOldPage[2];
      const mToken = token.match(/^d(\d+)_q(.*)$/);
      const days = mToken ? parseInt(mToken[1], 10) : 30;
      const q = mToken ? decodeURIComponent(mToken[2]) : '';
      const opts = { days, symbol: q || null, token };
      const { pages } = await renderOldAlertsList(ctx.from.id, opts);
      const page = pages[Math.max(0, Math.min(pageIdx, pages.length - 1))] || pages[0];
      try {
        await ctx.editMessageText(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } });
      } catch (e) {
        try { await ctx.reply(page.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: page.buttons } }); } catch (e2) { console.error('old_alerts_page edit/reply failed', e2); }
      }
      await ctx.answerCbQuery();
      return;
    }

    await ctx.answerCbQuery();
  } catch (e) {
    console.error(e);
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
});

bot.on('text', async (ctx) => {
  try {
    const step = ctx.session.step;
    const textRaw = (ctx.message.text || '').trim();
    const text = textRaw;

    // --- New: allow day-selection shortcuts even if session was cleared previously.
    // Only intercept when user is NOT in an active create-alert flow (to avoid breaking numeric price input).
    const daysMap = { '7 дней': 7, '30 дней': 30, '90 дней': 90 };
    const numeric = parseInt(text.replace(/\D/g, ''), 10);
    const isNumericDay = Number.isFinite(numeric) && [7, 30, 90].includes(numeric);
    const normalized = text.toLowerCase();
    const isDaysPhrase = daysMap[text] || daysMap[normalized] || isNumericDay || /^\d+\s*дн/i.test(text);
    if (( !step || step === 'old_alerts_select_days' ) && isDaysPhrase) {
      // handle as old alerts selection
      const days = daysMap[text] || daysMap[normalized] || (isNumericDay ? numeric : 30);
      const token = `d${days}_q`;
      const { pages } = await renderOldAlertsList(ctx.from.id, { days, symbol: null, token });
      const first = pages[0];
      // clear session and remove reply keyboard
      ctx.session = {};
      if (first.buttons && first.buttons.length) {
        await ctx.reply(first.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons, remove_keyboard: true } });
      } else {
        await ctx.reply(first.text, getMainMenu(ctx.from.id));
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
        await ctx.reply(`✅ Монета: *${symbol}* Текущая цена: *${fmtNum(price)}* Выбери направление:`, {
          parse_mode: 'Markdown',
          reply_markup: { keyboard: [[{ text: '⬆️ Когда выше' }, { text: '⬇️ Когда ниже' }], [{ text: '↩️ Отмена' }]], resize_keyboard:true }
        });
      } else {
        await ctx.reply('Пара не найдена на KuCoin. Попробуй другой символ.');
        ctx.session = {};
      }
      return;
    }

    if (ctx.session.step === 'alert_condition') {
      if (text === '⬆️ Когда выше') ctx.session.alertCondition = '>';
      else if (text === '⬇️ Когда ниже') ctx.session.alertCondition = '<';
      else { await ctx.reply('Выбери ⬆️ или ⬇️'); return; }
      ctx.session.step = 'alert_price';
      await ctx.reply('Введи цену уведомления:', { reply_markup: { keyboard: [[{ text: '↩️ Отмена' }]], resize_keyboard:true } });
      return;
    }

    if (ctx.session.step === 'alert_price') {
      const v = parseFloat(text);
      if (!Number.isFinite(v)) { await ctx.reply('Введите корректное число'); return; }
      ctx.session.alertPrice = v;
      ctx.session.step = 'ask_sl';
      const hint = ctx.session.alertCondition === '>' ? 'SL будет выше (для шорта — логика обратная)' : 'SL будет ниже';
      await ctx.reply(`Добавить стоп-лосс? ${hint}`, { reply_markup: { keyboard: [[{ text: '🛑 Добавить SL' }, { text: '⏭️ Без SL' }], [{ text: '↩️ Отмена' }]], resize_keyboard:true } });
      return;
    }

    if (ctx.session.step === 'ask_sl') {
      const { alertsCollection } = await import('./db.js');
      const limit = await getUserAlertLimit(ctx.from.id).catch(()=>10);
      let currentCount = 0;
      try {
        currentCount = await alertsCollection.countDocuments({ userId: ctx.from.id });
      } catch (e) {
        console.warn('countDocuments failed, falling back to cache count', e?.message || e);
        const currentAlerts = await getUserAlertsCached(ctx.from.id).catch(()=>[]);
        currentCount = (currentAlerts?.length || 0);
      }

      if (currentCount >= limit) {
        await ctx.reply(`У тебя уже ${currentCount} алертов — достигнут лимит ${limit}. Если нужно увеличить лимит, напиши мне: @pirial_gena`, getMainMenu(ctx.from.id));
        ctx.session = {};
        return;
      }

      if (text === '⏭️ Без SL') {
        try {
          const { alertsCollection: ac } = await import('./db.js');

          // ещё разная проверка прямо перед вставкой (чтобы минимизировать гонки)
          const beforeInsertCount = await ac.countDocuments({ userId: ctx.from.id }).catch(()=>currentCount);
          if (beforeInsertCount >= limit) {
            await ctx.reply(`У тебя уже ${beforeInsertCount} алертов — достигнут лимит ${limit}. Если нужно увеличить лимит, напиши мне: @pirial_gena`, getMainMenu(ctx.from.id));
            ctx.session = {};
            return;
          }

          await ac.insertOne({ userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: ctx.session.alertPrice, type: 'alert', createdAt: new Date() });
          invalidateUserAlertsCache(ctx.from.id);
          const cp = await getCachedPrice(ctx.session.symbol);
          await ctx.reply(`✅ Алерт создан: *${ctx.session.symbol}* ${ctx.session.alertCondition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${fmtNum(ctx.session.alertPrice)}* Текущая цена: *${fmtNum(cp) ?? '—'}*`, { parse_mode: 'Markdown', ...getMainMenu(ctx.from.id) });
        } catch (e) { console.error(e); await ctx.reply('Ошибка при создании алерта'); }
        ctx.session = {};
        return;
      }
      if (text === '🛑 Добавить SL') {
        ctx.session.step = 'sl_price';
        await ctx.reply('Введи цену стоп-лосса:', { reply_markup: { keyboard: [[{ text: '↩️ Отмена' }]], resize_keyboard:true } });
        return;
      }
      await ctx.reply('Выбери опцию: 🛑 Добавить SL / ⏭️ Без SL');
      return;
    }

    if (ctx.session.step === 'sl_price') {
      const sl = parseFloat(text);
      if (!Number.isFinite(sl)) { await ctx.reply('Введите корректное число SL'); return; }

      const { alertsCollection } = await import('./db.js');
      const limit = await getUserAlertLimit(ctx.from.id).catch(()=>10);
      let currentCount = 0;
      try {
        currentCount = await alertsCollection.countDocuments({ userId: ctx.from.id });
      } catch (e) {
        console.warn('countDocuments failed, falling back to cache count', e?.message || e);
        const currentAlerts = await getUserAlertsCached(ctx.from.id).catch(()=>[]);
        currentCount = (currentAlerts?.length || 0);
      }

      if (currentCount + 2 > limit) {
        await ctx.reply(`Нельзя создать связку (уведомление + SL). У тебя сейчас ${currentCount} алертов, лимит ${limit}. Чтобы увеличить лимит напиши: @pirial_gena`, getMainMenu(ctx.from.id));
        ctx.session = {};
        return;
      }

      try {
        const groupId = new ObjectId().toString();

        const beforeInsertCount = await alertsCollection.countDocuments({ userId: ctx.from.id }).catch(()=>currentCount);
        if (beforeInsertCount + 2 > limit) {
          await ctx.reply(`Нельзя создать связку (уведомление + SL). У тебя сейчас ${beforeInsertCount} алертов, лимит ${limit}. Чтобы увеличить лимит напиши: @pirial_gena`, getMainMenu(ctx.from.id));
          ctx.session = {};
          return;
        }

        const slDir = ctx.session.alertCondition === '<' ? 'ниже' : 'выше';
        const { alertsCollection: ac } = await import('./db.js');
        await ac.insertMany([
          { userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: ctx.session.alertPrice, type: 'alert', groupId, createdAt: new Date() },
          { userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: sl, type: 'sl', slDir, groupId, createdAt: new Date() }
        ]);
        invalidateUserAlertsCache(ctx.from.id);
        const cp = await getCachedPrice(ctx.session.symbol);
        await ctx.reply(`✅ Создана связка: 🔔 *${ctx.session.symbol}* ${ctx.session.alertCondition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${fmtNum(ctx.session.alertPrice)}*  🛑 SL (${slDir}) *${fmtNum(sl)}* Текущая: *${fmtNum(cp) ?? '—'}*`, { parse_mode: 'Markdown', ...getMainMenu(ctx.from.id) });
      } catch (e) { console.error(e); await ctx.reply('Ошибка при создании связки'); }
      ctx.session = {};
      return;
    }

    // Old alerts - user selected days (session-driven)
    if (ctx.session.step === 'old_alerts_select_days') {
      if (text === '↩️ Отмена') { ctx.session = {}; await ctx.reply('Отмена', getMainMenu(ctx.from.id)); return; }
      const daysMapLocal = { '7 дней': 7, '30 дней': 30, '90 дней': 90 };
      const days = daysMapLocal[text] || parseInt(text, 10) || 30;
      const token = `d${days}_q`;
      const { pages } = await renderOldAlertsList(ctx.from.id, { days, symbol: null, token });
      const first = pages[0];
      // clear session
      ctx.session = {};
      if (first.buttons && first.buttons.length) {
        await ctx.reply(first.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons, remove_keyboard: true } });
      } else {
        await ctx.reply(first.text, getMainMenu(ctx.from.id));
      }
      return;
    }

    // Old alerts search
    if (ctx.session.step === 'old_alerts_search') {
      if (text === '↩️ Отмена') { ctx.session = {}; await ctx.reply('Отмена', getMainMenu(ctx.from.id)); return; }
      const parts = text.split(/\s+/).filter(Boolean);
      const symbol = parts[0] || null;
      const days = parts[1] ? Math.max(1, parseInt(parts[1], 10)) : 30;
      const token = `d${days}_q${encodeURIComponent(String(symbol || ''))}`;
      const { pages } = await renderOldAlertsList(ctx.from.id, { days, symbol, token });
      const first = pages[0];
      ctx.session = {};
      if (first.buttons && first.buttons.length) {
        await ctx.reply(first.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons, remove_keyboard: true } });
      } else {
        await ctx.reply(first.text, getMainMenu(ctx.from.id));
      }
      return;
    }

  } catch (e) {
    console.error('text handler error', e);
    try { await ctx.reply('Произошла ошибка, попробуй ещё раз.'); } catch {}
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
    console.error('set_alert_limit error', e);
    try { await ctx.reply('Ошибка при выполнении команды'); } catch {}
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
    console.error('get_alert_limit error', e);
    try { await ctx.reply('Ошибка при выполнении команды'); } catch {}
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
      console.warn('Не удалось очистить dailyCache', e);
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
      console.error('preview fetchQuote error', e);
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
      console.error('preview fetchImage error', e);
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
      console.error('fetchAndStoreDailyMotivation error', e);
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
      console.error('read stored doc error', e);
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
      console.error('ensureDailyImageBuffer error', e);
      await ctx.reply(`Ошибка при загрузке изображения: ${String(e?.message || e)}`);
    }

    try {
      const ok = await daily.sendDailyToUser(bot, ctx.from.id, dateStr, { disableNotification: false, forceRefresh: true });
      if (ok) await ctx.reply('Готово — мотивация обновлена и отправлена тебе.');
      else await ctx.reply('Мотивация сохранена, но отправка не удалась (см логи).');
    } catch (e) {
      console.error('sendDailyToUser error', e);
      await ctx.reply(`Ошибка при отправке мотивации: ${String(e?.message || e)}`);
    }

  } catch (e) {
    console.error('refresh_daily top-level error', e);
    try { await ctx.reply('Внутренняя ошибка: ' + String(e?.message || e)); } catch {}
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
  try { await fetchAndStoreDailyMotivation(dateStrNow).catch(()=>{}); } catch (e) { console.warn('initial fetchAndStoreDailyMotivation failed', e); }

  let lastFetchDay = null;
  let lastPrepareDay = null;

  setInterval(async () => {
    try {
      const kyivNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'Europe/Kyiv' }));
      const day = kyivNow.toLocaleDateString('sv-SE');
      const hour = kyivNow.getHours();

      if (day !== lastFetchDay && hour === IMAGE_FETCH_HOUR) {
        try { await fetchAndStoreDailyMotivation(day, { force: true }); } catch (e) { console.warn('daily fetch failed', e); }
        lastFetchDay = day;
      }

      if (day !== lastPrepareDay && hour === PREPARE_SEND_HOUR) {
        try { await fetchAndStoreDailyMotivation(day, { force: false }); } catch (e) { console.warn('daily prepare failed', e); }
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
                } catch (e) { console.warn('scheduled broadcast send failed to', targetId, e); }
              }));
              batch = [];
            }
          }
          if (batch.length) {
            await Promise.all(batch.map(async (targetId) => {
              try {
                const ok = await sendDailyToUser(bot, targetId, dateStr, { disableNotification: false, forceRefresh: false }).catch(()=>false);
                await pendingDailySendsCollection.updateOne({ userId: targetId, date: dateStr }, { $set: { sent: !!ok, sentAt: ok ? new Date() : null, quoteSent: !!ok, permanentFail: !ok } }, { upsert: true });
              } catch (e) { console.warn('scheduled broadcast send failed to', targetId, e); }
            }));
          }
        } catch (e) {
          console.warn('scheduled daily broadcast failed', e);
        }
      }
    } catch (e) { console.warn('daily scheduler error', e); }
  }, 60_000);

  setInterval(async () => {
    try {
      await removeInactive();
    } catch (e) { console.warn('weekly removeInactive failed', e); }
  }, 7 * DAY_MS);

  await bot.launch();
  console.log('Bot started');
  return { server };
}
