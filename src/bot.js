// src/bot.js
import {session, Telegraf} from 'telegraf';
import dotenv from 'dotenv';
import {ObjectId} from './db/db.js';
import {refreshAllTickers} from './prices.js';
import {buildDeleteInlineForUser, renderAlertsList, renderOldAlertsList} from './alerts.js';
import {
  getUserAlertsCached,
  getUserAlertsOrder,
  getUserRecentSymbols,
  invalidateUserAlertsCache,
  resolveUserLang,
  setUserAlertsOrder
} from './cache.js';
import {
  buildCancelButton,
  buildSettingsInlineForUser,
  editHtmlOrReply,
  editReportMessageToFull,
  editReportMessageToShort,
  formatSurpriseMessage,
  geminiToHtml,
  getMainMenuSync,
  handleActiveUsers,
  handleMarketSnapshotRequest,
  handleMotivationRequest,
  mdBoldToHtml,
  splitMessage,
  startTyping,
} from './utils/utils.js';
import {ENTRIES_PER_PAGE, KYIV_TZ} from './constants.js';

import {getMarketSnapshot, sendShortReportToUser} from './utils/marketMonitor.js';
import {registerTextHandlers} from "./utils/textHandlers.js";
import {getOrCreateSurprise} from "./surpriseService.js";

dotenv.config();

const BOT_TOKEN = process.env.BOT_TOKEN;
if (!BOT_TOKEN) throw new Error('BOT_TOKEN не задан в окружении');

export const bot = new Telegraf(BOT_TOKEN);

bot.command('oleg', async (ctx) => {
  try {
    const collName = process.env.WATCH_FLAG_COLL || 'flags';
    const flagId = process.env.WATCH_FLAG_ID || 'collector_win';
    const dbName = process.env.DB_NAME || 'crypto_alert_dev';
    const {client} = await import('./db/db.js');
    const db = client.db(dbName);
    const token = `${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;

    await ctx.reply('⏳ Запускаю обновление…');

    await db.collection(collName).updateOne(
      {_id: flagId},
      {$set: {run: true, notifyChatId: ctx.from.id, requestedAt: new Date(), token}},
      {upsert: true}
    );

    await ctx.reply('✅ Обновление данных запущено');
  } catch {
    try {
      await ctx.reply('⚠️ Не удалось запустить обновление.');
    } catch {
    }
  }
});

bot.catch(async (err, ctx) => {
  try {
    console.error('[telegraf.catch]', err?.stack || String(err));
  } catch {
  }
  try {
    await ctx?.reply?.('⚠️ Внутренняя ошибка, попробуй ещё раз.');
  } catch {
  }
});

bot.use(session());
bot.use((ctx, next) => {
  if (!ctx.session) ctx.session = {};
  return next();
});
bot.use(async (ctx, next) => {
  try {
    if (ctx.from?.id) {
      const {usersCollection} = await import('./db/db.js');
      const u = await usersCollection.findOne({userId: ctx.from.id}, {projection: {lastActive: 1}});
      const now = new Date();
      const last = u?.lastActive ? new Date(u.lastActive) : null;
      if (!last || now - last > 5 * 60 * 1000) {
        await usersCollection.updateOne(
          {userId: ctx.from.id},
          {$set: {userId: ctx.from.id, lastActive: now, language_code: ctx.from.language_code || null}},
          {upsert: true}
        );
      }
    }
  } catch (e) {
  }
  return next();
});

bot.start(async (ctx) => {
  ctx.session = {};

  const lang = await resolveUserLang(ctx.from?.id, null, ctx.from?.language_code);
  const isEn = String(lang).split('-')[0] === 'en';

  const text = isEn
    ? [
      '👋 Hello! I am a crypto alert bot.',
      'en/ru → ⚙️ Settings / Настройки',
      '',
      'What I can do:',
      '• 📈 Create price alerts for your favorite coins',
      '• 🌅 Send an auto morning market report + short & full reports on demand',
      '• 🗺️ Show liquidation maps for popular coins',
      '• 💫 Send a daily motivation image/quote',
      '',
      'You can enable or disable the morning market report and daily motivation at any time in ⚙️ Settings.'
    ].join('\n')
    : [
      '👋 Привет! Я бот для крипто-уведомлений.',
      'en/ru → ⚙️ Настройки / Settings',
      '',
      'Что я умею:',
      '• 📈 Создавать ценовые уведомления по твоим монетам',
      '• 🌅 Присылать авто-утренний отчёт по рынку + краткий и полный отчёт по запросу',
      '• 🗺️ Показывать карты ликвидаций по популярным монетам',
      '• 💫 Присылать ежедневную мотивационную картинку/цитату',
      '',
      'Утренний отчёт по рынку и ежедневную мотивацию можно в любой момент включить или отключить в ⚙️ Настройки.'
    ].join('\n');

  await ctx.reply(text, getMainMenuSync(ctx.from.id, lang));
});


bot.command('menu', async (ctx) => {
  const lang = await resolveUserLang(ctx.from?.id, null, ctx.from?.language_code);
  await ctx.reply(String(lang).startsWith('en') ? 'Main menu' : 'Главное меню', getMainMenuSync(ctx.from.id, lang));
});

bot.hears(['⚙️ Настройки', '⚙️ Settings'], async (ctx) => {
  const lang = await resolveUserLang(ctx.from.id);
  const isEn = String(lang).startsWith('en');
  const inline = await buildSettingsInlineForUser(ctx.from.id);
  const text = isEn
    ? '⚙️ Settings\n— alerts order\n— language\n— daily motivation\n— morning market report\n\nTap to toggle.'
    : '⚙️ Настройки\n— порядок новых алертов\n— язык сообщений\n— ежедневная мотивация\n— утренний отчёт по рынку\n\nНажимай, чтобы переключить.';
  await ctx.reply(text, {reply_markup: inline});
});

bot.hears(['🔮 Удиви меня', '🔮 Surprise me'], async (ctx) => {
  const lang = await resolveUserLang(ctx.from.id)
  const isEn = String(lang).startsWith('en')

  // 1️⃣ сообщение-заглушка
  const loadingMsg = await ctx.reply(
    isEn ? '⏳ Generating …' : '⏳ Генерирую …'
  )

  try {
    await ctx.telegram.sendChatAction(ctx.chat.id, 'typing')
  } catch {}

  try {
    const isDev = process.env.NODE_ENV === 'development'
    const isAdmin = String(ctx.from.id) === String(process.env.CREATOR_ID)

    const { surprise, remainingMs } = await getOrCreateSurprise({
      forceFresh: isDev && isAdmin
    })

    const minutes = Math.max(1, Math.ceil(remainingMs / 60000))
    const text = formatSurpriseMessage(surprise, lang, minutes)

    await ctx.reply(text, { parse_mode: 'HTML' })

    await ctx.deleteMessage(loadingMsg.message_id).catch(() => {})
  } catch (e) {
    console.error('[surprise]', {
      message: e?.message,
      stack: e?.stack,
      response: e?.response?.data,
      status: e?.response?.status,
    })

    await ctx.reply(
      isEn
        ? '⚠️ Internal error, try again later.'
        : '⚠️ Внутренняя ошибка, попробуй позже.'
    )
  }
})

bot.hears(['🛠️ Техподдержка/пожелания', 'Пожелания/техподдержка', '🛠️ Support/wishes', 'Wishes/Support'], async (ctx) => {
  const lang = await resolveUserLang(ctx.from.id);
  const msg = String(lang).startsWith('en')
    ? "The bot is completely free and has no restrictions. If something is broken or you have ideas — write me. If you want to thank me with a cup of coffee — write to me in private @pirial_mersus"
    : "Бот полностью бесплатен и в нем нет никаких ограничений. Если что-то сломалось или есть идеи — напишите мне. Если хотите отблагодарить меня чашечкой кофе — напишите в личку @pirial_mersus";
  await ctx.reply(msg, getMainMenuSync(ctx.from.id, lang));
});

bot.hears(['➕ Создать', '➕ Create alert'], async (ctx) => {
  try {
    ctx.session = {step: 'symbol'};
    refreshAllTickers().catch(() => {
    });
    const lang = await resolveUserLang(ctx.from.id);
    const recent = await getUserRecentSymbols(ctx.from.id);
    const suggest = [...new Set([...recent, ...['BTC', 'ETH', 'SOL', 'BNB', 'XRP', 'DOGE']])].slice(0, 6).map(s => ({text: s}));
    const kb = suggest.length ? [suggest, [buildCancelButton(lang)]] : [[buildCancelButton(lang)]];
    await ctx.reply(String(lang).startsWith('en') ? 'Enter symbol (e.g. BTC) or press a button:' : 'Введи символ (например BTC) или нажми кнопку:', {
      reply_markup: {
        keyboard: kb,
        resize_keyboard: true
      }
    });
  } catch {
    ctx.session = {};
    await ctx.reply(String((await resolveUserLang(ctx.from.id)).startsWith('en')) ? 'Error starting alert creation.' : 'Ошибка при запуске создания алерта');
  }
});

bot.hears(['↩️ Отмена', '↩️ Cancel'], async (ctx) => {
  ctx.session = {};
  const lang = await resolveUserLang(ctx.from.id);
  const isEn = String(lang).startsWith('en');
  await ctx.reply(isEn ? 'Cancelled ✅' : 'Отмена ✅', getMainMenuSync(ctx.from.id, lang));
});

bot.hears(['📋 Мои уведомления', '📋 My alerts'], async (ctx) => {
  try {
    try {
      await bot.telegram.sendChatAction(ctx.chat.id, 'typing');
    } catch {
    }
    const lang = await resolveUserLang(ctx.from.id);
    const {pages} = await renderAlertsList(ctx.from.id, {fast: false, lang});
    const first = pages[0];
    await ctx.reply(mdBoldToHtml(first.text), {parse_mode: 'Markdown', reply_markup: {inline_keyboard: first.buttons}});
  } catch {
    const lang = await resolveUserLang(ctx.from.id);
    await ctx.reply(String(lang).startsWith('en') ? 'Error fetching alerts.' : 'Ошибка при получении алертов.');
  }
});

bot.hears(['🗺️ Карты ликвидаций', '🗺️ Liquidation maps'], async (ctx) => {
  const lang = await resolveUserLang(ctx.from.id);
  const isEn = String(lang).startsWith('en');
  const msg = isEn
    ? 'Enter the crypto symbol (e.g., BTC, ETH, ARB) or tap a button below:'
    : 'Введите символ (например: BTC, ETH, ARB) или нажмите кнопку ниже:';
  const recent = await getUserRecentSymbols(ctx.from.id);
  const {POPULAR_COINS} = await import('./constants.js');
  const suggest = [...new Set([...recent, ...POPULAR_COINS])].slice(0, 6).map(s => ({text: s}));
  const kb = suggest.length ? [suggest, [buildCancelButton(lang)]] : [[buildCancelButton(lang)]];
  ctx.session = {liqAwait: true};
  await ctx.reply(msg, {reply_markup: {keyboard: kb, resize_keyboard: true}});
});

bot.hears(['👥 Количество активных пользователей', '👥 Active users'], async (ctx) => {
  await handleActiveUsers(ctx);
});

bot.hears(['📈 Краткий отчёт', '📈 Short market report'], async (ctx) => {
  try {
    await ctx.telegram.sendChatAction(ctx.chat.id, 'typing').catch(() => {
    });
  } catch {
  }
  try {
    await sendShortReportToUser(bot, ctx.from.id);
  } catch (e) {
    try {
      await ctx.reply('⚠️ Не удалось сформировать краткий отчёт.');
    } catch {
    }
  }
});

bot.hears(['📊 Полный отчёт', '📊 Full report'], handleMarketSnapshotRequest);

bot.hears(['🌅 Прислать мотивацию', '🌅 Send motivation'], handleMotivationRequest);

bot.command('motivate', handleMotivationRequest);
bot.command('market', handleMarketSnapshotRequest);
bot.command('snapshot', handleMarketSnapshotRequest);
bot.command('report', handleMarketSnapshotRequest);

bot.hears(['📜 Старые алерты', '📜 Old alerts'], async (ctx) => {
  ctx.session = {step: 'old_alerts_select_days'};
  const lang = await resolveUserLang(ctx.from.id);
  const isEn = String(lang).startsWith('en');
  const kb = [[{text: isEn ? '7 days' : '7 дней'}, {text: isEn ? '30 days' : '30 дней'}, {text: isEn ? '90 days' : '90 дней'}], [buildCancelButton(lang)]];
  await ctx.reply(isEn ? 'Choose a period to view old alerts:' : 'Выбери период для просмотра старых алертов:', {
    reply_markup: {
      keyboard: kb,
      resize_keyboard: true
    }
  });
});

bot.hears(['🔎 Поиск старых алертов', '🔎 Search old alerts'], async (ctx) => {
  ctx.session = {step: 'old_alerts_search'};
  const lang = await resolveUserLang(ctx.from.id);
  await ctx.reply(String(lang).startsWith('en') ? 'Enter query in format: SYMBOL [DAYS]\nExamples: "BTC", "BTC 30". Default DAYS=30.' : 'Введи запрос в формате: SYMBOL [DAYS]\nПримеры: "BTC", "BTC 30". По умолчанию DAYS=30.', {
    reply_markup: {
      keyboard: [[buildCancelButton(lang)]],
      resize_keyboard: true
    }
  });
});

bot.on('callback_query', async (ctx) => {
  try {
    const data = ctx.callbackQuery?.data;
    if (!data) return ctx.answerCbQuery();

    const lang = await resolveUserLang(ctx.from.id);
    if (data === 'alerts_history') {
      const lang2 = await resolveUserLang(ctx.from.id);
      const isEn = String(lang2).startsWith('en');

      const header = isEn ? '📜 Alerts history' : '📜 История уведомлений';
      const inline = {
        inline_keyboard: [
          [{ text: isEn ? 'Old alerts' : 'Старые алерты', callback_data: 'history_old' }],
          [{ text: isEn ? 'Search old alerts' : 'Поиск старых алертов', callback_data: 'history_search' }],
          [{ text: isEn ? '↩️ Back' : '↩️ Назад', callback_data: 'back_to_alerts_root' }]
        ]
      };

      try {
        await ctx.editMessageText(header, { reply_markup: inline });
      } catch {
        await ctx.reply(header, { reply_markup: inline });
      }

      await ctx.answerCbQuery();
      return;
    }
    if (data === 'market_ai') {
      const userId = ctx.from.id;
      const lang = await resolveUserLang(userId).catch(() => 'ru');
      const isEn = String(lang).toLowerCase().startsWith('en');

      let stopTypingFn = null;
      try {
        stopTypingFn = startTyping(ctx);

        const snap = await getMarketSnapshot(['BTC', 'ETH', 'PAXG']).catch(() => null);
        if (!snap?.ok) {
          await ctx.answerCbQuery(isEn ? 'Error' : 'Ошибка');
          return;
        }

        let answer = null;
        let generatedTimeStr = null;

        const aiSrc = snap.gemini || null;
        if (aiSrc && typeof aiSrc === 'object') {
          const key = isEn ? 'en' : 'ru';
          const fallbackKey = isEn ? 'ru' : 'en';
          const entry = aiSrc[key] || aiSrc[fallbackKey] || null;

          if (entry && typeof entry.text === 'string' && entry.text.trim()) {
            answer = entry.text.trim();

            if (Number.isFinite(entry.createdAt) && entry.createdAt > 0) {
              const date = new Date(entry.createdAt);
              generatedTimeStr = date.toLocaleString(isEn ? 'en-GB' : 'ru-RU', {
                timeZone: KYIV_TZ,
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
              }).replace(',', '');
            }
          }
        }

        if (!answer) {
          answer = isEn
            ? 'AI recommendations are being prepared or temporarily unavailable.'
            : 'Рекомендации ИИ готовятся или временно недоступны.';
        }

        if (!generatedTimeStr) {
          const when = formatKyiv(snap.fetchedAt ?? null, snap.atIsoKyiv || '');
          generatedTimeStr = isEn ? when.en : when.ru;
        }

        const tail = isEn
          ? `\n\n🧠 AI market analysis\nGenerated at: **${generatedTimeStr}** (Europe/Kyiv) — AI answer updates every hour`
          : `\n\n🧠 Ответ ИИ по рынку\nСгенерировано: **${generatedTimeStr}** (Europe/Kyiv) — Анализ ИИ обновляется каждый час`;

        answer += tail;

        const chunks = splitMessage(answer, 3500);
        for (let i = 0; i < chunks.length; i++) {
          const html = geminiToHtml(chunks[i]);
          const baseExtra = { parse_mode: 'HTML', disable_web_page_preview: true };
          const menuExtra = i === chunks.length - 1 ? getMainMenuSync(userId, lang) : {};
          await ctx.reply(html, { ...baseExtra, ...menuExtra });
        }

        await ctx.answerCbQuery(isEn ? 'Done.' : 'Готово.');
      } catch (e) {
        console.error('[market_ai]', e?.stack || e);
        try { await ctx.answerCbQuery(isEn ? 'Error' : 'Ошибка'); } catch {}
      } finally {
        if (stopTypingFn) stopTypingFn();
      }
      return;
    }

    if (data === 'market_short') {
      try {
        await editReportMessageToShort(ctx);
      } catch {
        try {
          await ctx.answerCbQuery('Ошибка');
        } catch {
        }
      }
      return;
    }

    if (data === 'market_full') {
      try {
        await editReportMessageToFull(ctx);
      } catch {
        try {
          await ctx.answerCbQuery('Ошибка');
        } catch {
        }
      }
      return;
    }

    if (data === 'market_help') {
      const mm = await import('./utils/marketMonitor.js');
      try {
        await mm.editReportMessageWithHelp(ctx);
        await ctx.answerCbQuery();
      } catch {
        try {
          await ctx.answerCbQuery('Ошибка');
        } catch {
        }
      }
      return;
    }

    if (data === 'history_old') {
      ctx.session = {step: 'old_alerts_select_days'};
      const lang2 = await resolveUserLang(ctx.from.id);
      const isEn = String(lang2).startsWith('en');
      const kb = [[{text: isEn ? '7 days' : '7 дней'}, {text: isEn ? '30 days' : '30 дней'}, {text: isEn ? '90 days' : '90 дней'}], [buildCancelButton(lang2)]];
      await ctx.reply(isEn ? 'Choose a period to view old alerts:' : 'Выбери период для просмотра старых алертов:', {
        reply_markup: {
          keyboard: kb,
          resize_keyboard: true
        }
      });
      await ctx.answerCbQuery();
      return;
    }
    if (data === 'history_search') {
      ctx.session = {step: 'old_alerts_search'};
      const lang2 = await resolveUserLang(ctx.from.id);
      await ctx.reply(String(lang2).startsWith('en')
          ? 'Enter query in format: SYMBOL [DAYS]\nExamples: "BTC", "BTC 30". Default DAYS=30.'
          : 'Введи запрос в формате: SYMBOL [DAYS]\nПримеры: "BTC", "BTC 30". По умолчанию DAYS=30.',
        {reply_markup: {keyboard: [[buildCancelButton(lang2)]], resize_keyboard: true}});
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'back_to_main') {
      try {
        await ctx.editMessageReplyMarkup({inline_keyboard: []});
      } catch {
      }
      try {
        const lang2 = await resolveUserLang(ctx.from?.id).catch(() => 'ru');
        await ctx.reply(String(lang2).startsWith('en') ? 'Main menu' : 'Главное меню', getMainMenuSync(ctx.from.id, lang2));
      } catch {
      }
      try {
        await ctx.answerCbQuery();
      } catch {
      }
      return;
    }

    if (data === 'back_to_alerts_root') {
      const lang2 = await resolveUserLang(ctx.from.id);
      const { pages } = await renderAlertsList(ctx.from.id, { fast: true, lang: lang2 });
      const page = pages[0];

      try {
        await ctx.editMessageText(page.text, {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: page.buttons }
        });
      } catch {
        await ctx.reply(page.text, {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: page.buttons }
        });
      }

      await ctx.answerCbQuery();
      return;
    }

    if (data === 'toggle_order') {
      const cur = await getUserAlertsOrder(ctx.from.id).catch(() => 'new_bottom');
      const next = cur === 'new_top' ? 'new_bottom' : 'new_top';
      await setUserAlertsOrder(ctx.from.id, next).catch(() => {
      });
      const inline = await buildSettingsInlineForUser(ctx.from.id, lang);
      try {
        await ctx.editMessageReplyMarkup(inline);
      } catch {
      }
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'toggle_lang') {
      const cur = await resolveUserLang(ctx.from.id).catch(() => 'ru');
      const next = String(cur).startsWith('en') ? 'ru' : 'en';
      try {
        const {usersCollection} = await import('./db/db.js');
        await usersCollection.updateOne({userId: ctx.from.id}, {$set: {preferredLang: next}}, {upsert: true});
      } catch {
      }
      try {
        await ctx.reply(next === 'en' ? 'Language switched to English.' : 'Я переключился на русский.', getMainMenuSync(ctx.from.id, next));
      } catch {
      }
      const inline = await buildSettingsInlineForUser(ctx.from.id, next);
      try {
        const header = next === 'en'
          ? '⚙️ Settings\n— alerts order\n— language\n— daily motivation\n— morning market report\n\nTap to toggle.'
          : '⚙️ Настройки\n— порядок новых алертов\n— язык сообщений\n— ежедневная мотивация\n— утренний отчёт по рынку\n\nНажимай, чтобы переключить.';
        try {
          await ctx.editMessageText(header, {reply_markup: inline});
        } catch {
          await ctx.editMessageReplyMarkup(inline);
        }
      } catch {
      }
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'toggle_motivation') {
      try {
        const {usersCollection} = await import('./db/db.js');
        const u = await usersCollection.findOne({userId: ctx.from.id}) || {};
        const next = !(u.sendMotivation !== false);
        await usersCollection.updateOne({userId: ctx.from.id}, {$set: {sendMotivation: next}}, {upsert: true});
      } catch {
      }
      const inline = await buildSettingsInlineForUser(ctx.from.id, lang);
      try {
        await ctx.editMessageReplyMarkup(inline);
      } catch {
      }
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'toggle_market') {
      try {
        const {usersCollection} = await import('./db/db.js');
        const u = await usersCollection.findOne({userId: ctx.from.id}) || {};
        const next = !(u.sendMarketReport !== false);
        await usersCollection.updateOne({userId: ctx.from.id}, {$set: {sendMarketReport: next}}, {upsert: true});
      } catch {
      }
      const inline = await buildSettingsInlineForUser(ctx.from.id, lang);
      try {
        await ctx.editMessageReplyMarkup(inline);
      } catch {
      }
      await ctx.answerCbQuery();
      return;
    }

    const mPage = data.match(/^alerts_page_(\d+)_view$/);
    if (mPage) {
      const pageIdx = parseInt(mPage[1], 10);
      const {pages} = await renderAlertsList(ctx.from.id, {fast: true, lang});
      const page = pages[Math.max(0, Math.min(pageIdx, pages.length - 1))] || pages[0];
      const chatId = ctx.update.callback_query.message.chat.id;
      const msgId = ctx.update.callback_query.message.message_id;
      try {
        await ctx.answerCbQuery();
      } catch {
      }
      await editHtmlOrReply(ctx, chatId, msgId, page.text, page.buttons);
      return;
    }

    const mShow = data.match(/^show_delete_menu_(all|\d+)$/);
    if (mShow) {
      const token = mShow[1];
      const {pages} = await renderAlertsList(ctx.from.id, {fast: true, lang});
      const totalPages = pages.length;
      let sourcePage = null;
      if (token !== 'all') sourcePage = Math.max(0, Math.min(parseInt(token, 10), totalPages - 1));
      const inline = await buildDeleteInlineForUser(ctx.from.id, {
        fast: true,
        sourcePage,
        totalPages: (sourcePage === null ? null : totalPages),
        lang
      });
      try {
        await ctx.editMessageReplyMarkup({inline_keyboard: inline});
      } catch {
        try {
          const originalText = ctx.update.callback_query.message?.text || 'Your alerts';
          await ctx.reply(originalText, {reply_markup: {inline_keyboard: inline}});
        } catch {
        }
      }
      await ctx.answerCbQuery();
      return;
    }

    const mBack = data.match(/^back_to_alerts(?:_p(\d+))?$/);
    if (mBack) {
      const p = mBack[1] ? parseInt(mBack[1], 10) : 0;
      const {pages} = await renderAlertsList(ctx.from.id, {fast: true, lang});
      const page = pages[Math.max(0, Math.min(p, pages.length - 1))] || pages[0];
      await editHtmlOrReply(ctx, ctx.update.callback_query.message.chat.id, ctx.update.callback_query.message.message_id, page.text, page.buttons);
      try {
        await ctx.answerCbQuery();
      } catch {
      }
      return;
    }

    const mSet = data.match(/^set_order_(new_top|new_bottom)$/);
    if (mSet) {
      const order = mSet[1];
      await setUserAlertsOrder(ctx.from.id, order).catch(() => {
      });
      const inline = await buildSettingsInlineForUser(ctx.from.id, lang);
      try {
        await ctx.editMessageReplyMarkup(inline);
      } catch {
        try {
          await ctx.reply(String(lang).startsWith('en') ? 'Order set' : 'Порядок установлен', {reply_markup: inline});
        } catch {
        }
      }
      await ctx.answerCbQuery(String(lang).startsWith('en') ? 'Order set' : 'Порядок установлен');
      return;
    }

    const mDel = data.match(/^del_([0-9a-fA-F]{24})_p(all|\d+)$/);
    const mLegacy = !mDel && data.startsWith('del_') ? data.match(/^del_([0-9a-fA-F]{24})$/) : null;

    if (mDel || mLegacy) {
      const id = (mDel ? mDel[1] : mLegacy[1]);
      const token = mDel ? mDel[2] : null;

      const {alertsCollection} = await import('./db/db.js');
      const doc = await alertsCollection.findOne({_id: new ObjectId(id)});
      if (!doc) {
        await ctx.answerCbQuery('Алерт не найден');
        return;
      }

      let sourcePage = null;
      if (token) {
        if (token === 'all') {
          sourcePage = null;
        } else {
          const p = parseInt(token, 10);
          sourcePage = Number.isFinite(p) && p >= 0 ? p : 0;
        }
      } else {
        try {
          const alertsBefore = await getUserAlertsCached(ctx.from.id);
          const idxBefore = alertsBefore.findIndex(a => String(a._id) === String(doc._id) || a._id?.toString() === id);
          sourcePage = idxBefore >= 0 ? Math.floor(idxBefore / ENTRIES_PER_PAGE) : 0;
        } catch {
          sourcePage = 0;
        }
      }

      try {
        const {alertsArchiveCollection} = await import('./db/db.js');
        await alertsArchiveCollection.insertOne({
          ...doc,
          deletedAt: new Date(),
          deleteReason: 'user_deleted',
          archivedAt: new Date()
        });
      } catch {
      }

      const {alertsCollection: ac} = await import('./db/db.js');
      await ac.deleteOne({_id: new ObjectId(id)});
      invalidateUserAlertsCache(ctx.from.id);

      const alertsAfter = await getUserAlertsCached(ctx.from.id);
      const totalPages = Math.max(1, Math.ceil((alertsAfter.length || 0) / ENTRIES_PER_PAGE));

      if (sourcePage !== null) {
        sourcePage = Math.max(0, Math.min(sourcePage, totalPages - 1));
      }

      const { pages } = await renderAlertsList(ctx.from.id, { fast: true, lang });
      const page = pages[sourcePage ?? 0] || pages[0];

      const deleteInline = await buildDeleteInlineForUser(ctx.from.id, {
        fast: true,
        sourcePage,
        totalPages,
        lang
      });

      try {
        await ctx.editMessageText(page.text, {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: deleteInline }
        });
      } catch {
        await ctx.reply(page.text, {
          parse_mode: 'Markdown',
          reply_markup: { inline_keyboard: deleteInline }
        });
      }

      await ctx.answerCbQuery(
        String(lang).startsWith('en') ? 'Alert deleted' : 'Алерт удалён'
      );
      return;

    }

    const mOldPage = data.match(/^old_alerts_page_(\d+)_view_(d(\d+)_q(.*))$/);
    if (mOldPage) {
      const pageIdx = parseInt(mOldPage[1], 10);
      const token = mOldPage[2];
      const mToken = token.match(/^d(\d+)_q(.*)$/);
      const days = mToken ? parseInt(mToken[1], 10) : 30;
      const q = mToken ? decodeURIComponent(mToken[2]) : '';
      const opts = {days, symbol: q || null, token, lang};
      const {pages} = await renderOldAlertsList(ctx.from.id, opts);
      const page = pages[Math.max(0, Math.min(pageIdx, pages.length - 1))] || pages[0];

      const chatId = ctx.update.callback_query.message.chat.id;
      const msgId = ctx.update.callback_query.message.message_id;

      try {
        await ctx.answerCbQuery();
      } catch {
      }
      await editHtmlOrReply(ctx, chatId, msgId, page.text, page.buttons);
      return;
    }

    if (data === 'clear_old_alerts_confirm') {
      const isEn = String(lang).split('-')[0] === 'en';
      const text = isEn ? 'Are you sure?' : 'Вы уверены?';
      const inline = {
        inline_keyboard: [[{
          text: isEn ? 'Yes' : 'Да',
          callback_data: 'clear_old_alerts_yes'
        }, {text: isEn ? 'No' : 'Нет', callback_data: 'clear_old_alerts_no'}]]
      };
      try {
        await ctx.editMessageText(text, {reply_markup: inline});
      } catch {
        try {
          await ctx.reply(text, {reply_markup: inline});
        } catch {
        }
      }
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'clear_old_alerts_no') {
      try {
        await ctx.editMessageReplyMarkup({inline_keyboard: []});
      } catch {
      }
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'clear_old_alerts_yes') {
      const isEn = String(lang).split('-')[0] === 'en';
      try {
        const alertsMod = await import('./alerts.js');
        const res = await alertsMod.clearUserOldAlerts(ctx.from.id, {forceAll: true});
        const deleted = res?.deletedCount || 0;
        const msg = deleted ? (isEn ? `Deleted ${deleted} items.` : `Удалено ${deleted} записей.`) : (isEn ? 'No old alerts to delete.' : 'Нет старых алертов для удаления.');
        try {
          await ctx.editMessageText(msg, {reply_markup: {inline_keyboard: []}});
        } catch {
          try {
            await ctx.reply(msg);
          } catch {
          }
        }
      } catch {
        try {
          await ctx.answerCbQuery('Error');
        } catch {
        }
      }
      await ctx.answerCbQuery();
      return;
    }

    await ctx.answerCbQuery();
  } catch {
    try {
      await ctx.answerCbQuery('Ошибка');
    } catch {
    }
  }
});

registerTextHandlers(bot);

