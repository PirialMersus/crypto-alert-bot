// src/bot.js
import {session, Telegraf} from 'telegraf';
import dotenv from 'dotenv';
import {ObjectId} from './db/db.js';
import {getCachedPrice, refreshAllTickers} from './prices.js';
import {buildDeleteInlineForUser, renderAlertsList, renderOldAlertsList} from './alerts.js';
import {
  getUserAlertLimit,
  getUserAlertsCached,
  getUserAlertsOrder,
  getUserRecentSymbols,
  invalidateUserAlertsCache,
  pushRecentSymbol,
  resolveUserLang,
  setUserAlertsOrder
} from './cache.js';
import {
  buildAskSlKeyboard,
  buildCancelButton,
  buildDirectionKeyboard,
  buildSettingsInlineForUser,
  editHtmlOrReply, editReportMessageToFull, editReportMessageToShort,
  extractReportTimeLine,
  fmtNum,
  geminiToHtml,
  getMainMenuSync,
  handleActiveUsers,
  handleMarketSnapshotRequest,
  handleMotivationRequest,
  mdBoldToHtml,
  splitMessage,
  startTyping,
  stopTyping
} from './utils/utils.js';
import {ENTRIES_PER_PAGE, KYIV_TZ} from './constants.js';

import {
  buildMorningReportHtml,
  getMarketSnapshot,
  sendShortReportToUser
} from './utils/marketMonitor.js';
import {getLiqMapInfo} from './liqBridgeApi.js';

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

bot.hears(['📜 История уведомлений', '📜 Alerts history'], async (ctx) => {
  const lang = await resolveUserLang(ctx.from.id);
  const isEn = String(lang).startsWith('en');
  try {
    await ctx.deleteMessage(ctx.message.message_id);
  } catch {
  }
  let rm;
  try {
    rm = await ctx.reply('…', {reply_markup: {remove_keyboard: true}});
  } catch {
  }
  if (rm?.message_id) {
    try {
      await ctx.deleteMessage(rm.message_id);
    } catch {
    }
  }
  const header = isEn ? '📜 Alerts history' : '📜 История уведомлений';
  const inline = {
    inline_keyboard: [
      [{text: isEn ? 'Old alerts' : 'Старые алерты', callback_data: 'history_old'}],
      [{text: isEn ? 'Search old alerts' : 'Поиск старых алертов', callback_data: 'history_search'}],
      [{text: isEn ? '↩️ Back' : '↩️ Назад', callback_data: 'back_to_main'}],
    ]
  };
  await ctx.reply(header, {reply_markup: inline});
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
    if (data === 'market_ai') {
      const userId = ctx.from.id;
      const lang = await resolveUserLang(userId).catch(() => 'ru');
      const isEn = String(lang).toLowerCase().startsWith('en');

      let typingTimer = null;
      try {
        typingTimer = startTyping(ctx);

        const snap = await getMarketSnapshot(['BTC', 'ETH', 'PAXG']).catch(() => null);
        if (!snap?.ok) {
          try {
            await ctx.answerCbQuery(isEn ? 'Error' : 'Ошибка');
          } catch {
          }
          return;
        }

        const fullHtml = await buildMorningReportHtml(
          snap.snapshots,
          lang,
          snap.atIsoKyiv || '',
          snap.fetchedAt ?? null,
          {
            btcDominancePct: snap.btcDominancePct,
            btcDominanceDelta: snap.btcDominanceDelta,
            spx: snap.spx,
            totals: snap.totals,
            fgiNow: snap.fgiNow,
            fgiDelta: snap.fgiDelta,
            oiCvdBTC: snap.oiCvdBTC,
            oiCvdETH: snap.oiCvdETH,
            leadersTop: snap.leadersTop,
            cryptoquant: snap.cryptoquant
          }
        );

        let answer = null;
        const aiSrc = snap.gemini || null;
        if (aiSrc && typeof aiSrc === 'object') {
          const key = isEn ? 'en' : 'ru';
          const fallbackKey = isEn ? 'ru' : 'en';
          const entry = aiSrc[key] || aiSrc[fallbackKey] || null;
          if (entry && typeof entry.text === 'string' && entry.text.trim()) {
            answer = entry.text.trim();
          }
        }

        const timeLine2 = extractReportTimeLine(fullHtml);

        if (timeLine2) {
          const emphasizeTimeLine = (line) => {
            const ru = line.match(/^(Данные на:\s+)(.+?)(\s+—.*)$/);
            if (ru) return `${ru[1]}**${ru[2]}**${ru[3]}`;

            const en = line.match(/^(Data as of:\s+)(.+?)(\s+—.*)$/i);
            if (en) return `${en[1]}**${en[2]}**${en[3]}`;

            return line;
          };

          const decorated = emphasizeTimeLine(timeLine2);

          const tail = isEn
            ? `\n\n🧠 AI answer based on the report\n${decorated}`
            : `\n\n🧠 Ответ ИИ по рынку\n${decorated}`;

          answer += tail;
        }

        const chunks = splitMessage(answer, 3500);
        for (let i = 0; i < chunks.length; i++) {
          const html = geminiToHtml(chunks[i]);
          const baseExtra = { parse_mode: 'HTML', disable_web_page_preview: true };
          const menuExtra = i === chunks.length - 1 ? getMainMenuSync(userId, lang) : {};
          await ctx.reply(html, { ...baseExtra, ...menuExtra });
        }

        try {
          await ctx.answerCbQuery(isEn ? 'Done.' : 'Готово.');
        } catch {
        }
      } catch (e) {
        try {
          console.error('[market_ai]', e?.stack || e);
        } catch {
        }
        try {
          await ctx.answerCbQuery(isEn ? 'Error' : 'Ошибка');
        } catch {
        }
      } finally {
        if (typingTimer) {
          try {
            stopTyping(typingTimer);
          } catch {
          }
        }
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
      const computedTotalPages = Math.max(1, Math.ceil((alertsAfter?.length || 0) / ENTRIES_PER_PAGE));
      if (sourcePage !== null) {
        sourcePage = Math.max(0, Math.min(sourcePage, computedTotalPages - 1));
      }

      const inline2 = await buildDeleteInlineForUser(ctx.from.id, {
        fast: true,
        sourcePage,
        totalPages: (sourcePage === null ? null : computedTotalPages),
        lang
      });

      if (!inline2 || inline2.length === 0) {
        try {
          await ctx.editMessageText(
            String(lang).startsWith('en') ? 'You have no active alerts.' : 'У тебя больше нет активных алертов.',
            {parse_mode: 'HTML', reply_markup: {inline_keyboard: []}}
          );
        } catch {
          try {
            await ctx.reply(String(lang).startsWith('en') ? 'You have no active alerts.' : 'У тебя больше нет активных алертов.', {parse_mode: 'HTML'});
          } catch {
          }
        }
        await ctx.answerCbQuery(String(lang).startsWith('en') ? 'Alert deleted' : 'Алерт удалён');
        return;
      }

      try {
        await ctx.editMessageReplyMarkup({inline_keyboard: inline2});
      } catch {
        try {
          const originalText = ctx.update.callback_query.message?.text || (String(lang).startsWith('en') ? 'Your alerts' : 'Твои алерты');
          await ctx.reply(originalText, {reply_markup: {inline_keyboard: inline2}});
        } catch {
        }
      }

      await ctx.answerCbQuery(String(lang).startsWith('en') ? 'Alert deleted' : 'Алерт удалён');
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

bot.on('text', async (ctx, next) => {
  if (!ctx.session?.liqAwait) return next();

  const txt = (ctx.message?.text || '').trim();
  const lang = await resolveUserLang(ctx.from.id);
  const isEn = String(lang).startsWith('en');

  if (txt === '↩️ Отмена' || txt === '↩️ Cancel') {
    ctx.session = {};
    await ctx.reply(isEn ? 'Cancelled ✅' : 'Отмена ✅', getMainMenuSync(ctx.from.id, lang));
    return;
  }

  const symbol = txt.toUpperCase();

  try {
    const loading = await ctx.reply(isEn ? '⏳ Fetching liquidation map…' : '⏳ Получаю карту ликвидаций…');
    const info = await getLiqMapInfo(symbol);
    const fileId = info.file_id;
    const header = isEn ? '🗺️ Liquidation map' : '🗺️ Карта ликвидаций';
    const pairLabel = (symbol || '').toUpperCase();
    const explain = isEn
      ? 'Shows clusters of stop-loss/liquidation areas on futures markets; helps spot squeeze zones and liquidity pools.'
      : 'Показывает кластеры стопов/ликвидаций на фьючерсных рынках; помогает видеть зоны сквизов и «пулы ликвидности».';
    let timeLine = '';
    if (typeof info.snapshot_ts === 'number' && info.snapshot_ts > 0) {
      const asOf = new Date(info.snapshot_ts).toLocaleString('uk-UA', {
        timeZone: KYIV_TZ,
        year: 'numeric', month: '2-digit', day: '2-digit',
        hour: '2-digit', minute: '2-digit', hourCycle: 'h23'
      });
      timeLine = isEn
        ? `\n\nSnapshot time: <b>${asOf} (Europe/Kyiv)</b>. Updates in 15 minutes`
        : `\n\nВремя снимка: <b>${asOf} (Europe/Kyiv)</b>. Обновится через 15 минут`;
    } else {
      timeLine = isEn
        ? `\n\nUpdates in 15 minutes`
        : `\n\nОбновится через 15 минут`;
    }

    const menu = getMainMenuSync(ctx.from.id, lang);
    await ctx.replyWithPhoto(fileId, {
      ...menu,
      caption: `${header} — ${pairLabel}\n\n${explain}${timeLine}`,
      parse_mode: 'HTML'
    });

    try {
      await ctx.deleteMessage(loading.message_id);
    } catch {
    }
    try {
      await pushRecentSymbol(ctx.from.id, pairLabel);
    } catch {
    }

    try {
      ctx.session.liqAwait = false;
    } catch {
    }
  } catch (e) {
    try {
      ctx.session.liqAwait = true;
    } catch {
    }
    try {
      ctx.session.step = null;
    } catch {
    }

    const lang2 = await resolveUserLang(ctx.from.id);
    const isEn2 = String(lang2).startsWith('en');
    const recent = await getUserRecentSymbols(ctx.from.id).catch(() => []);
    const {POPULAR_COINS} = await import('./constants.js');
    const suggestRow = [...new Set([...recent, ...POPULAR_COINS])].slice(0, 6).map(s => ({text: s}));
    const liqReplyMarkup = {
      reply_markup: {
        keyboard: (suggestRow.length ? [suggestRow, [buildCancelButton(lang2)]] : [[buildCancelButton(lang2)]]),
        resize_keyboard: true
      }
    };

    await ctx.reply(
      isEn2
        ? '❗ Symbol not found or service unavailable. Try: BTC, ETH, ARB.'
        : '❗ Проверь правильность написания или монета с низкой ликвидностью. Попробуй: BTC, ETH, ARB.',
      liqReplyMarkup
    );
    return;
  }
});

bot.on('text', async (ctx) => {
  if (ctx.session?.liqAwait) return;
  try {
    const step = ctx.session.step;
    const text = (ctx.message.text || '').trim();

    const daysMap = {'7 дней': 7, '30 дней': 30, '90 дней': 90, '7 days': 7, '30 days': 30, '90 days': 90};
    const numeric = parseInt(text.replace(/\D/g, ''), 10);
    const isNumericDay = Number.isFinite(numeric) && [7, 30, 90].includes(numeric);
    const normalized = text.toLowerCase();
    const isDaysPhrase = daysMap[text] || daysMap[normalized] || isNumericDay || /^\d+\s*дн/i.test(text) || /^\d+\s*day/i.test(text);
    if ((!step || step === 'old_alerts_select_days') && isDaysPhrase) {
      const days = daysMap[text] || daysMap[normalized] || (isNumericDay ? numeric : 30);
      const token = `d${days}_q`;
      const lang = await resolveUserLang(ctx.from.id);
      const {pages} = await renderOldAlertsList(ctx.from.id, {days, symbol: null, token, lang});
      const first = pages[0];
      ctx.session = {};
      if (first.buttons && first.buttons.length) {
        await ctx.reply(mdBoldToHtml(first.text), {parse_mode: 'HTML', reply_markup: {inline_keyboard: first.buttons}});
      } else {
        await ctx.reply(mdBoldToHtml(first.text), {parse_mode: 'HTML', ...getMainMenuSync(ctx.from.id, lang)});
      }
      return;
    }

    if (!step && /^[A-Z0-9]{2,10}$/i.test(text)) ctx.session = {step: 'symbol'};
    if (!ctx.session.step) return;

    if (ctx.session.step === 'symbol') {
      const base = text.toUpperCase();
      const symbol = `${base}-USDT`;
      const price = await getCachedPrice(symbol);
      if (Number.isFinite(price)) {
        try {
          await pushRecentSymbol(ctx.from.id, base);
        } catch {
        }
        ctx.session.symbol = symbol;
        ctx.session.step = 'alert_condition';
        const lang = await resolveUserLang(ctx.from.id);

        // ⬇️ многострочное HTML-сообщение с жирным тикером и ценой
        const isEn = String(lang).startsWith('en');
        const html = isEn
          ? `✅ Coin: <b>${symbol}</b>\nCurrent price: <b>${fmtNum(price)}</b>\nChoose direction: 👇`
          : `✅ Монета: <b>${symbol}</b>\nТекущая цена: <b>${fmtNum(price)}</b>\nВыбери направление: 👇`;
        await ctx.reply(html, {
          parse_mode: 'HTML',
          reply_markup: buildDirectionKeyboard(lang),
          disable_web_page_preview: true
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
      else {
        await ctx.reply(String(lang).startsWith('en') ? 'Choose ⬆️ or ⬇️' : 'Выбери ⬆️ или ⬇️');
        return;
      }
      ctx.session.step = 'alert_price';
      await ctx.reply(String(lang).startsWith('en') ? 'Enter alert price:' : 'Введи цену уведомления:', {
        reply_markup: {
          keyboard: [[buildCancelButton(lang)]],
          resize_keyboard: true
        }
      });
      return;
    }

    if (ctx.session.step === 'alert_price') {
      const v = parseFloat(text);
      if (!Number.isFinite(v)) {
        await ctx.reply('Введите корректное число');
        return;
      }
      ctx.session.alertPrice = v;
      ctx.session.step = 'ask_sl';
      const lang = await resolveUserLang(ctx.from.id);
      const hint = ctx.session.alertCondition === '>' ? (String(lang).startsWith('en') ? 'SL will be higher (for short — reverse)' : 'SL будет выше (для шорта — логика обратная)') : (String(lang).startsWith('en') ? 'SL will be lower' : 'SL будет ниже');
      await ctx.reply((String(lang).startsWith('en') ? 'Add stop-loss?' : 'Добавить стоп-лосс?') + ` ${hint}`, {reply_markup: buildAskSlKeyboard(lang)});
      return;
    }

    if (ctx.session.step === 'ask_sl') {
      const {alertsCollection} = await import('./db/db.js');
      const limit = await getUserAlertLimit(ctx.from.id).catch(() => 1000000000);
      let currentCount = 0;
      try {
        currentCount = await alertsCollection.countDocuments({userId: ctx.from.id});
      } catch {
        const currentAlerts = await getUserAlertsCached(ctx.from.id).catch(() => []);
        currentCount = (currentAlerts?.length || 0);
      }

      if (currentCount >= limit) {
        const lang = await resolveUserLang(ctx.from.id);
        await ctx.reply(String(lang).startsWith('en') ? `You already have ${currentCount} alerts — limit ${limit}. Contact @pirial_gena to increase.` : `У тебя уже ${currentCount} алертов — достигнут лимит ${limit}. Если нужно увеличить лимит, напиши мне: @pirial_genа`, getMainMenuSync(ctx.from.id, lang));
        ctx.session = {};
        return;
      }

      const lang = await resolveUserLang(ctx.from.id);
      if (text === (String(lang).startsWith('en') ? '⏭️ Skip SL' : '⏭️ Без SL')) {
        try {
          const {alertsCollection: ac} = await import('./db/db.js');
          const beforeInsertCount = await ac.countDocuments({userId: ctx.from.id}).catch(() => currentCount);
          if (beforeInsertCount >= limit) {
            await ctx.reply(String(lang).startsWith('en') ? `You already have ${beforeInsertCount} alerts — limit ${limit}.` : `У тебя уже ${beforeInsertCount} алертов — достигнут лимит ${limit}. Если нужно увеличить лимит, напиши мне: @pirial_gena`, getMainMenuSync(ctx.from.id, lang));
            ctx.session = {};
            return;
          }

          await ac.insertOne({
            userId: ctx.from.id,
            symbol: ctx.session.symbol,
            condition: ctx.session.alertCondition,
            price: ctx.session.alertPrice,
            type: 'alert',
            createdAt: new Date()
          });
          invalidateUserAlertsCache(ctx.from.id);
          const cp = await getCachedPrice(ctx.session.symbol);
          const isEn = String(lang).startsWith('en');
          const conditionLine = ctx.session.alertCondition === '>' ? (isEn ? '⬆️ when above' : '⬆️ выше') : (isEn ? '⬇️ when below' : '⬇️ ниже');

          // ⬇️ жирный тикер, жирная целевая, жирная текущая
          const msg = isEn
            ? `✅ Alert created:\n🔔 <b>${ctx.session.symbol}</b>\n${conditionLine} <b>${fmtNum(ctx.session.alertPrice)}</b>\nCurrent: <b>${fmtNum(cp) ?? '—'}</b>`
            : `✅ Алерт создан:\n🔔 <b>${ctx.session.symbol}</b>\n${conditionLine} <b>${fmtNum(ctx.session.alertPrice)}</b>\nТекущая: <b>${fmtNum(cp) ?? '—'}</b>`;

          await ctx.reply(msg, {
            ...getMainMenuSync(ctx.from.id, lang),
            parse_mode: 'HTML',
            disable_web_page_preview: true
          });
        } catch {
          await ctx.reply('Ошибка при создании алерта');
        }
        ctx.session = {};
        return;
      }
      if (text === (String(lang).startsWith('en') ? '🛑 Add SL' : '🛑 Добавить SL')) {
        ctx.session.step = 'sl_price';
        await ctx.reply(String(lang).startsWith('en') ? 'Enter stop-loss price:' : 'Введи цену стоп-лосса:', {
          reply_markup: {
            keyboard: [[buildCancelButton(lang)]],
            resize_keyboard: true
          }
        });
        return;
      }
      await ctx.reply(String(lang).startsWith('en') ? 'Choose: 🛑 Add SL / ⏭️ Skip SL' : 'Выбери опцию: 🛑 Добавить SL / ⏭️ Без SL');
      return;
    }

    if (ctx.session.step === 'sl_price') {
      const sl = parseFloat(text);
      if (!Number.isFinite(sl)) {
        await ctx.reply('Введите корректное число SL');
        return;
      }

      const {alertsCollection} = await import('./db/db.js');
      const limit = await getUserAlertLimit(ctx.from.id).catch(() => 1000000000);
      let currentCount = 0;
      try {
        currentCount = await alertsCollection.countDocuments({userId: ctx.from.id});
      } catch {
        const currentAlerts = await getUserAlertsCached(ctx.from.id).catch(() => []);
        currentCount = (currentAlerts?.length || 0);
      }

      if (currentCount + 2 > limit) {
        const lang = await resolveUserLang(ctx.from.id);
        await ctx.reply(String(lang).startsWith('en') ? `Can't create pair (alert + SL). You have ${currentCount} alerts, limit ${limit}.` : `Нельзя создать связку (уведомление + SL). У тебя сейчас ${currentCount} алертов, лимит ${limit}. Чтобы увеличить лимит напиши: @pirial_genа`, getMainMenuSync(ctx.from.id, lang));
        ctx.session = {};
        return;
      }

      try {
        const groupId = new ObjectId().toString();
        const beforeInsertCount = await alertsCollection.countDocuments({userId: ctx.from.id}).catch(() => currentCount);
        if (beforeInsertCount + 2 > limit) {
          const lang = await resolveUserLang(ctx.from.id);
          await ctx.reply(String(lang).startsWith('en') ? `Can't create pair (alert + SL). You have ${beforeInsertCount} alerts, limit ${limit}.` : `Нельзя создать связку (уведомление + SL). У тебя сейчас ${beforeInsertCount} алертов, лимит ${limit}. Чтобы увеличить лимит напиши: @pirial_genа`, getMainMenuSync(ctx.from.id, lang));
          ctx.session = {};
          return;
        }

        const slDir = ctx.session.alertCondition === '<' ? (await resolveUserLang(ctx.from.id)) === 'en' ? 'lower' : 'ниже' : (await resolveUserLang(ctx.from.id)) === 'en' ? 'higher' : 'выше';
        const {alertsCollection: ac} = await import('./db/db.js');
        await ac.insertMany([
          {
            userId: ctx.from.id,
            symbol: ctx.session.symbol,
            condition: ctx.session.alertCondition,
            price: ctx.session.alertPrice,
            type: 'alert',
            groupId,
            createdAt: new Date()
          },
          {
            userId: ctx.from.id,
            symbol: ctx.session.symbol,
            condition: ctx.session.alertCondition,
            price: sl,
            type: 'sl',
            slDir,
            groupId,
            createdAt: new Date()
          }
        ]);
        invalidateUserAlertsCache(ctx.from.id);
        const cp = await getCachedPrice(ctx.session.symbol);
        const lang = await resolveUserLang(ctx.from.id);
        const isEn = String(lang).startsWith('en');

        // ⬇️ жирные тикер/цены
        const slLine = isEn ? `🛑 SL (${slDir}) <b>${fmtNum(sl)}</b>` : `🛑 SL (${slDir}) <b>${fmtNum(sl)}</b>`;
        const msg = isEn
          ? `✅ Pair created:\n🔔 <b>${ctx.session.symbol}</b>\n${ctx.session.alertCondition === '>' ? '⬆️ when above' : '⬇️ when below'} <b>${fmtNum(ctx.session.alertPrice)}</b>\n${slLine}\nCurrent: <b>${fmtNum(cp) ?? '—'}</b>`
          : `✅ Создана связка:\n🔔 <b>${ctx.session.symbol}</b>\n${ctx.session.alertCondition === '>' ? '⬆️ выше' : '⬇️ ниже'} <b>${fmtNum(ctx.session.alertPrice)}</b>\n${slLine}\nТекущая: <b>${fmtNum(cp) ?? '—'}</b>`;

        await ctx.reply(msg, {
          ...getMainMenuSync(ctx.from.id, lang),
          parse_mode: 'HTML',
          disable_web_page_preview: true
        });
      } catch {
        await ctx.reply('Ошибка при создании связки');
      }
      ctx.session = {};
      return;
    }

    if (ctx.session.step === 'old_alerts_select_days') {
      if (text === '↩️ Отмена' || text === '↩️ Cancel') {
        ctx.session = {};
        const lang = await resolveUserLang(ctx.from.id);
        await ctx.reply('Отмена', getMainMenuSync(ctx.from.id, lang));
        return;
      }
      const daysMapLocal = {'7 дней': 7, '30 дней': 30, '90 дней': 90};
      const days = daysMapLocal[text] || parseInt(text, 10) || 30;
      const token = `d${days}_q`;
      const lang = await resolveUserLang(ctx.from.id);
      const {pages} = await renderOldAlertsList(ctx.from.id, {days, symbol: null, token, lang});
      const first = pages[0];
      ctx.session = {};
      if (first.buttons && first.buttons.length) {
        await ctx.reply(mdBoldToHtml(first.text), {parse_mode: 'HTML', reply_markup: {inline_keyboard: first.buttons}});
      } else {
        await ctx.reply(mdBoldToHtml(first.text), {parse_mode: 'HTML', ...getMainMenuSync(ctx.from.id, lang)});
      }
      return;
    }

    if (ctx.session.step === 'old_alerts_search') {
      if (text === '↩️ Отмена' || text === '↩️ Cancel') {
        ctx.session = {};
        const lang = await resolveUserLang(ctx.from.id);
        await ctx.reply('Отмена', getMainMenuSync(ctx.from.id, lang));
        return;
      }
      const parts = text.split(/\s+/).filter(Boolean);
      const symbol = parts[0] || null;
      const days = parts[1] ? Math.max(1, parseInt(parts[1], 10)) : 30;
      const token = `d${days}_q${encodeURIComponent(String(symbol || ''))}`;
      const lang = await resolveUserLang(ctx.from.id);
      const {pages} = await renderOldAlertsList(ctx.from.id, {days, symbol, token, lang});
      const first = pages[0];
      ctx.session = {};
      if (first.buttons && first.buttons.length) {
        await ctx.reply(mdBoldToHtml(first.text), {parse_mode: 'HTML', reply_markup: {inline_keyboard: first.buttons}});
      } else {
        await ctx.reply(mdBoldToHtml(first.text), {parse_mode: 'HTML', ...getMainMenuSync(ctx.from.id, lang)});
      }
      return;
    }

  } catch {
    await ctx.reply('Произошла ошибка, попробуй ещё раз.');
    ctx.session = {};
  }
});
