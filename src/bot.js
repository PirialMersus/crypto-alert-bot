// src/bot.js
import { session } from 'telegraf';
import { ObjectId } from 'mongodb';
import { getCollections } from './db.js';
import { renderAlertsList, buildDeleteInlineForUser, getUserAlertsCached, invalidateUserAlertsCache } from './alerts.js';
import { refreshAllTickers, getPriceFast } from './pricing.js';
import { fmtNum } from './utils.js';
import { sendDailyToUser } from './motivation.js';
import { DELETE_MENU_LABEL, ENTRIES_PER_PAGE } from './config.js';

export async function setupBot(bot) {
  // session middleware
  bot.use(session());

  // obtain collections lazily (connectDb must be called before setupBot)
  const { usersCollection, alertsCollection, pendingDailySendsCollection } = getCollections();

  // activity middleware: update lastActive & keep language and recentSymbols up to date
  bot.use(async (ctx, next) => {
    if (!ctx.session) ctx.session = {};
    const uid = ctx.from?.id;
    if (uid) {
      try {
        await usersCollection.updateOne(
          { userId: uid },
          {
            $set: {
              userId: uid,
              username: ctx.from.username || null,
              lastActive: new Date(),
              language_code: ctx.from?.language_code || null
            },
            $setOnInsert: { createdAt: new Date(), recentSymbols: [] }
          },
          { upsert: true }
        );
      } catch (e) {
        console.error('activity middleware error', e);
      }
    }
    return next();
  });

  // /start
  bot.start(ctx => {
    ctx.session = {};
    ctx.reply(
      'Привет! Я бот-алерт для крипты.',
      {
        reply_markup: {
          keyboard: [
            [{ text: '➕ Создать алерт' }, { text: '📋 Мои алерты' }],
            [{ text: '🌅 Прислать мотивацию' }]
          ],
          resize_keyboard: true
        }
      }
    );
  });

  // Create alert: start flow
  bot.hears('➕ Создать алерт', async (ctx) => {
    ctx.session = { step: 'symbol' };
    refreshAllTickers().catch(() => {});
    try {
      const u = await usersCollection.findOne({ userId: ctx.from.id }, { projection: { recentSymbols: 1 } });
      const recent = Array.isArray(u?.recentSymbols) ? u.recentSymbols.slice(-6).reverse() : [];
      const suggest = [...new Set([...recent, 'BTC','ETH','SOL','BNB','XRP','DOGE'])].slice(0,6).map(s => ({ text: s }));
      const kb = suggest.length ? [suggest, [{ text: '↩️ Отмена' }]] : [[{ text: '↩️ Отмена' }]];
      await ctx.reply('Введи символ (например BTC) или нажми кнопку:', { reply_markup: { keyboard: kb, resize_keyboard: true } });
    } catch (e) {
      console.error('create alert keyboard error', e);
      await ctx.reply('Внутренняя ошибка. Попробуй позже.');
      ctx.session = {};
    }
  });

  // Cancel
  bot.hears('↩️ Отмена', ctx => {
    ctx.session = {};
    ctx.reply(
      'Отмена ✅',
      {
        reply_markup: {
          keyboard: [
            [{ text: '➕ Создать алерт' }, { text: '📋 Мои алерты' }],
            [{ text: '🌅 Прислать мотивацию' }]
          ],
          resize_keyboard: true
        }
      }
    );
  });

  // My alerts view (paginated)
  bot.hears('📋 Мои алерты', async (ctx) => {
    try {
      try { await bot.telegram.sendChatAction(ctx.chat.id, 'typing'); } catch (_) {}
      const { pages } = await renderAlertsList(ctx.from.id, { fast: false });
      const first = pages[0];
      await ctx.reply(first.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons } });
    } catch (e) {
      console.error('Мои алерты error', e);
      await ctx.reply('Ошибка при получении алертов.');
    }
  });

  // Motivation button for testing (sends immediately to the user)
  bot.hears('🌅 Прислать мотивацию', async (ctx) => {
    try {
      const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Kyiv' });
      // опция silent: false для кнопки тестирования
      try {
        await sendDailyToUser(bot, ctx.from.id, dateStr, ctx.from?.language_code || 'ru', { silent: false });
      } catch (err) {
        if (err && (err.message === 'quote_pending' || err.message === 'no_motivation_yet')) {
          // уведомим аккуратно, без лишних деталей
          await ctx.reply('Мотивация готовится — попробуй через пару минут.');
          return;
        }
        console.error('sendDailyToUser error (button):', err);
        try { await ctx.reply('Ошибка при отправке мотивации'); } catch {}
        return;
      }
    } catch (e) {
      console.error('motivation button error', e);
      try { await ctx.reply('Ошибка при отправке мотивации'); } catch {}
    }
  });

  // Show delete menu for a specific page — adds inline keyboard under existing text
  bot.action(/show_delete_menu_(\d+)/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
      const sourcePage = Math.max(0, parseInt(ctx.match[1], 10));
      const viewQuick = await renderAlertsList(ctx.from.id, { fast: true });
      const totalPages = viewQuick.pageCount || 1;
      const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage, totalPages });
      try {
        await ctx.editMessageReplyMarkup({ inline_keyboard: inline });
      } catch (err) {
        const originalText = ctx.update.callback_query.message?.text || 'Твои алерты';
        try { await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } }); } catch (e) { console.error('fallback reply failed', e); }
      }

      // background precise refresh to replace placeholders with fresh prices
      (async () => {
        try {
          const freshInline = await buildDeleteInlineForUser(ctx.from.id, { fast: false, sourcePage, totalPages });
          try { await ctx.editMessageReplyMarkup({ inline_keyboard: freshInline }); } catch {}
        } catch (err) { console.error('async refresh delete menu err', err); }
      })();
    } catch (e) {
      console.error('show_delete_menu error', e);
      try { await ctx.answerCbQuery('Ошибка'); } catch {}
    }
  });

  // Show delete menu for all alerts
  bot.action('show_delete_menu_all', async (ctx) => {
    try {
      await ctx.answerCbQuery();
      const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage: null, totalPages: null });
      try {
        await ctx.editMessageReplyMarkup({ inline_keyboard: inline });
      } catch (err) {
        const originalText = ctx.update.callback_query.message?.text || 'Твои алерты';
        try { await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } }); } catch (e) { console.error('fallback show_delete_menu_all reply failed', e); }
      }

      (async () => {
        try {
          const freshInline = await buildDeleteInlineForUser(ctx.from.id, { fast: false, sourcePage: null, totalPages: null });
          try { await ctx.editMessageReplyMarkup({ inline_keyboard: freshInline }); } catch {}
        } catch (err) { console.error('async refresh show_delete_menu_all', err); }
      })();
    } catch (e) {
      console.error('show_delete_menu_all error', e);
      try { await ctx.answerCbQuery('Ошибка'); } catch {}
    }
  });

  // Collapse back to alerts view (supports optional page back_to_alerts_p{N})
  bot.action(/back_to_alerts(?:_p(\d+))?/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
      const requestedPage = ctx.match && ctx.match[1] ? Math.max(0, parseInt(ctx.match[1], 10)) : 0;
      const { pages } = await renderAlertsList(ctx.from.id, { fast: true });
      const idx = Math.min(requestedPage, Math.max(0, pages.length - 1));
      const p = pages[idx] || pages[0];
      try {
        await ctx.editMessageText(p.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: p.buttons } });
      } catch {
        await ctx.reply(p.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: p.buttons } });
      }

      (async () => {
        try {
          const fresh = await renderAlertsList(ctx.from.id, { fast: false });
          const fp = fresh.pages[idx] || fresh.pages[0];
          try { await ctx.editMessageText(fp.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: fp.buttons } }); } catch {}
        } catch (err) { console.error('async refresh back_to_alerts err', err); }
      })();
    } catch (e) {
      console.error('back_to_alerts error', e);
      try { await ctx.answerCbQuery('Ошибка'); } catch {}
    }
  });

  // Page navigation in view mode
  bot.action(/alerts_page_(\d+)_view/, async (ctx) => {
    try {
      await ctx.answerCbQuery();
      const pageIndex = Math.max(0, parseInt(ctx.match[1], 10));
      const { pages } = await renderAlertsList(ctx.from.id, { fast: false });
      const idx = Math.max(0, Math.min(pageIndex, pages.length - 1));
      const p = pages[idx];
      try { await ctx.editMessageText(p.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: p.buttons } }); }
      catch { await ctx.reply(p.text, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: p.buttons } }); }
    } catch (e) {
      console.error('alerts_page action error', e);
      try { await ctx.answerCbQuery('Ошибка'); } catch {}
    }
  });

  // Callback_query handler that includes deletion (authoritative page token encoded in callback_data as del_<id>_p{N|all})
  bot.on('callback_query', async (ctx) => {
    try {
      const data = ctx.callbackQuery?.data;
      if (!data) return ctx.answerCbQuery();

      const m = data.match(/^del_([0-9a-fA-F]{24})_p(all|\d+)$/);
      const mLegacy = !m && data.startsWith('del_') ? data.match(/^del_([0-9a-fA-F]{24})$/) : null;

      if (m || mLegacy) {
        const id = (m ? m[1] : mLegacy[1]);
        const token = m ? m[2] : null;

        const doc = await alertsCollection.findOne({ _id: new ObjectId(id) });
        if (!doc) { await ctx.answerCbQuery('Алерт не найден'); return; }

        // determine sourcePage: token overrides
        let sourcePage = null; // null === "all"
        if (token) {
          sourcePage = (token === 'all') ? null : Math.max(0, parseInt(token, 10));
        } else {
          // legacy fallback: compute index from cached alerts BEFORE deletion
          try {
            const alertsBefore = await getUserAlertsCached(ctx.from.id);
            const idxBefore = alertsBefore.findIndex(a => String(a._id) === String(doc._id) || a._id?.toString() === id);
            if (idxBefore >= 0) sourcePage = Math.floor(idxBefore / (ENTRIES_PER_PAGE || 20));
            else sourcePage = 0;
          } catch (e) { sourcePage = 0; }
        }

        // actual delete
        await alertsCollection.deleteOne({ _id: new ObjectId(id) });
        invalidateUserAlertsCache(ctx.from.id);

        // recompute pages after deletion
        const alertsAfter = await getUserAlertsCached(ctx.from.id);
        const computedTotalPages = Math.max(1, Math.ceil((alertsAfter?.length || 0) / (ENTRIES_PER_PAGE || 20)));

        if (sourcePage !== null) {
          sourcePage = Math.max(0, Math.min(sourcePage, computedTotalPages - 1));
        }

        // rebuild inline keyboard for the same page (or all)
        const inline = await buildDeleteInlineForUser(ctx.from.id, { fast: true, sourcePage, totalPages: (sourcePage === null ? null : computedTotalPages) });

        if (!inline || inline.length === 0) {
          try { await ctx.editMessageText('У тебя больше нет активных алертов.', { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [] } }); } catch (e) { /*ignore*/ }
          await ctx.answerCbQuery('Алерт удалён');
          return;
        }

        try {
          await ctx.editMessageReplyMarkup({ inline_keyboard: inline });
        } catch (err) {
          // fallback: send a new message with the same text + inline
          try {
            const originalText = ctx.update.callback_query.message?.text || 'Твои алерты';
            await ctx.reply(originalText, { parse_mode: 'Markdown', reply_markup: { inline_keyboard: inline } });
          } catch (e) { console.error('fallback after delete failed', e); }
        }

        await ctx.answerCbQuery('Алерт удалён');
        return;
      }

      // If it's not a del_ action – leave default answer
      await ctx.answerCbQuery();
    } catch (e) {
      console.error('callback_query error', e);
      try { await ctx.answerCbQuery('Ошибка'); } catch {}
    }
  });

  // Text handler – supports alert creation flow
  bot.on('text', async (ctx) => {
    try {
      const step = ctx.session.step;
      const text = (ctx.message.text || '').trim();

      if (!step && /^[A-Z0-9]{2,10}$/i.test(text)) ctx.session = { step: 'symbol' };
      if (!ctx.session.step) return;

      // Step: symbol
      if (ctx.session.step === 'symbol') {
        const base = text.toUpperCase();
        const symbol = `${base}-USDT`;
        const price = await getPriceFast(symbol);
        if (Number.isFinite(price)) {
          try { await usersCollection.updateOne({ userId: ctx.from.id }, { $pull: { recentSymbols: base } }); } catch (e) {}
          try { await usersCollection.updateOne({ userId: ctx.from.id }, { $push: { recentSymbols: { $each: [base], $slice: -20 } } }, { upsert: true }); } catch (e) {}
          ctx.session.symbol = symbol;
          ctx.session.step = 'alert_condition';
          await ctx.reply(`✅ Монета: *${symbol}*\nТекущая цена: *${fmtNum(price)}*\nВыбери направление:`, {
            parse_mode: 'Markdown',
            reply_markup: { keyboard: [[{ text: '⬆️ Когда выше' }, { text: '⬇️ Когда ниже' }], [{ text: '↩️ Отмена' }]], resize_keyboard:true }
          });
        } else {
          await ctx.reply('Пара не найдена на KuCoin. Попробуй другой символ.');
          ctx.session = {};
        }
        return;
      }

      // Step: alert_condition
      if (ctx.session.step === 'alert_condition') {
        if (text === '⬆️ Когда выше') ctx.session.alertCondition = '>';
        else if (text === '⬇️ Когда ниже') ctx.session.alertCondition = '<';
        else { await ctx.reply('Выбери ⬆️ или ⬇️'); return; }
        ctx.session.step = 'alert_price';
        await ctx.reply('Введи цену уведомления:', { reply_markup: { keyboard: [[{ text: '↩️ Отмена' }]], resize_keyboard:true } });
        return;
      }

      // Step: alert_price
      if (ctx.session.step === 'alert_price') {
        const v = parseFloat(text);
        if (!Number.isFinite(v)) { await ctx.reply('Введите корректное число'); return; }
        ctx.session.alertPrice = v;
        ctx.session.step = 'ask_sl';
        const hint = ctx.session.alertCondition === '>' ? 'SL будет выше (для шорта — логика обратная)' : 'SL будет ниже';
        await ctx.reply(`Добавить стоп-лосс? ${hint}`, { reply_markup: { keyboard: [[{ text: '🛑 Добавить SL' }, { text: '⏭️ Без SL' }], [{ text: '↩️ Отмена' }]], resize_keyboard:true } });
        return;
      }

      // Step: ask_sl
      if (ctx.session.step === 'ask_sl') {
        if (text === '⏭️ Без SL') {
          await alertsCollection.insertOne({ userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: ctx.session.alertPrice, type: 'alert' });
          invalidateUserAlertsCache(ctx.from.id);
          const cp = await getPriceFast(ctx.session.symbol);
          await ctx.reply(`✅ Алерт создан: *${ctx.session.symbol}* ${ctx.session.alertCondition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${fmtNum(ctx.session.alertPrice)}*\nТекущая цена: *${fmtNum(cp) ?? '—'}*`, { parse_mode: 'Markdown', reply_markup: { keyboard: [[{ text: '➕ Создать алерт' }, { text: '📋 Мои алерты' }], [{ text: '🌅 Прислать мотивацию' }]], resize_keyboard: true } });
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

      // Step: sl_price
      if (ctx.session.step === 'sl_price') {
        const sl = parseFloat(text);
        if (!Number.isFinite(sl)) { await ctx.reply('Введите корректное число SL'); return; }
        const groupId = new ObjectId().toString();
        const slDir = ctx.session.alertCondition === '<' ? 'ниже' : 'выше';
        await alertsCollection.insertMany([
          { userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: ctx.session.alertPrice, type: 'alert', groupId },
          { userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: sl, type: 'sl', slDir, groupId }
        ]);
        invalidateUserAlertsCache(ctx.from.id);
        const cp = await getPriceFast(ctx.session.symbol);
        await ctx.reply(`✅ Создана связка:\n🔔 *${ctx.session.symbol}* ${ctx.session.alertCondition === '>' ? '⬆️ выше' : '⬇️ ниже'} *${fmtNum(ctx.session.alertPrice)}*\n🛑 SL (${slDir}) *${fmtNum(sl)}*\nТекущая: *${fmtNum(cp) ?? '—'}*`, { parse_mode: 'Markdown', reply_markup: { keyboard: [[{ text: '➕ Создать алерт' }, { text: '📋 Мои алерты' }], [{ text: '🌅 Прислать мотивацию' }]], resize_keyboard: true } });
        ctx.session = {};
        return;
      }
    } catch (e) {
      console.error('text handler error', e);
      try { await ctx.reply('Произошла ошибка, попробуй ещё раз.'); } catch {}
      ctx.session = {};
    }
  });
}
