// src/handlers/textHandlers.js
import {
  getUserAlertLimit,
  getUserAlertsCached,
  getUserRecentSymbols, invalidateUserAlertsCache,
  pushRecentSymbol,
  resolveUserLang
} from "../cache.js";
import {
  buildAskSlKeyboard,
  buildCancelButton,
  buildDirectionKeyboard,
  fmtNum,
  getMainMenuSync,
  mdBoldToHtml
} from "./utils.js";
import {getLiqMapInfo} from "../liqBridgeApi.js";
import {KYIV_TZ} from "../constants.js";
import {renderOldAlertsList} from "../alerts.js";
import {getCachedPrice} from "../prices.js";
import {ObjectId} from "../db/db.js";

export function registerTextHandlers(bot) {
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
          await ctx.reply(mdBoldToHtml(first.text), {
            parse_mode: 'HTML',
            reply_markup: {inline_keyboard: first.buttons}
          });
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
        const {alertsCollection} = await import('../db/db.js');
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
            const {alertsCollection: ac} = await import('../db/db.js');
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

        const {alertsCollection} = await import('../db/db.js');
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
          const {alertsCollection: ac} = await import('../db/db.js');
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

          const slLine = isEn ? `🛑 SL (${slDir}) <b>${fmtNum(sl)}</b>` : `🛑 SL (${slDir}) <b>${fmtNum(sl)}</b>`;
          const msg = isEn
            ? `✅ Pair created:\n🔔 <b>${ctx.session.symbol}</b>\n${ctx.session.alertCondition === '>' ? '⬆️ when above' : '⬇️ when below'} <b>${fmtNum(ctx.session.alertPrice)}</b>\n${slLine}\nCurrent: <b>${fmtNum(cp) ?? '—'}</b>`
            : `✅ Создана связка:\n🔔 <b>${ctx.session.symbol}</b>\n${ctx.session.alertCondition === '>' ? '⬆️ выше' : '⬇️ ниже'} <b>${fmtNum(ctx.session.alertPrice)}</b>\n${slLine}\nТекущая: <b>${fmtNum(cp) ?? '—'}</b>`;

          await ctx.reply(msg, {
            ...getMainMenuSync(ctx.from.id, lang),
            parse_mode: 'HTML',
            disable_web_page_preview: true
          });
        } catch (e){
          console.error('[Ошибка при создании связки]', {
            message: e?.message,
            stack: e?.stack,
            response: e?.response?.data,
            status: e?.response?.status,
          })
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
          await ctx.reply(mdBoldToHtml(first.text), {
            parse_mode: 'HTML',
            reply_markup: {inline_keyboard: first.buttons}
          });
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
          await ctx.reply(mdBoldToHtml(first.text), {
            parse_mode: 'HTML',
            reply_markup: {inline_keyboard: first.buttons}
          });
        } else {
          await ctx.reply(mdBoldToHtml(first.text), {parse_mode: 'HTML', ...getMainMenuSync(ctx.from.id, lang)});
        }
        return;
      }

    } catch(e) {
      console.error('[surprise]', {
        message: e?.message,
        stack: e?.stack,
        response: e?.response?.data,
        status: e?.response?.status,
      })
      await ctx.reply('Произошла ошибка, попробуй ещё раз.');
      ctx.session = {};
    }
  });
}
