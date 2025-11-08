// src/bot.js
import { Telegraf, session } from 'telegraf';
import dotenv from 'dotenv';
import { connectToMongo, ObjectId, countDocumentsWithTimeout, isDbConnected } from './db.js';
import { createServer } from './server.js';
import { startTickersRefresher, refreshAllTickers, getCachedPrice } from './prices.js';
import { startAlertsChecker, renderAlertsList, buildDeleteInlineForUser, renderOldAlertsList } from './alerts.js';
import { removeInactive } from './cleanup.js';
import { getUserRecentSymbols, pushRecentSymbol, getUserAlertsOrder, setUserAlertsOrder, getUserAlertsCached, invalidateUserAlertsCache, statsCache, getUserAlertLimit, setUserAlertLimit, resolveUserLang } from './cache.js';
import { fmtNum, safeSendTelegram } from './utils.js';
import { sendDailyToUser, processDailyQuoteRetry, watchForNewQuotes, fetchAndStoreDailyMotivation, ensureDailyImageBuffer } from './daily.js';
import { CACHE_TTL, INACTIVE_DAYS, DAY_MS, IMAGE_FETCH_HOUR, PREPARE_SEND_HOUR, ENTRIES_PER_PAGE, KYIV_TZ, MARKET_SEND_HOUR, MARKET_SEND_MIN, MARKET_BATCH_SIZE, MARKET_BATCH_PAUSE_MS } from './constants.js';
import { setLastHeartbeat } from './monitor.js';
import {
  startMarketMonitor,
  getMarketSnapshot,
  broadcastMarketSnapshot,
  sendMarketReportToUser,
  buildMorningReportHtml,
  editReportMessageToFull,
  editReportMessageToShort,
  sendShortReportToUser
} from './marketMonitor.js';
import { getLiqMapInfo } from './liqBridgeApi.js';

dotenv.config();

const BOT_TOKEN = process.env.BOT_TOKEN;
const CREATOR_ID = process.env.CREATOR_ID ? parseInt(process.env.CREATOR_ID, 10) : null;
if (!BOT_TOKEN) throw new Error('BOT_TOKEN не задан в окружении');

export const bot = new Telegraf(BOT_TOKEN);
bot.command('interest', (ctx) => handleInterest(ctx, { size: 50 }));

bot.catch(async (err, ctx) => {
  try { console.error('[telegraf.catch]', err?.stack || String(err)); } catch {}
  try { await ctx?.reply?.('⚠️ Внутренняя ошибка, попробуй ещё раз.'); } catch {}
});

bot.use(session());
bot.use((ctx, next) => { if (!ctx.session) ctx.session = {}; return next(); });
bot.use(async (ctx, next) => {
  try {
    if (ctx.from?.id) {
      const { usersCollection } = await import('./db.js');
      await usersCollection.updateOne(
        { userId: ctx.from.id },
        { $set: { userId: ctx.from.id, lastActive: new Date(), language_code: ctx.from.language_code || null } },
        { upsert: true }
      );
    }
  } catch (e) {}
  return next();
});

const reportInFlight = new Map();

// ⬇️ конвертация **...** / __...__ в <b>...</b> — для листингов/историй
function mdBoldToHtml(s) {
  return String(s)
    .replace(/\*\*(.+?)\*\*/g, '<b>$1</b>')
    .replace(/__(.+?)__/g, '<b>$1</b>');
}

// универсальная правка: безопасное редактирование текста с HTML или фолбэк на reply
async function editHtmlOrReply(ctx, chatId, msgId, text, buttons) {
  const html = mdBoldToHtml(text);
  try {
    await ctx.telegram.editMessageText(
      chatId,
      msgId,
      undefined,
      html,
      { parse_mode: 'Markdown', reply_markup: buttons ? { inline_keyboard: buttons } : undefined, disable_web_page_preview: true }
    );
  } catch {
    await ctx.reply(html, { parse_mode: 'HTML', reply_markup: buttons ? { inline_keyboard: buttons } : undefined, disable_web_page_preview: true });
  }
}

function startTyping(ctx) {
  try { ctx.telegram.sendChatAction(ctx.chat.id, 'typing').catch(()=>{}); } catch {}
  const t = setInterval(() => { try { ctx.telegram.sendChatAction(ctx.chat.id, 'typing').catch(()=>{}); } catch {} }, 4000);
  return t;
}
function stopTyping(t) { try { if (t) clearInterval(t); } catch {} }
function lockReport(userId, ms = 30000) { reportInFlight.set(userId, { until: Date.now() + ms, typingTimer: null, startedMsgId: null }); }
function unlockReport(userId) { const s = reportInFlight.get(userId); if (s?.typingTimer) stopTyping(s.typingTimer); reportInFlight.delete(userId); }
function isLocked(userId) { const s = reportInFlight.get(userId); if (!s) return false; if (Date.now() > s.until) { unlockReport(userId); return false; } return true; }

function supportText(isEn) { return isEn ? '🛠️ Support/wishes' : '🛠️ Техподдержка/пожелания'; }

function getMainMenuSync(userId, lang = 'ru') {
  const isEn = String(lang).split('-')[0] === 'en';
  const create   = isEn ? '➕ Create alert' : '➕ Создать алерт';
  const my       = isEn ? '📋 My alerts' : '📋 Мои алерты';
  const shortBtn = isEn ? '📈 Short market report' : '📈 Краткий отчёт';
  const fullBtn  = isEn ? '📊 Full report' : '📊 Полный отчёт';
  const history  = isEn ? '📜 Alerts history' : '📜 История алертов';
  const liqBtn   = isEn ? '🗺️ Liquidation maps' : '🗺️ Карты ликвидаций';
  const settings = isEn ? '⚙️ Settings' : '⚙️ Настройки';
  const motivate = isEn ? '🌅 Send motivation' : '🌅 Прислать мотивацию';
  const stats    = isEn ? '👥 Active users' : '👥 Количество активных пользователей';

  const kb = [
    [{ text: create }, { text: my }],
    [{ text: shortBtn }, { text: fullBtn }],
    [{ text: liqBtn }, { text: history }],
    [{ text: supportText(isEn) }, { text: settings }],
  ];
  if (CREATOR_ID && String(userId) === String(CREATOR_ID)) {
    kb.push([{ text: motivate }], [{ text: stats }]);
  }
  return { reply_markup: { keyboard: kb, resize_keyboard: true } };
}

function getMainMenuBusy(userId, lang = 'ru') {
  const isEn = String(lang).split('-')[0] === 'en';
  const create   = isEn ? '➕ Create alert' : '➕ Создать алерт';
  const my       = isEn ? '📋 My alerts' : '📋 Мои алерты';
  const shortBtn = isEn ? '📈 Short market report' : '📈 Краткий отчёт';
  const busy     = isEn ? '📊 ⏳ Building…' : '📊 ⏳ Формирую…';
  const history  = isEn ? '📜 Alerts history' : '📜 История алертов';
  const settings = isEn ? '⚙️ Settings' : '⚙️ Настройки';

  const kb = [
    [{ text: create }, { text: my }],
    [{ text: shortBtn }, { text: busy }],
    [{ text: history }],
    [{ text: supportText(isEn) }, { text: settings }],
  ];
  if (CREATOR_ID && String(userId) === String(CREATOR_ID)) {
    kb.push([{ text: isEn ? '🌅 Send motivation' : '🌅 Прислать мотивацию' }], [{ text: isEn ? '👥 Active users' : '👥 Количество активных пользователей' }]);
  }
  return { reply_markup: { keyboard: kb, resize_keyboard: true } };
}

async function showMainMenuSilently(ctx, lang) {
  try {
    const isEn = String(lang).startsWith('en');
    const menu = getMainMenuSync(ctx.from.id, lang);
    await ctx.telegram.sendMessage(
      ctx.chat.id,
      isEn ? 'Main menu' : 'Главное меню',
      {
        ...menu,
        disable_notification: true
      }
    );
  } catch {}
}
async function buildSettingsInlineForUser(userId, langOverride = null) {
  const order = await getUserAlertsOrder(userId).catch(()=> 'new_bottom');
  const lang = langOverride || await resolveUserLang(userId).catch(()=> 'ru');
  const isEn = String(lang).split('-')[0] === 'en';
  const isTop = order === 'new_top';
  let sendMotivation = true;
  let sendMarketReport = true;
  try {
    const { usersCollection } = await import('./db.js');
    const u = await usersCollection.findOne({ userId });
    if (typeof u?.sendMotivation === 'boolean') sendMotivation = u.sendMotivation;
    if (typeof u?.sendMarketReport === 'boolean') sendMarketReport = u.sendMarketReport;
  } catch {}
  const kb = [
    [{ text: (isEn ? 'New: ' : 'Новые: ') + (isTop ? '↑' : '↓'), callback_data: 'toggle_order' }],
    [{ text: '🌐 ' + (isEn ? 'Language: English' : 'Язык: Русский'), callback_data: 'toggle_lang' }],
    [{ text: `🌅 ${isEn ? 'Motivation' : 'Мотивация'}: ${sendMotivation ? '✅' : '🚫'}`, callback_data: 'toggle_motivation' }],
    [{ text: `📊 ${isEn ? 'Report' : 'Отчёт'}: ${sendMarketReport ? '✅' : '🚫'}`, callback_data: 'toggle_market' }],
    [{ text: isEn ? '↩️ Back' : '↩️ Назад', callback_data: 'back_to_main' }]
  ];
  return { inline_keyboard: kb };
}

function buildCancelButton(lang) { return String(lang).startsWith('en') ? { text: '↩️ Cancel' } : { text: '↩️ Отмена' }; }
function buildDirectionKeyboard(lang) {
  const isEn = String(lang).startsWith('en');
  return { keyboard: [[{ text: isEn ? '⬆️ When above' : '⬆️ Когда выше' }, { text: isEn ? '⬇️ When below' : '⬇️ Когда ниже' }], [buildCancelButton(lang)]], resize_keyboard: true };
}
function buildAskSlKeyboard(lang) {
  const isEn = String(lang).startsWith('en');
  return { keyboard: [[{ text: isEn ? '🛑 Add SL' : '🛑 Добавить SL' }, { text: isEn ? '⏭️ Skip SL' : '⏭️ Без SL' }], [buildCancelButton(lang)]], resize_keyboard: true };
}

async function safeCtxReply(ctx, text, opts = {}) {
  try { return await ctx.reply(text, opts); }
  catch (e) {
    try {
      const chatId = ctx.chat?.id || ctx.from?.id;
      return await safeSendTelegram(bot, 'sendMessage', [chatId, text, opts]);
    } catch (err) { throw err; }
  }
}

function startHeartbeat(intervalMs = 60_000) {
  try { setLastHeartbeat(new Date().toISOString()); } catch {}
  setInterval(() => { try { setLastHeartbeat(new Date().toISOString()); } catch {} }, intervalMs);
}

bot.start(async (ctx) => {
  ctx.session = {};
  const lang = await resolveUserLang(ctx.from?.id, null, ctx.from?.language_code);
  const isEn = String(lang).split('-')[0] === 'en';
  const greet = isEn ? 'Hello! I am a crypto alert bot.' : 'Привет! Я бот-алерт для крипты.';
  await ctx.reply(`${greet}\n${isEn ? '(Language: English)' : '(Язык: Русский)'}`, getMainMenuSync(ctx.from.id, lang));
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
  await ctx.reply(text, { reply_markup: inline });
});

bot.hears(['🛠️ Техподдержка/пожелания', 'Пожелания/техподдержка', '🛠️ Support/wishes', 'Wishes/Support'], async (ctx) => {
  const lang = await resolveUserLang(ctx.from.id);
  const msg = String(lang).startsWith('en')
    ? "The bot is completely free and has no restrictions. If something is broken or you have ideas — write me. If you want to thank me with a cup of coffee — write to me in private @pirial_mersus"
    : "Бот полностью бесплатен и в нем нет никаких ограничений. Если что-то сломалось или есть идеи — напишите мне. Если хотите отблагодарить меня чашечкой кофе — напишите в личку @pirial_mersus";
  await ctx.reply(msg, getMainMenuSync(ctx.from.id, lang));
});

bot.hears(['➕ Создать алерт', '➕ Create alert'], async (ctx) => {
  try {
    ctx.session = { step: 'symbol' };
    refreshAllTickers().catch(()=>{});
    const lang = await resolveUserLang(ctx.from.id);
    const recent = await getUserRecentSymbols(ctx.from.id);
    const suggest = [...new Set([...recent, ...['BTC','ETH','SOL','BNB','XRP','DOGE']])].slice(0,6).map(s=>({ text: s }));
    const kb = suggest.length ? [suggest, [buildCancelButton(lang)]] : [[buildCancelButton(lang)]];
    await ctx.reply(String(lang).startsWith('en') ? 'Enter symbol (e.g. BTC) or press a button:' : 'Введи символ (например BTC) или нажми кнопку:', { reply_markup: { keyboard: kb, resize_keyboard: true } });
  } catch { ctx.session = {}; await ctx.reply(String((await resolveUserLang(ctx.from.id)).startsWith('en')) ? 'Error starting alert creation.' : 'Ошибка при запуске создания алерта'); }
});

bot.hears(['↩️ Отмена', '↩️ Cancel'], async (ctx) => {
  ctx.session = {};
  const lang = await resolveUserLang(ctx.from.id);
  const isEn = String(lang).startsWith('en');
  await ctx.reply(isEn ? 'Cancelled ✅' : 'Отмена ✅', getMainMenuSync(ctx.from.id, lang));
});

bot.hears(['📋 Мои алерты', '📋 My alerts'], async (ctx) => {
  try {
    try { await bot.telegram.sendChatAction(ctx.chat.id, 'typing'); } catch {}
    const lang = await resolveUserLang(ctx.from.id);
    const { pages } = await renderAlertsList(ctx.from.id, { fast: false, lang });
    const first = pages[0];
    await ctx.reply(mdBoldToHtml(first.text), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: first.buttons } });
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
  const { POPULAR_COINS } = await import('./constants.js');
  const suggest = [...new Set([...recent, ...POPULAR_COINS])].slice(0, 6).map(s => ({ text: s }));
  const kb = suggest.length ? [suggest, [buildCancelButton(lang)]] : [[buildCancelButton(lang)]];
  ctx.session = { liqAwait: true };
  await ctx.reply(msg, { reply_markup: { keyboard: kb, resize_keyboard: true } });
});

bot.hears(['📜 История алертов', '📜 Alerts history'], async (ctx) => {
  const lang = await resolveUserLang(ctx.from.id);
  const isEn = String(lang).startsWith('en');
  try { await ctx.deleteMessage(ctx.message.message_id); } catch {}
  let rm;
  try { rm = await ctx.reply('…', { reply_markup: { remove_keyboard: true } }); } catch {}
  if (rm?.message_id) { try { await ctx.deleteMessage(rm.message_id); } catch {} }
  const header = isEn ? '📜 Alerts history' : '📜 История алертов';
  const inline = {
    inline_keyboard: [
      [{ text: isEn ? 'Old alerts' : 'Старые алерты', callback_data: 'history_old' }],
      [{ text: isEn ? 'Search old alerts' : 'Поиск старых алертов', callback_data: 'history_search' }],
      [{ text: isEn ? '↩️ Back' : '↩️ Назад', callback_data: 'back_to_main' }],
    ]
  };
  await ctx.reply(header, { reply_markup: inline });
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
    } catch { return ctx.reply('Ошибка получения статистики (таймаут или проблема с БД). Попробуйте позже.'); }
    statsCache.count = activeCount;
    statsCache.time = now;
    await ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${activeCount}`);
  } catch { await ctx.reply('Ошибка получения статистики.'); }
}

bot.hears('👥 Количество активных пользователей', async (ctx) => { await handleActiveUsers(ctx); });
bot.hears('👥 Active users', async (ctx) => { await handleActiveUsers(ctx); });

async function handleMotivationRequest(ctx) {
  try {
    const lang = await resolveUserLang(ctx.from?.id, null, ctx.from?.language_code).catch(() => ctx.from?.language_code || 'ru');
    const isEn = String(lang).toLowerCase().startsWith('en');
    try { await ctx.telegram.sendChatAction(ctx.chat.id, 'upload_photo').catch(()=>{}); } catch {}
    const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ });
    const ok = await sendDailyToUser(bot, ctx.from.id, dateStr, { disableNotification: false, forceRefresh: false }).catch(()=>false);
    if (!ok) await ctx.reply(isEn ? '⚠️ Could not send motivation now.' : '⚠️ Не удалось отправить мотивацию сейчас.');
  } catch {
    try { await ctx.reply('⚠️ Внутренняя ошибка при отправке мотивации.'); } catch {}
  }
}

async function handleMarketSnapshotRequest(ctx) {
  try {
    const pref = await resolveUserLang(ctx.from?.id, null, ctx.from?.language_code).catch(() => ctx.from?.language_code || 'ru');
    const isEn = String(pref).toLowerCase().startsWith('en');
    if (isLocked(ctx.from.id)) { try { await ctx.reply(isEn ? '⏳ Already generating the report…' : '⏳ Уже формирую отчёт…'); } catch {} return; }
    lockReport(ctx.from.id, 60000);
    try { await ctx.telegram.sendChatAction(ctx.chat.id, 'typing'); } catch {}
    const typingTimer = startTyping(ctx);
    const state = reportInFlight.get(ctx.from.id);
    if (state) state.typingTimer = typingTimer;
    let startedMsgId = null;
    try {
      const m = await ctx.reply(isEn ? '⏳ Generating the report…' : '⏳ Формирую отчёт…').catch(()=>null);
      if (m?.message_id) startedMsgId = m.message_id;
      if (state) state.startedMsgId = startedMsgId;
    } catch {}
    try {
      const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ });
      const res = await sendMarketReportToUser(bot, ctx.from.id, dateStr).catch(()=>null);
      if (res?.ok) { return; }
      const snap = await getMarketSnapshot(['BTC','ETH']).catch(()=>null);
      if (!snap?.ok) {
        await ctx.reply(isEn ? '⚠️ Не удалось собрать данные.' : '⚠️ Не удалось собрать данные.');
        return;
      }
      const html = await buildMorningReportHtml(snap.snapshots, pref);
      await ctx.reply(html, { parse_mode: 'HTML' });
    } catch (e) {
      try { console.error('[handleMarketSnapshotRequest]', e?.stack || String(e)); } catch {}
      try { await ctx.reply(isEn ? '⚠️ Ошибка при формировании отчёта.' : '⚠️ Ошибка при формировании отчёта.'); } catch {}
    } finally {
      try { if (startedMsgId) { await ctx.deleteMessage(startedMsgId).catch(()=>{}); } } catch {}
      unlockReport(ctx.from.id);
    }
  } catch (e) {
    try { console.error('[handleMarketSnapshotRequest:outer]', e?.stack || String(e)); } catch {}
    try { await ctx.reply('⚠️ Внутренняя ошибка.'); } catch {}
    unlockReport(ctx.from.id);
  }
}

bot.hears(['📈 Краткий отчёт', '📈 Short market report'], async (ctx) => {
  try { await ctx.telegram.sendChatAction(ctx.chat.id, 'typing').catch(()=>{}); } catch {}
  try { await sendShortReportToUser(bot, ctx.from.id); }
  catch (e) { try { await ctx.reply('⚠️ Не удалось сформировать краткий отчёт.'); } catch {} }
});

bot.hears(['📊 Полный отчёт', '📊 Full report', '📊 прислать данные мониторинга', '📊 Send market snapshot', '📊 ⏳ Формирую…', '📊 ⏳ Building…'], handleMarketSnapshotRequest);

bot.hears(['🌅 Прислать мотивацию', '🌅 Send motivation'], handleMotivationRequest);

bot.command('motivate', handleMotivationRequest);
bot.command('market', handleMarketSnapshotRequest);
bot.command('snapshot', handleMarketSnapshotRequest);
bot.command('report', handleMarketSnapshotRequest);

bot.hears(['📜 Старые алерты', '📜 Old alerts'], async (ctx) => {
  ctx.session = { step: 'old_alerts_select_days' };
  const lang = await resolveUserLang(ctx.from.id);
  const isEn = String(lang).startsWith('en');
  const kb = [[{ text: isEn ? '7 days' : '7 дней' }, { text: isEn ? '30 days' : '30 дней' }, { text: isEn ? '90 days' : '90 дней' }], [buildCancelButton(lang)]];
  await ctx.reply(isEn ? 'Choose a period to view old alerts:' : 'Выбери период для просмотра старых алертов:', { reply_markup: { keyboard: kb, resize_keyboard: true } });
});

bot.hears(['🔎 Поиск старых алертов', '🔎 Search old alerts'], async (ctx) => {
  ctx.session = { step: 'old_alerts_search' };
  const lang = await resolveUserLang(ctx.from.id);
  await ctx.reply(String(lang).startsWith('en') ? 'Enter query in format: SYMBOL [DAYS]\nExamples: "BTC", "BTC 30". Default DAYS=30.' : 'Введи запрос в формате: SYMBOL [DAYS]\nПримеры: "BTC", "BTC 30". По умолчанию DAYS=30.', { reply_markup: { keyboard: [[buildCancelButton(lang)]], resize_keyboard: true } });
});

bot.on('callback_query', async (ctx) => {
  try {
    const data = ctx.callbackQuery?.data;
    if (!data) return ctx.answerCbQuery();

    const lang = await resolveUserLang(ctx.from.id);
    if (data === 'market_short') {
      try { await editReportMessageToShort(ctx); } catch { try { await ctx.answerCbQuery('Ошибка'); } catch {} }
      return;
    }

    if (data === 'market_full') {
      try { await editReportMessageToFull(ctx); } catch { try { await ctx.answerCbQuery('Ошибка'); } catch {} }
      return;
    }

    if (data === 'market_help') {
      const mm = await import('./marketMonitor.js');
      try { await mm.editReportMessageWithHelp(ctx); await ctx.answerCbQuery(); } catch { try { await ctx.answerCbQuery('Ошибка'); } catch {} }
      return;
    }

    if (data === 'history_old') {
      ctx.session = { step: 'old_alerts_select_days' };
      const lang2 = await resolveUserLang(ctx.from.id);
      const isEn = String(lang2).startsWith('en');
      const kb = [[{ text: isEn ? '7 days' : '7 дней' }, { text: isEn ? '30 days' : '30 дней' }, { text: isEn ? '90 days' : '90 дней' }], [buildCancelButton(lang2)]];
      await ctx.reply(isEn ? 'Choose a period to view old alerts:' : 'Выбери период для просмотра старых алертов:', { reply_markup: { keyboard: kb, resize_keyboard: true } });
      await ctx.answerCbQuery();
      return;
    }
    if (data === 'history_search') {
      ctx.session = { step: 'old_alerts_search' };
      const lang2 = await resolveUserLang(ctx.from.id);
      await ctx.reply(String(lang2).startsWith('en')
          ? 'Enter query in format: SYMBOL [DAYS]\nExamples: "BTC", "BTC 30". Default DAYS=30.'
          : 'Введи запрос в формате: SYMBOL [DAYS]\nПримеры: "BTC", "BTC 30". По умолчанию DAYS=30.',
        { reply_markup: { keyboard: [[buildCancelButton(lang2)]], resize_keyboard: true } });
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'back_to_main') {
      try { await ctx.editMessageReplyMarkup({ inline_keyboard: [] }); } catch {}
      try {
        const lang2 = await resolveUserLang(ctx.from?.id).catch(() => 'ru');
        await ctx.reply(String(lang2).startsWith('en') ? 'Main menu' : 'Главное меню', getMainMenuSync(ctx.from.id, lang2));
      } catch {}
      try { await ctx.answerCbQuery(); } catch {}
      return;
    }

    if (data === 'toggle_order') {
      const cur = await getUserAlertsOrder(ctx.from.id).catch(()=> 'new_bottom');
      const next = cur === 'new_top' ? 'new_bottom' : 'new_top';
      await setUserAlertsOrder(ctx.from.id, next).catch(()=>{});
      const inline = await buildSettingsInlineForUser(ctx.from.id, lang);
      try { await ctx.editMessageReplyMarkup(inline); } catch {}
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'toggle_lang') {
      const cur = await resolveUserLang(ctx.from.id).catch(()=> 'ru');
      const next = String(cur).startsWith('en') ? 'ru' : 'en';
      try {
        const { usersCollection } = await import('./db.js');
        await usersCollection.updateOne({ userId: ctx.from.id }, { $set: { preferredLang: next } }, { upsert: true });
      } catch {}
      try { await ctx.reply(next === 'en' ? 'Language switched to English.' : 'Я переключился на русский.', getMainMenuSync(ctx.from.id, next)); } catch {}
      const inline = await buildSettingsInlineForUser(ctx.from.id, next);
      try {
        const header = next === 'en'
          ? '⚙️ Settings\n— alerts order\n— language\n— daily motivation\n— morning market report\n\nTap to toggle.'
          : '⚙️ Настройки\n— порядок новых алертов\n— язык сообщений\n— ежедневная мотивация\n— утренний отчёт по рынку\n\nНажимай, чтобы переключить.';
        try { await ctx.editMessageText(header, { reply_markup: inline }); }
        catch { await ctx.editMessageReplyMarkup(inline); }
      } catch {}
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'toggle_motivation') {
      try {
        const { usersCollection } = await import('./db.js');
        const u = await usersCollection.findOne({ userId: ctx.from.id }) || {};
        const next = !(u.sendMotivation !== false);
        await usersCollection.updateOne({ userId: ctx.from.id }, { $set: { sendMotivation: next } }, { upsert: true });
      } catch {}
      const inline = await buildSettingsInlineForUser(ctx.from.id, lang);
      try { await ctx.editMessageReplyMarkup(inline); } catch {}
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'toggle_market') {
      try {
        const { usersCollection } = await import('./db.js');
        const u = await usersCollection.findOne({ userId: ctx.from.id }) || {};
        const next = !(u.sendMarketReport !== false);
        await usersCollection.updateOne({ userId: ctx.from.id }, { $set: { sendMarketReport: next } }, { upsert: true });
      } catch {}
      const inline = await buildSettingsInlineForUser(ctx.from.id, lang);
      try { await ctx.editMessageReplyMarkup(inline); } catch {}
      await ctx.answerCbQuery();
      return;
    }

    const mPage = data.match(/^alerts_page_(\d+)_view$/);
    if (mPage) {
      const pageIdx = parseInt(mPage[1], 10);
      const { pages } = await renderAlertsList(ctx.from.id, { fast: true, lang });
      const page = pages[Math.max(0, Math.min(pageIdx, pages.length - 1))] || pages[0];
      const chatId = ctx.update.callback_query.message.chat.id;
      const msgId  = ctx.update.callback_query.message.message_id;
      try { await ctx.answerCbQuery(); } catch {}
      await editHtmlOrReply(ctx, chatId, msgId, page.text, page.buttons);
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
      catch {
        try { const originalText = ctx.update.callback_query.message?.text || 'Your alerts'; await ctx.reply(originalText, { reply_markup: { inline_keyboard: inline } }); } catch {}
      }
      await ctx.answerCbQuery();
      return;
    }

    const mBack = data.match(/^back_to_alerts(?:_p(\d+))?$/);
    if (mBack) {
      const p = mBack[1] ? parseInt(mBack[1], 10) : 0;
      const { pages } = await renderAlertsList(ctx.from.id, { fast: true, lang });
      const page = pages[Math.max(0, Math.min(p, pages.length - 1))] || pages[0];
      await editHtmlOrReply(ctx, ctx.update.callback_query.message.chat.id, ctx.update.callback_query.message.message_id, page.text, page.buttons);
      try { await ctx.answerCbQuery(); } catch {}
      return;
    }

    const mSet = data.match(/^set_order_(new_top|new_bottom)$/);
    if (mSet) {
      const order = mSet[1];
      await setUserAlertsOrder(ctx.from.id, order).catch(()=>{});
      const inline = await buildSettingsInlineForUser(ctx.from.id, lang);
      try { await ctx.editMessageReplyMarkup(inline); }
      catch { try { await ctx.reply(String(lang).startsWith('en') ? 'Order set' : 'Порядок установлен', { reply_markup: inline }); } catch {} }
      await ctx.answerCbQuery(String(lang).startsWith('en') ? 'Order set' : 'Порядок установлен');
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
        } catch { sourcePage = 0; }
      }

      try {
        const { alertsArchiveCollection } = await import('./db.js');
        await alertsArchiveCollection.insertOne({
          ...doc,
          deletedAt: new Date(),
          deleteReason: 'user_deleted',
          archivedAt: new Date()
        });
      } catch {}

      const { alertsCollection: ac } = await import('./db.js');
      await ac.deleteOne({ _id: new ObjectId(id) });
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
            { parse_mode: 'HTML', reply_markup: { inline_keyboard: [] } }
          );
        } catch {
          try {
            await ctx.reply(String(lang).startsWith('en') ? 'You have no active alerts.' : 'У тебя больше нет активных алертов.', { parse_mode: 'HTML' });
          } catch {}
        }
        await ctx.answerCbQuery(String(lang).startsWith('en') ? 'Alert deleted' : 'Алерт удалён');
        return;
      }

      try { await ctx.editMessageReplyMarkup({ inline_keyboard: inline2 }); }
      catch {
        try {
          const originalText = ctx.update.callback_query.message?.text || (String(lang).startsWith('en') ? 'Your alerts' : 'Твои алерты');
          await ctx.reply(originalText, { reply_markup: { inline_keyboard: inline2 } });
        } catch {}
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
      const opts = { days, symbol: q || null, token, lang };
      const { pages } = await renderOldAlertsList(ctx.from.id, opts);
      const page = pages[Math.max(0, Math.min(pageIdx, pages.length - 1))] || pages[0];

      const chatId = ctx.update.callback_query.message.chat.id;
      const msgId  = ctx.update.callback_query.message.message_id;

      try { await ctx.answerCbQuery(); } catch {}
      await editHtmlOrReply(ctx, chatId, msgId, page.text, page.buttons);
      return;
    }

    const mSetLang = data.match(/^set_lang_(ru|en)$/);
    if (mSetLang) {
      const newLang = mSetLang[1];
      try {
        const { usersCollection } = await import('./db.js');
        await usersCollection.updateOne({ userId: ctx.from.id }, { $set: { preferredLang: newLang } }, { upsert: true });
        await ctx.reply(newLang === 'en' ? 'Language switched to English.' : 'Я переключился на русский.', getMainMenuSync(ctx.from.id, newLang));
        const inline = await buildSettingsInlineForUser(ctx.from.id, newLang);
        try { await ctx.editMessageReplyMarkup(inline); } catch {}
      } catch {}
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'clear_old_alerts_confirm') {
      const isEn = String(lang).split('-')[0] === 'en';
      const text = isEn ? 'Are you sure?' : 'Вы уверены?';
      const inline = { inline_keyboard: [[{ text: isEn ? 'Yes' : 'Да', callback_data: 'clear_old_alerts_yes' }, { text: isEn ? 'No' : 'Нет', callback_data: 'clear_old_alerts_no' }]] };
      try { await ctx.editMessageText(text, { reply_markup: inline }); } catch { try { await ctx.reply(text, { reply_markup: inline }); } catch {} }
      await ctx.answerCbQuery();
      return;
    }

    if (data === 'clear_old_alerts_no') {
      try { await ctx.editMessageReplyMarkup({ inline_keyboard: [] }); } catch {}
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
        try { await ctx.editMessageText(msg, { reply_markup: { inline_keyboard: [] } }); } catch { try { await ctx.reply(msg); } catch {} }
      } catch {
        try { await ctx.answerCbQuery('Error'); } catch {}
      }
      await ctx.answerCbQuery();
      return;
    }

    await ctx.answerCbQuery();
  } catch {
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
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
    await ctx.replyWithPhoto(fileId, { ...menu, caption: `${header} — ${pairLabel}\n\n${explain}${timeLine}`, parse_mode: 'HTML' });

    try { await ctx.deleteMessage(loading.message_id); } catch {}
    try { await pushRecentSymbol(ctx.from.id, pairLabel); } catch {}

    try { ctx.session.liqAwait = false; } catch {}
  } catch (e) {
    try { ctx.session.liqAwait = true; } catch {}
    try { ctx.session.step = null; } catch {}

    const lang2 = await resolveUserLang(ctx.from.id);
    const isEn2 = String(lang2).startsWith('en');
    const recent = await getUserRecentSymbols(ctx.from.id).catch(() => []);
    const { POPULAR_COINS } = await import('./constants.js');
    const suggestRow = [...new Set([...recent, ...POPULAR_COINS])].slice(0, 6).map(s => ({ text: s }));
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
    const textRaw = (ctx.message.text || '').trim();
    const text = textRaw;

    const daysMap = { '7 дней': 7, '30 дней': 30, '90 дней': 90, '7 days': 7, '30 days': 30, '90 days': 90 };
    const numeric = parseInt(text.replace(/\D/g, ''), 10);
    const isNumericDay = Number.isFinite(numeric) && [7, 30, 90].includes(numeric);
    const normalized = text.toLowerCase();
    const isDaysPhrase = daysMap[text] || daysMap[normalized] || isNumericDay || /^\d+\s*дн/i.test(text) || /^\d+\s*day/i.test(text);
    if ((!step || step === 'old_alerts_select_days') && isDaysPhrase) {
      const days = daysMap[text] || daysMap[normalized] || (isNumericDay ? numeric : 30);
      const token = `d${days}_q`;
      const lang = await resolveUserLang(ctx.from.id);
      const { pages } = await renderOldAlertsList(ctx.from.id, { days, symbol: null, token, lang });
      const first = pages[0];
      ctx.session = {};
      if (first.buttons && first.buttons.length) { await ctx.reply(mdBoldToHtml(first.text), { parse_mode: 'HTML', reply_markup: { inline_keyboard: first.buttons } }); }
      else { await ctx.reply(mdBoldToHtml(first.text), { parse_mode: 'HTML', ...getMainMenuSync(ctx.from.id, lang) }); }
      return;
    }

    if (!step && /^[A-Z0-9]{2,10}$/i.test(text)) ctx.session = { step: 'symbol' };
    if (!ctx.session.step) return;

    if (ctx.session.step === 'symbol') {
      const base = text.toUpperCase();
      const symbol = `${base}-USDT`;
      const price = await getCachedPrice(symbol);
      if (Number.isFinite(price)) {
        try { await pushRecentSymbol(ctx.from.id, base); } catch {}
        ctx.session.symbol = symbol;
        ctx.session.step = 'alert_condition';
        const lang = await resolveUserLang(ctx.from.id);

        // ⬇️ многострочное HTML-сообщение с жирным тикером и ценой
        const isEn = String(lang).startsWith('en');
        const html = isEn
          ? `✅ Coin: <b>${symbol}</b>\nCurrent price: <b>${fmtNum(price)}</b>\nChoose direction: 👇`
          : `✅ Монета: <b>${symbol}</b>\nТекущая цена: <b>${fmtNum(price)}</b>\nВыбери направление: 👇`;
        await ctx.reply(html, { parse_mode: 'HTML', reply_markup: buildDirectionKeyboard(lang), disable_web_page_preview: true });
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
      else { await ctx.reply(String(lang).startsWith('en') ? 'Choose ⬆️ or ⬇️' : 'Выбери ⬆️ или ⬇️'); return; }
      ctx.session.step = 'alert_price';
      await ctx.reply(String(lang).startsWith('en') ? 'Enter alert price:' : 'Введи цену уведомления:', { reply_markup: { keyboard: [[buildCancelButton(lang)]], resize_keyboard:true } });
      return;
    }

    if (ctx.session.step === 'alert_price') {
      const v = parseFloat(text);
      if (!Number.isFinite(v)) { await ctx.reply('Введите корректное число'); return; }
      ctx.session.alertPrice = v;
      ctx.session.step = 'ask_sl';
      const lang = await resolveUserLang(ctx.from.id);
      const hint = ctx.session.alertCondition === '>' ? (String(lang).startsWith('en') ? 'SL will be higher (for short — reverse)' : 'SL будет выше (для шорта — логика обратная)') : (String(lang).startsWith('en') ? 'SL will be lower' : 'SL будет ниже');
      await ctx.reply((String(lang).startsWith('en') ? 'Add stop-loss?' : 'Добавить стоп-лосс?') + ` ${hint}`, { reply_markup: buildAskSlKeyboard(lang) });
      return;
    }

    if (ctx.session.step === 'ask_sl') {
      const { alertsCollection } = await import('./db.js');
      const limit = await getUserAlertLimit(ctx.from.id).catch(()=>1000000000);
      let currentCount = 0;
      try { currentCount = await alertsCollection.countDocuments({ userId: ctx.from.id }); }
      catch { const currentAlerts = await getUserAlertsCached(ctx.from.id).catch(()=>[]); currentCount = (currentAlerts?.length || 0); }

      if (currentCount >= limit) {
        const lang = await resolveUserLang(ctx.from.id);
        await ctx.reply(String(lang).startsWith('en') ? `You already have ${currentCount} alerts — limit ${limit}. Contact @pirial_gena to increase.` : `У тебя уже ${currentCount} алертов — достигнут лимит ${limit}. Если нужно увеличить лимит, напиши мне: @pirial_genа`, getMainMenuSync(ctx.from.id, lang));
        ctx.session = {};
        return;
      }

      const lang = await resolveUserLang(ctx.from.id);
      if (text === (String(lang).startsWith('en') ? '⏭️ Skip SL' : '⏭️ Без SL')) {
        try {
          const { alertsCollection: ac } = await import('./db.js');
          const beforeInsertCount = await ac.countDocuments({ userId: ctx.from.id }).catch(()=>currentCount);
          if (beforeInsertCount >= limit) {
            await ctx.reply(String(lang).startsWith('en') ? `You already have ${beforeInsertCount} alerts — limit ${limit}.` : `У тебя уже ${beforeInsertCount} алертов — достигнут лимит ${limit}. Если нужно увеличить лимит, напиши мне: @pirial_gena`, getMainMenuSync(ctx.from.id, lang));
            ctx.session = {};
            return;
          }

          await ac.insertOne({ userId: ctx.from.id, symbol: ctx.session.symbol, condition: ctx.session.alertCondition, price: ctx.session.alertPrice, type: 'alert', createdAt: new Date() });
          invalidateUserAlertsCache(ctx.from.id);
          const cp = await getCachedPrice(ctx.session.symbol);
          const isEn = String(lang).startsWith('en');
          const conditionLine = ctx.session.alertCondition === '>' ? (isEn ? '⬆️ when above' : '⬆️ выше') : (isEn ? '⬇️ when below' : '⬇️ ниже');

          // ⬇️ жирный тикер, жирная целевая, жирная текущая
          const msg = isEn
            ? `✅ Alert created:\n🔔 <b>${ctx.session.symbol}</b>\n${conditionLine} <b>${fmtNum(ctx.session.alertPrice)}</b>\nCurrent: <b>${fmtNum(cp) ?? '—'}</b>`
            : `✅ Алерт создан:\n🔔 <b>${ctx.session.symbol}</b>\n${conditionLine} <b>${fmtNum(ctx.session.alertPrice)}</b>\nТекущая: <b>${fmtNum(cp) ?? '—'}</b>`;

          await ctx.reply(msg, { ...getMainMenuSync(ctx.from.id, lang), parse_mode: 'HTML', disable_web_page_preview: true });
        } catch { await ctx.reply('Ошибка при создании алерта'); }
        ctx.session = {};
        return;
      }
      if (text === (String(lang).startsWith('en') ? '🛑 Add SL' : '🛑 Добавить SL')) {
        ctx.session.step = 'sl_price';
        await ctx.reply(String(lang).startsWith('en') ? 'Enter stop-loss price:' : 'Введи цену стоп-лосса:', { reply_markup: { keyboard: [[buildCancelButton(lang)]], resize_keyboard:true } });
        return;
      }
      await ctx.reply(String(lang).startsWith('en') ? 'Choose: 🛑 Add SL / ⏭️ Skip SL' : 'Выбери опцию: 🛑 Добавить SL / ⏭️ Без SL');
      return;
    }

    if (ctx.session.step === 'sl_price') {
      const sl = parseFloat(text);
      if (!Number.isFinite(sl)) { await ctx.reply('Введите корректное число SL'); return; }

      const { alertsCollection } = await import('./db.js');
      const limit = await getUserAlertLimit(ctx.from.id).catch(()=>1000000000);
      let currentCount = 0;
      try { currentCount = await alertsCollection.countDocuments({ userId: ctx.from.id }); }
      catch { const currentAlerts = await getUserAlertsCached(ctx.from.id).catch(()=>[]); currentCount = (currentAlerts?.length || 0); }

      if (currentCount + 2 > limit) {
        const lang = await resolveUserLang(ctx.from.id);
        await ctx.reply(String(lang).startsWith('en') ? `Can't create pair (alert + SL). You have ${currentCount} alerts, limit ${limit}.` : `Нельзя создать связку (уведомление + SL). У тебя сейчас ${currentCount} алертов, лимит ${limit}. Чтобы увеличить лимит напиши: @pirial_genа`, getMainMenuSync(ctx.from.id, lang));
        ctx.session = {};
        return;
      }

      try {
        const groupId = new ObjectId().toString();
        const beforeInsertCount = await alertsCollection.countDocuments({ userId: ctx.from.id }).catch(()=>currentCount);
        if (beforeInsertCount + 2 > limit) {
          const lang = await resolveUserLang(ctx.from.id);
          await ctx.reply(String(lang).startsWith('en') ? `Can't create pair (alert + SL). You have ${beforeInsertCount} alerts, limit ${limit}.` : `Нельзя создать связку (уведомление + SL). У тебя сейчас ${beforeInsertCount} алертов, лимит ${limit}. Чтобы увеличить лимит напиши: @pirial_genа`, getMainMenuSync(ctx.from.id, lang));
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
        const isEn = String(lang).startsWith('en');

        // ⬇️ жирные тикер/цены
        const slLine = isEn ? `🛑 SL (${slDir}) <b>${fmtNum(sl)}</b>` : `🛑 SL (${slDir}) <b>${fmtNum(sl)}</b>`;
        const msg = isEn
          ? `✅ Pair created:\n🔔 <b>${ctx.session.symbol}</b>\n${ctx.session.alertCondition === '>' ? '⬆️ when above' : '⬇️ when below'} <b>${fmtNum(ctx.session.alertPrice)}</b>\n${slLine}\nCurrent: <b>${fmtNum(cp) ?? '—'}</b>`
          : `✅ Создана связка:\n🔔 <b>${ctx.session.symbol}</b>\n${ctx.session.alertCondition === '>' ? '⬆️ выше' : '⬇️ ниже'} <b>${fmtNum(ctx.session.alertPrice)}</b>\n${slLine}\nТекущая: <b>${fmtNum(cp) ?? '—'}</b>`;

        await ctx.reply(msg, { ...getMainMenuSync(ctx.from.id, lang), parse_mode: 'HTML', disable_web_page_preview: true });
      } catch { await ctx.reply('Ошибка при создании связки'); }
      ctx.session = {};
      return;
    }

    if (ctx.session.step === 'old_alerts_select_days') {
      if (text === '↩️ Отмена' || text === '↩️ Cancel') { ctx.session = {}; const lang = await resolveUserLang(ctx.from.id); await ctx.reply('Отмена', getMainMenuSync(ctx.from.id, lang)); return; }
      const daysMapLocal = { '7 дней': 7, '30 дней': 30, '90 дней': 90 };
      const days = daysMapLocal[text] || parseInt(text, 10) || 30;
      const token = `d${days}_q`;
      const lang = await resolveUserLang(ctx.from.id);
      const { pages } = await renderOldAlertsList(ctx.from.id, { days, symbol: null, token, lang });
      const first = pages[0];
      ctx.session = {};
      if (first.buttons && first.buttons.length) { await ctx.reply(mdBoldToHtml(first.text), { parse_mode: 'HTML', reply_markup: { inline_keyboard: first.buttons } }); }
      else { await ctx.reply(mdBoldToHtml(first.text), { parse_mode: 'HTML', ...getMainMenuSync(ctx.from.id, lang) }); }
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
      if (first.buttons && first.buttons.length) { await ctx.reply(mdBoldToHtml(first.text), { parse_mode: 'HTML', reply_markup: { inline_keyboard: first.buttons } }); }
      else { await ctx.reply(mdBoldToHtml(first.text), { parse_mode: 'HTML', ...getMainMenuSync(ctx.from.id, lang) }); }
      return;
    }

  } catch {
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
      } catch {
        return ctx.reply('Не удалось найти пользователя по идентификатору/имени.');
      }
    }

    const newLim = await setUserAlertLimit(targetId, lim);
    if (newLim === null) return ctx.reply('Ошибка при установке лимита.');
    await ctx.reply(`Лимит для пользователя ${targetId} установлен: ${newLim}`);
    try { await bot.telegram.sendMessage(targetId, `Тебе установлен лимит алертов: ${newLim} (вручную от администратора)`); } catch {}
  } catch { await ctx.reply('Ошибка при выполнении команды'); }
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
      } catch {
        return ctx.reply('Не удалось найти пользователя по идентификатору/имени.');
      }
    }
    const lim = await getUserAlertLimit(targetId);
    await ctx.reply(`Лимит для пользователя ${targetId}: ${lim}`);
  } catch { await ctx.reply('Ошибка при выполнении команды'); }
});

bot.command('refresh_daily', async (ctx) => {
  try {
    if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) return ctx.reply('У вас нет доступа.');
    const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ });
    await ctx.reply(`⏳ Начинаю принудительное обновление мотивации на ${dateStr}...`);

    try {
      const cacheMod = await import('./cache.js');
      if (cacheMod && cacheMod.dailyCache) {
        cacheMod.dailyCache.date = null;
        cacheMod.dailyCache.doc = null;
        cacheMod.dailyCache.imageBuffer = null;
        await ctx.reply('Кэш dailyCache очищен.');
      }
    } catch { await ctx.reply('⚠️ Не удалось очистить кэш в памяти (см логи). Продолжаю.'); }

    const daily = await import('./daily.js');
    const { dailyMotivationCollection } = await import('./db.js');

    try {
      const previewQuote = await daily.fetchQuoteFromAny();
      if (previewQuote && previewQuote.text) { await ctx.reply(`Превью цитаты:\n${previewQuote.text}${previewQuote.author ? `\n— ${previewQuote.author}` : ''}`); }
      else { await ctx.reply('Превью цитаты: не удалось загрузить новую цитату (источники вернули пусто).'); }
    } catch (e) { await ctx.reply(`Ошибка при получении превью цитаты: ${String(e?.message || e)}`); }

    try {
      if (typeof daily.fetchRandomImage === 'function') {
        const img = await daily.fetchRandomImage();
        if (img?.url) { await ctx.reply(`Превью картинки: ${img.url} (${img.source || 'unknown'})`); }
        else { await ctx.reply('Превью картинки: не удалось получить картинку из источников.'); }
      } else { await ctx.reply('Превью картинки: функция fetchRandomImage недоступна.'); }
    } catch (e) { await ctx.reply(`Ошибка при получении превью картинки: ${String(e?.message || e)}`); }

    try {
      const stored = await daily.fetchAndStoreDailyMotivation(dateStr, { force: true });
      await ctx.reply(stored ? '✅ Цитата и метаданные сохранены в БД (force).' : '⚠️ fetchAndStoreDailyMotivation вернул null/undefined.');
    } catch (e) { await ctx.reply(`Ошибка при сохранении мотивации: ${String(e?.message || e)}`); }

    try {
      const doc = await dailyMotivationCollection.findOne({ date: dateStr });
      if (doc) {
        const q = doc.quote?.original || doc.quote?.translations?.ru || null;
        await ctx.reply(`Текущий документ в БД:\nЦитата: ${q ? q : '—'}\nАвтор: ${doc.quote?.author || '—'}\nImage URL: ${doc.image?.url || '—'}`);
      } else { await ctx.reply('В БД нет документа для сегодняшней даты после сохранения.'); }
    } catch (e) { await ctx.reply(`Ошибка при чтении doc из БД: ${String(e?.message || e)}`); }

    try {
      const buf = await ensureDailyImageBuffer(dateStr);
      await ctx.reply(buf?.length ? `Картинка загружена в память, размер ${buf.length} байт.` : 'Картинка не загружена (будет отправлён текст без изображения).');
    } catch (e) { await ctx.reply(`Ошибка при загрузке изображения: ${String(e?.message || e)}`); }

    try {
      const ok = await daily.sendDailyToUser(bot, ctx.from.id, dateStr, { disableNotification: false, forceRefresh: true });
      await ctx.reply(ok ? 'Готово — мотивация обновлена и отправлена тебе.' : 'Мотивация сохранена, но отправка не удалась (см логи).');
    } catch (e) { await ctx.reply(`Ошибка при отправке мотивации: ${String(e?.message || e)}`); }

  } catch (e) { await ctx.reply('Внутренняя ошибка: ' + String(e?.message || e)); }
});

bot.command('broadcast_market_now', async (ctx) => {
  if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) return ctx.reply('У вас нет доступа.');
  await ctx.reply('Запускаю рассылку мониторинга (тест)...');
  try {
    const res = await broadcastMarketSnapshot(bot, { batchSize: MARKET_BATCH_SIZE, pauseMs: MARKET_BATCH_PAUSE_MS });
    await ctx.reply('Done: ' + JSON.stringify(res));
  } catch (e) { await ctx.reply('Ошибка при рассылке: ' + String(e?.message || e)); }
});

export async function startBot() {
  await connectToMongo();
  startTickersRefresher();

  if (isDbConnected()) { try { startAlertsChecker(bot); } catch {} }
  else {
    const tryStartChecker = setInterval(() => {
      if (isDbConnected()) { try { startAlertsChecker(bot); } catch {} clearInterval(tryStartChecker); }
    }, 10000);
  }

  try { if (typeof startMarketMonitor === 'function') startMarketMonitor(bot); } catch {}

  await removeInactive();
  startHeartbeat(60000);

  const app = createServer();
  const PORT = process.env.PORT || 3000;
  const server = app.listen(PORT, () => console.log(`HTTP server on ${PORT}`));

  setInterval(() => processDailyQuoteRetry(bot), 60000);
  setInterval(() => watchForNewQuotes(bot), 30000);

  const dateStrNow = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ });
  try { await fetchAndStoreDailyMotivation(dateStrNow).catch(()=>{}); } catch {}

  let lastFetchDay = null;
  let lastPrepareDay = null;
  let lastMarketSendDay = null;

  setInterval(async () => {
    try {
      const kyivNow = new Date(new Date().toLocaleString('en-US', { timeZone: KYIV_TZ }));
      const day = kyivNow.toLocaleDateString('sv-SE');
      const hour = kyivNow.getHours();

      if (day !== lastFetchDay && hour === IMAGE_FETCH_HOUR) { try { await fetchAndStoreDailyMotivation(day, { force: true }); } catch {} lastFetchDay = day; }

      if (day !== lastPrepareDay && hour === PREPARE_SEND_HOUR) {
        try { await fetchAndStoreDailyMotivation(day, { force: false }); } catch {}
        lastPrepareDay = day;

        try {
          const dateStr = day;
          const { usersCollection, pendingDailySendsCollection } = await import('./db.js');
          const already = await pendingDailySendsCollection.find({ date: dateStr, sent: true }, { projection: { userId: 1 } }).toArray();
          const sentSet = new Set((already || []).map(r => r.userId));
          const cursor = usersCollection.find(
            { $or: [{ botBlocked: { $exists: false } }, { botBlocked: false }], sendMotivation: { $ne: false } },
            { projection: { userId: 1 } }
          );
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
                } catch {}
              }));
              batch = [];
            }
          }
          if (batch.length) {
            await Promise.all(batch.map(async (targetId) => {
              try {
                const ok = await sendDailyToUser(bot, targetId, dateStr, { disableNotification: false, forceRefresh: false }).catch(()=>false);
                await pendingDailySendsCollection.updateOne({ userId: targetId, date: dateStr }, { $set: { sent: !!ok, sentAt: ok ? new Date() : null, quoteSent: !!ok, permanentFail: !ok } }, { upsert: true });
              } catch {}
            }));
          }
        } catch {}
      }

      if (day !== lastMarketSendDay && hour === (MARKET_SEND_HOUR ?? 7) && kyivNow.getMinutes() === (MARKET_SEND_MIN ?? 30)) {
        try {
          if (typeof broadcastMarketSnapshot === 'function') {
            await broadcastMarketSnapshot(bot, { batchSize: MARKET_BATCH_SIZE, pauseMs: MARKET_BATCH_PAUSE_MS }).catch(()=>{});
            lastMarketSendDay = day;
          }
        } catch {}
      }
    } catch {}
  }, 60000);

  setInterval(async () => { try { await removeInactive(); } catch {} }, 7 * DAY_MS);

  await bot.launch();
  return { server };
}
