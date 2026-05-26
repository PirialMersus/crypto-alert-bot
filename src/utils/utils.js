// src/utils/utils.js
import {getUserAlertsOrder, resolveUserLang, statsCache} from "../cache.js";
import {
  CACHE_TTL, CREATOR_ID,
  DAY_MS,
  IMAGE_FETCH_HOUR,
  INACTIVE_DAYS,
  KYIV_TZ, MARKET_BATCH_PAUSE_MS, MARKET_BATCH_SIZE,
  MARKET_SEND_HOUR, MARKET_SEND_MIN,
  PREPARE_SEND_HOUR
} from "../constants.js";
import {
  broadcastMarketSnapshot,
  buildMorningReportHtml, buildMorningReportParts,
  getMarketSnapshot,
  sendMarketReportToUser
} from "./marketMonitor.js";
import {bot} from "../bot.js";
import {setLastHeartbeat} from "../monitor.js";
import {connectToMongo, countDocumentsWithTimeout, isDbConnected} from "../db/db.js";
import {fetchAndStoreDailyMotivation, processDailyQuoteRetry, sendDailyToUser, watchForNewQuotes} from "../daily.js";
import {startTickersRefresher} from "../prices.js";
import {startAlertsChecker} from "../alerts.js";
import {removeInactive} from "../cleanup.js";
import {createServer} from "../server.js";

export function fmtNum(n) {
  if (!Number.isFinite(n)) return '—';
  if (n >= 1000 || n === Math.floor(n)) return String(Math.round(n));
  if (n >= 1) return n.toFixed(4).replace(/\.?0+$/, '');
  return n.toPrecision(6).replace(/\.?0+$/, '');
}
export function formatChangeWithIcons(change) {
  const sign = change >= 0 ? '+' : '';
  const value = `${sign}${change.toFixed(2)}%`;
  if (change > 0) return `${value} 📈`;
  if (change < 0) return `${value} 📉`;
  return `${value}`;
}
export function padLabel(text, targetLen = 30) {
  const cur = String(text);
  if (cur.length >= targetLen) return cur;
  const needed = targetLen - cur.length;
  return cur + '\u00A0'.repeat(needed);
}
export async function buildWish() { return 'Хорошего дня!'; }

export function splitMessage(text, maxLen = 3500) {
  const chunks = [];
  let rest = String(text || '');
  while (rest.length > maxLen) {
    let idx = rest.lastIndexOf('\n', maxLen);
    if (idx <= 0) idx = maxLen;
    chunks.push(rest.slice(0, idx));
    rest = rest.slice(idx);
  }
  if (rest.length) chunks.push(rest);
  return chunks;
}
export function geminiToHtml(s) {
  let t = String(s || '');

  // 1) Убираем маркеры списков в начале строки (*, -, +)
  t = t.replace(/^\s*[\*\-\+]\s+/gm, '');

  // 2) Экранируем спецсимволы HTML
  t = t
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');

  // 3) Заголовки вида "# ...", "## ...", ..., "###### ..."
  t = t.replace(/^#{1,6}\s+(.+)$/gm, '<b>$1</b>');

  // 4) Жирный markdown: **text** или __text__
  t = t
    .replace(/\*\*(.+?)\*\*/g, '<b>$1</b>')
    .replace(/__(.+?)__/g, '<b>$1</b>');

  return t;
}

export function mdBoldToHtml(s) {
  return String(s)
    .replace(/\*\*(.+?)\*\*/g, '<b>$1</b>')
    .replace(/__(.+?)__/g, '<b>$1</b>');
}

export async function editHtmlOrReply(ctx, chatId, msgId, text, buttons) {
  const html = mdBoldToHtml(text);
  try {
    await ctx.telegram.editMessageText(
      chatId,
      msgId,
      undefined,
      html,
      {
        parse_mode: 'Markdown',
        reply_markup: buttons ? {inline_keyboard: buttons} : undefined,
        disable_web_page_preview: true
      }
    );
  } catch {
    await ctx.reply(html, {
      parse_mode: 'HTML',
      reply_markup: buttons ? {inline_keyboard: buttons} : undefined,
      disable_web_page_preview: true
    });
  }
}

export async function handleMarketSnapshotRequest(ctx) {
  try {
    const pref = await resolveUserLang(ctx.from?.id, null, ctx.from?.language_code).catch(() => ctx.from?.language_code || 'ru');
    const isEn = String(pref).toLowerCase().startsWith('en');
    if (isLocked(ctx.from.id)) {
      try {
        await ctx.reply(isEn ? '⏳ Already generating the report…' : '⏳ Уже формирую отчёт…');
      } catch {
      }
      return;
    }
    lockReport(ctx.from.id, 60000);
    try {
      await ctx.telegram.sendChatAction(ctx.chat.id, 'typing');
    } catch {
    }
    const stop = startTyping(ctx)
    const state = reportInFlight.get(ctx.from.id)
    if (state) state.stopTyping = stop
    let startedMsgId = null;
    try {
      const m = await ctx.reply(isEn ? '⏳ Generating the report…' : '⏳ Формирую отчёт…').catch(() => null);
      if (m?.message_id) startedMsgId = m.message_id;
      if (state) state.startedMsgId = startedMsgId;
    } catch {
    }
    try {
      const dateStr = new Date().toLocaleDateString('sv-SE', {timeZone: KYIV_TZ});
      const res = await sendMarketReportToUser(bot, ctx.from.id, dateStr).catch(() => null);
      if (res?.ok) {
        return;
      }
      const snap = await getMarketSnapshot(['BTC', 'ETH']).catch(() => null);
      if (!snap?.ok) {
        await ctx.reply(isEn ? '⚠️ Не удалось собрать данные.' : '⚠️ Не удалось собрать данные.');
        return;
      }
      const html = await buildMorningReportHtml(snap.snapshots, pref);
      await ctx.reply(html, {parse_mode: 'HTML'});
    } catch (e) {
      try {
        console.error('[handleMarketSnapshotRequest]', e?.stack || String(e));
      } catch {
      }
      try {
        await ctx.reply(isEn ? '⚠️ Ошибка при формировании отчёта.' : '⚠️ Ошибка при формировании отчёта.');
      } catch {
      }
    } finally {
      try {
        if (startedMsgId) {
          await ctx.deleteMessage(startedMsgId).catch(() => {
          });
        }
      } catch {
      }
      unlockReport(ctx.from.id);
    }
  } catch (e) {
    try {
      console.error('[handleMarketSnapshotRequest:outer]', e?.stack || String(e));
    } catch {
    }
    try {
      await ctx.reply('⚠️ Внутренняя ошибка.');
    } catch {
    }
    unlockReport(ctx.from.id);
  }
}

export async function buildSettingsInlineForUser(userId, langOverride = null) {
  const order = await getUserAlertsOrder(userId).catch(() => 'new_bottom');
  const lang = langOverride || await resolveUserLang(userId).catch(() => 'ru');
  const isEn = String(lang).split('-')[0] === 'en';
  const isTop = order === 'new_top';
  let sendMotivation = true;
  let sendMarketReport = true;
  try {
    const {usersCollection} = await import('../db/db.js');
    const u = await usersCollection.findOne({userId});
    if (typeof u?.sendMotivation === 'boolean') sendMotivation = u.sendMotivation;
    if (typeof u?.sendMarketReport === 'boolean') sendMarketReport = u.sendMarketReport;
  } catch {
  }
  const kb = [
    [{text: (isEn ? 'New: ' : 'Новые: ') + (isTop ? '↑' : '↓'), callback_data: 'toggle_order'}],
    [{text: '🌐 ' + (isEn ? 'Language: English' : 'Язык: Русский'), callback_data: 'toggle_lang'}],
    [{
      text: `🌅 ${isEn ? 'Motivation' : 'Мотивация'}: ${sendMotivation ? '✅' : '🚫'}`,
      callback_data: 'toggle_motivation'
    }],
    [{text: `📊 ${isEn ? 'Report' : 'Отчёт'}: ${sendMarketReport ? '✅' : '🚫'}`, callback_data: 'toggle_market'}],
    [{text: isEn ? '↩️ Back' : '↩️ Назад', callback_data: 'back_to_main'}]
  ];
  return {inline_keyboard: kb};
}

export function buildCancelButton(lang) {
  return String(lang).startsWith('en') ? {text: '↩️ Cancel'} : {text: '↩️ Отмена'};
}

export function buildDirectionKeyboard(lang) {
  const isEn = String(lang).startsWith('en');
  return {
    keyboard: [[{text: isEn ? '⬆️ When above' : '⬆️ Когда выше'}, {text: isEn ? '⬇️ When below' : '⬇️ Когда ниже'}], [buildCancelButton(lang)]],
    resize_keyboard: true
  };
}

export function buildAskSlKeyboard(lang) {
  const isEn = String(lang).startsWith('en');
  return {
    keyboard: [[{text: isEn ? '🛑 Add SL' : '🛑 Добавить SL'}, {text: isEn ? '⏭️ Skip SL' : '⏭️ Без SL'}], [buildCancelButton(lang)]],
    resize_keyboard: true
  };
}

export function startHeartbeat(intervalMs = 60_000) {
  try {
    setLastHeartbeat(new Date().toISOString());
  } catch {
  }
  setInterval(() => {
    try {
      setLastHeartbeat(new Date().toISOString());
    } catch {
    }
  }, intervalMs);
}

const reportInFlight = new Map();



export function startTyping(ctx) {
  let stopped = false

  const send = () => {
    if (stopped) return
    try {
      ctx.telegram.sendChatAction(ctx.chat.id, 'typing').catch(() => {})
    } catch {}
  }

  send()
  const timer = setInterval(send, 5100)

  return () => {
    if (stopped) return
    stopped = true
    clearInterval(timer)
  }
}

function lockReport(userId, ms = 30000) {
  reportInFlight.set(userId, {
    until: Date.now() + ms,
    stopTyping: null,
    startedMsgId: null
  });
}

function unlockReport(userId) {
  const s = reportInFlight.get(userId);
  if (typeof s?.stopTyping === 'function') {
    s.stopTyping();
  }
  reportInFlight.delete(userId);
}


function isLocked(userId) {
  const s = reportInFlight.get(userId);
  if (!s) return false;
  if (Date.now() > s.until) {
    unlockReport(userId);
    return false;
  }
  return true;
}

function supportText(isEn) {
  return isEn ? '🛠️ Support/wishes' : '🛠️ Техподдержка/пожелания';
}

export function getMainMenuSync(userId, lang = 'ru') {
  const isEn = String(lang).split('-')[0] === 'en';
  const create = isEn ? '➕ Create alert' : '➕ Создать';
  const my = isEn ? '📋 My alerts' : '📋 Мои уведомления';
  const shortBtn = isEn ? '📈 Short market report' : '📈 Краткий отчёт';
  const fullBtn = isEn ? '📊 Full report' : '📊 Полный отчёт';
  const history = isEn ? '🔮 Surprise me' : '🔮 Удиви меня';
  const liqBtn = isEn ? '🗺️ Liquidation maps' : '🗺️ Карты ликвидаций';
  const settings = isEn ? '⚙️ Settings' : '⚙️ Настройки';
  const motivate = isEn ? '🌅 Send motivation' : '🌅 Прислать мотивацию';
  const stats = isEn ? '👥 Active users' : '👥 Количество активных пользователей';

  const kb = [
    [{text: create}, {text: my}],
    [{text: shortBtn}, {text: fullBtn}],
    [{text: liqBtn}, {text: history}],
    [{text: supportText(isEn)}, {text: settings}],
  ];
  if (CREATOR_ID && String(userId) === String(CREATOR_ID)) {
    kb.push([{text: motivate}, {text: stats}]);
  }
  return {reply_markup: {keyboard: kb, resize_keyboard: true}};
}

export async function handleActiveUsers(ctx) {
  try {
    if (!CREATOR_ID || String(ctx.from.id) !== String(CREATOR_ID)) {
      return ctx.reply('У вас нет доступа к этой команде.');
    }
    const now = Date.now();
    if (statsCache.count !== null && (now - statsCache.time) < CACHE_TTL) {
      return ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${statsCache.count}`);
    }
    const cutoff = new Date(Date.now() - INACTIVE_DAYS * DAY_MS);
    let activeCount;
    try {
      activeCount = await countDocumentsWithTimeout('users', {
        lastActive: {$gte: cutoff},
        $or: [{botBlocked: {$exists: false}}, {botBlocked: false}]
      }, 7000);
    } catch {
      return ctx.reply('Ошибка получения статистики (таймаут или проблема с БД). Попробуйте позже.');
    }
    statsCache.count = activeCount;
    statsCache.time = now;
    await ctx.reply(`👥 Активных пользователей за последние ${INACTIVE_DAYS} дней: ${activeCount}`);
  } catch {
    await ctx.reply('Ошибка получения статистики.');
  }
}
export async function handleMotivationRequest(ctx) {
  try {
    const lang = await resolveUserLang(ctx.from?.id, null, ctx.from?.language_code).catch(() => ctx.from?.language_code || 'ru');
    const isEn = String(lang).toLowerCase().startsWith('en');
    try {
      await ctx.telegram.sendChatAction(ctx.chat.id, 'upload_photo').catch(() => {});
    } catch {}

    const dateStr = new Date().toLocaleDateString('sv-SE', {timeZone: KYIV_TZ});
    
    await fetchAndStoreDailyMotivation(dateStr, { force: true }).catch(() => {});
    
    const ok = await sendDailyToUser(bot, ctx.chat.id, dateStr, { forceRefresh: false, disableNotification: false }).catch(() => false);
    
    if (ok !== false) {
      await ctx.reply(isEn ? '✅ New motivation generated!' : '✅ Новая мотивация сгенерирована!');
    } else {
      await ctx.reply(isEn ? '⚠️ Could not send motivation now.' : '⚠️ Не удалось отправить мотивацию сейчас.');
    }
  } catch {
    try {
      await ctx.reply('⚠️ Внутренняя ошибка при отправке мотивации.');
    } catch {}
  }
}

async function broadcastMotivation(dateStr, ignoreHistory = false) {
  const {usersCollection, pendingDailySendsCollection} = await import('../db/db.js');
  let sentSet = new Set();
  
  if (!ignoreHistory) {
    const already = await pendingDailySendsCollection.find({
      date: dateStr,
      sent: true
    }, {projection: {userId: 1}}).toArray();
    sentSet = new Set((already || []).map(r => r.userId));
  }
  
  const cursor = usersCollection.find(
    {$or: [{botBlocked: {$exists: false}}, {botBlocked: false}], sendMotivation: {$ne: false}},
    {projection: {userId: 1}}
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
          const ok = await sendDailyToUser(bot, targetId, dateStr, {
            disableNotification: false,
            forceRefresh: false
          }).catch(() => false);
          await pendingDailySendsCollection.updateOne({userId: targetId, date: dateStr}, {
            $set: {
              sent: !!ok,
              sentAt: ok ? new Date() : null,
              quoteSent: !!ok,
              permanentFail: !ok
            }
          }, {upsert: true});
        } catch {}
      }));
      batch = [];
    }
  }
  if (batch.length) {
    await Promise.all(batch.map(async (targetId) => {
      try {
        const ok = await sendDailyToUser(bot, targetId, dateStr, {
          disableNotification: false,
          forceRefresh: false
        }).catch(() => false);
        await pendingDailySendsCollection.updateOne({userId: targetId, date: dateStr}, {
          $set: {
            sent: !!ok,
            sentAt: ok ? new Date() : null,
            quoteSent: !!ok,
            permanentFail: !ok
          }
        }, {upsert: true});
      } catch {}
    }));
  }
  return true;
}

export async function startBot() {
  await connectToMongo();
  startTickersRefresher();

  if (isDbConnected()) {
    try {
      startAlertsChecker(bot);
    } catch {
    }
  } else {
    const tryStartChecker = setInterval(() => {
      if (isDbConnected()) {
        try {
          startAlertsChecker(bot);
        } catch {
        }
        clearInterval(tryStartChecker);
      }
    }, 10000);
  }

  await removeInactive();
  startHeartbeat(60000);

  const app = createServer();
  const PORT = process.env.PORT || 3000;
  const server = app.listen(PORT, () => console.log(`HTTP server on ${PORT}`));

  setInterval(() => processDailyQuoteRetry(bot), 60000);
  setInterval(() => watchForNewQuotes(bot), 30000);



  let lastFetchDay = null;
  let lastPrepareDay = null;
  let lastMarketSendDay = null;

  setInterval(async () => {
    try {
      const kyivNow = new Date(new Date().toLocaleString('en-US', {timeZone: KYIV_TZ}));
      const day = kyivNow.toLocaleDateString('sv-SE');
      const hour = kyivNow.getHours();

      if (day !== lastFetchDay && hour === IMAGE_FETCH_HOUR) {
        try {
          await fetchAndStoreDailyMotivation(day, {force: true});
        } catch {
        }
        lastFetchDay = day;
      }

      if (day !== lastPrepareDay && hour === PREPARE_SEND_HOUR) {
        try {
          await fetchAndStoreDailyMotivation(day, {force: false});
        } catch {
        }
        lastPrepareDay = day;

        try {
          await broadcastMotivation(day, false);
        } catch {
        }
      }

      if (day !== lastMarketSendDay && hour === (MARKET_SEND_HOUR ?? 7) && kyivNow.getMinutes() === (MARKET_SEND_MIN ?? 30)) {
        try {
          if (typeof broadcastMarketSnapshot === 'function') {
            await broadcastMarketSnapshot(bot, {
              batchSize: MARKET_BATCH_SIZE,
              pauseMs: MARKET_BATCH_PAUSE_MS
            }).catch(() => {
            });
            lastMarketSendDay = day;
          }
        } catch {
        }
      }
    } catch {
    }
  }, 60000);

  setInterval(async () => {
    try {
      await removeInactive();
    } catch {
    }
  }, 7 * DAY_MS);

  await bot.launch();
  return {server};
}

export function extractReportTimeLine(reportHtml) {
  const text = String(reportHtml || '').replace(/<[^>]+>/g, '');
  const m = text.match(/(Данные на:[^\n]+|Data as of:[^\n]+)/);
  return m ? m[1].trim() : null;
}

export async function editReportMessageToFull(ctx){
  try{
    const userId = ctx.from?.id;
    const lang = await resolveUserLang(userId).catch(()=> 'ru');
    const isEn = String(lang).toLowerCase().startsWith('en');
    const snap=await getMarketSnapshot(['BTC','ETH','PAXG']);
    console.log("📥 RAW SNAP FROM DB:", JSON.stringify(snap, null, 2));
    if(!snap?.ok) {
      await ctx.answerCbQuery(isEn?'Error':'Ошибка');
      return;
    }
    const parts = await buildMorningReportParts(
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
        cryptoquant: snap.cryptoquant,
        macro: snap.macro || null,
      }
    );
    const kb = { inline_keyboard: [[
        { text: isEn ? 'AI recommendations' : 'Рекомендации ИИ', callback_data: 'market_ai' },
        { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
      ]] };
    await ctx.editMessageText(parts.headHtml + '\n' + parts.footerHtml, { parse_mode:'HTML', reply_markup: kb });
    await ctx.answerCbQuery(isEn?'Done.':'Готово.');
  } catch {
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
}


export async function editReportMessageToShort(ctx){
  try{
    const userId = ctx.from?.id;
    const lang = await resolveUserLang(userId).catch(()=> 'ru');
    const isEn = String(lang).toLowerCase().startsWith('en');
    const snap=await getMarketSnapshot(['BTC','ETH','PAXG']);
    if(!snap?.ok) {
      await ctx.answerCbQuery(isEn?'Error':'Ошибка');
      return;
    }
    const { shortHtml, footerHtml } = buildShortReportParts(
      snap.snapshots,
      lang,
      snap.atIsoKyiv || '',
      snap.fetchedAt ?? null,
      { btcDominancePct: snap.btcDominancePct, btcDominanceDelta: snap.btcDominanceDelta, totals: snap.totals, fgiNow: snap.fgiNow, fgiDelta: snap.fgiDelta }
    );
    const kb = { inline_keyboard: [[
        { text: isEn ? 'AI recommendations' : 'Рекомендации ИИ', callback_data: 'market_ai' },
        { text: isEn ? 'Guide' : 'Справка', callback_data: 'market_help' }
      ]] };
    await ctx.editMessageText(shortHtml + '\n' + footerHtml, { parse_mode:'HTML', reply_markup: kb });
    await ctx.answerCbQuery(isEn?'Done.':'Готово.');
  } catch {
    try { await ctx.answerCbQuery('Ошибка'); } catch {}
  }
}

export function formatSurpriseMessage(surprise, lang, remainingMinutes) {
  const isEn = String(lang).startsWith('en')
  const c = isEn ? surprise.content.en : surprise.content.ru

  return [
    `<b>${c.title}</b>`,
    '',
    c.text,
    '',
    isEn
      ? `🕒 Can be updated in <b>${remainingMinutes} minutes</b>`
      : `🕒 Обновится через <b>${remainingMinutes} минут</b>`
  ].join('\n')
}
