// src/utils.js
import {getUserAlertsOrder, resolveUserLang, statsCache} from "./cache.js";
import {
  CACHE_TTL, CREATOR_ID,
  DAY_MS,
  IMAGE_FETCH_HOUR,
  INACTIVE_DAYS,
  KYIV_TZ, MARKET_BATCH_PAUSE_MS, MARKET_BATCH_SIZE,
  MARKET_SEND_HOUR, MARKET_SEND_MIN,
  PREPARE_SEND_HOUR
} from "./constants.js";
import {
  broadcastMarketSnapshot,
  buildMorningReportHtml,
  getMarketSnapshot,
  sendMarketReportToUser
} from "./marketMonitor.js";
import {bot} from "./bot.js";
import {setLastHeartbeat} from "./monitor.js";
import {connectToMongo, countDocumentsWithTimeout, isDbConnected} from "./db.js";
import {fetchAndStoreDailyMotivation, processDailyQuoteRetry, sendDailyToUser, watchForNewQuotes} from "./daily.js";
import {startTickersRefresher} from "./prices.js";
import {startAlertsChecker} from "./alerts.js";
import {removeInactive} from "./cleanup.js";
import {createServer} from "./server.js";

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


export function buildAiPrompt(lang, reportText) {
  const isEn = String(lang).toLowerCase().startsWith('en');
  const reportBlock = String(reportText || '');
  if (isEn) {
    return (
      'You are a professional crypto market analyst and educator for beginner–intermediate traders and investors.\n\n' +
      'User addressing rules:\n' +
      '- Address the user directly as a single person.\n' +
      '- Use a friendly, confident, mentor-like tone, as if guiding a future pro trader.\n' +
      '- Always address the user as "Future millionaire", but no more than once in the whole answer.\n' +
      '- Do NOT use: "colleagues", "everyone", "dear friends", "ladies and gentlemen".\n\n' +
      'You are given a fresh market report below. Based ONLY on that report, you must:\n' +
      '1) Describe the overall market state: who is in control (buyers vs sellers), whether there is panic, oversold/overbought conditions, etc.\n' +
      '2) Highlight key risks and threats (liquidations, long/short imbalance, extreme fear, funding, flows, etc.).\n' +
      '3) Describe two main scenarios:\n' +
      '   • short term (hours / couple of days),\n' +
      '   • medium term (several days to weeks).\n' +
      '4) Provide concrete recommendations for a TRADER:\n' +
      '   • separate block "✅ What a trader SHOULD do",\n' +
      '   • separate block "❌ What a trader SHOULD NOT do".\n' +
      '5) Provide recommendations for a LONG-TERM INVESTOR:\n' +
      '   • block "✅ What an investor SHOULD do",\n' +
      '   • block "❌ What an investor SHOULD NOT do".\n' +
      '6) List which metrics are important to monitor in the near future (RSI, funding, long/short ratio, OI, CVD, flows, etc.).\n' +
      '7) End with a short 2–3 sentence summary: your overall verdict on the market.\n\n' +
      'Important:\n' +
      '- Rely ONLY on the data from the report below. Do NOT invent your own prices or indicators.\n' +
      '- Answer in English, clearly and structurally, as if explaining to a thinking but not very advanced trader.\n' +
      '- Avoid vague statements like "the market is volatile, be careful". Be specific and scenario-based.\n' +
      '- Use a few emojis to structure the answer (in section titles and key bullet points: 📊, ⚠️, 📈, 📉, 🧠, 🧘, 🔍, ✅, ❌).\n\n' +
      'Response format (Markdown, no links, no tables):\n' +
      '1. Short headline with the main takeaway.\n' +
      '2. Section 📊 "Overall market picture".\n' +
      '3. Section ⚠️ "Main risks".\n' +
      '4. Section 🧠 "Price scenarios".\n' +
      '5. Section 📈 "Trader recommendations" (with "✅ What to do" / "❌ What NOT to do").\n' +
      '6. Section 🧘 "Investor recommendations" (with "✅" / "❌").\n' +
      '7. Section 🔍 "What to watch next".\n' +
      '8. Short final summary.\n\n' +
      'Here is the report data you must base your analysis on:\n' +
      '```\n' +
      reportBlock +
      '\n```'
    );
  }
  return (
    'Ты — профессиональный аналитик криптовалютного рынка и преподаватель для начинающих трейдеров и инвесторов.\n\n' +
    'Правила обращения к пользователю:\n' +
    '- Обращайся к пользователю на "ты".\n' +
    '- Используй дружеский, уверенный и наставнический тон, как будто ты опытный трейдер-наставник.\n' +
    '- Всегда обращайся к пользователю как к "Будущий миллионер". Но не более одного раза за весь текст\n' +
    '- Не используй слова: "коллеги", "друзья", "вы", "уважаемые", "господа".\n\n' +
    'У тебя есть свежий рыночный отчёт внизу. По нему нужно:\n' +
    '1) Дать общую картину рынка: кто сейчас доминирует — покупатели или продавцы, есть ли паника, перепроданность/перекупленность.\n' +
    '2) Выделить ключевые риски и угрозы (ликвидации, перекос лонги/шорты, экстремальный страх и т.п.).\n' +
    '3) Описать два основных сценария:\n' +
    '   • краткосрочный (часы/пара дней),\n' +
    '   • среднесрочный (несколько дней–недели).\n' +
    '4) Дать конкретные рекомендации для ТРЕЙДЕРА:\n' +
    '   • отдельный блок "✅ Что делать трейдеру",\n' +
    '   • отдельный блок "❌ Чего НЕ делать трейдеру".\n' +
    '5) Дать рекомендации для ДОЛГОСРОЧНОГО ИНВЕСТОРА:\n' +
    '   • отдельный блок "✅ Что делать инвестору",\n' +
    '   • отдельный блок "❌ Чего НЕ делать инвестору".\n' +
    '6) Указать, какие метрики важно отслеживать в ближайшее время (RSI, фандинг, лонги/шорты, OI, CVD, притоки/оттоки и т.п.).\n' +
    '7) В конце дать короткое резюме в 2–3 предложения: общий вердикт по рынку.\n\n' +
    'Очень важно:\n' +
    '- Опираться ТОЛЬКО на данные отчёта ниже. Не придумывай свои цены или показатели.\n' +
    '- Пиши по-русски, структурно и понятно, как для думающего, но не супер-опытного трейдера.\n' +
    '- Избегай воды и общих фраз вроде "рынок волатилен, будьте осторожны".\n' +
    '- Используй немного эмодзи, чтобы структурировать ответ:\n' +
    '  • заголовки разделов можно помечать: 📊, ⚠️, 📈, 📉, 🧠, 🧘, 🔍, ✅, ❌\n' +
    'Формат ответа (Markdown, но без ссылок и без таблиц):\n' +
    '1. Короткий заголовок с общим выводом.\n' +
    '2. Раздел 📊 "Общая картина рынка".\n' +
    '3. Раздел ⚠️ "Основные риски".\n' +
    '4. Раздел 🧠 "Сценарии движения цены".\n' +
    '5. Раздел 📈 "Рекомендации для трейдера" (с подпунктами "✅ Что делать" и "❌ Чего не делать").\n' +
    '6. Раздел 🧘 "Рекомендации для инвестора" (также с "✅" и "❌").\n' +
    '7. Раздел 🔍 "Что смотреть дальше".\n' +
    '8. Короткое итоговое резюме.\n\n' +
    'Ниже данные отчёта, на которых нужно основать анализ:\n' +
    '```\n' +
    reportBlock +
    '\n```'
  );
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
    const typingTimer = startTyping(ctx);
    const state = reportInFlight.get(ctx.from.id);
    if (state) state.typingTimer = typingTimer;
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
    const {usersCollection} = await import('./db.js');
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
  try {
    ctx.telegram.sendChatAction(ctx.chat.id, 'typing').catch(() => {
    });
  } catch {
  }
  const t = setInterval(() => {
    try {
      ctx.telegram.sendChatAction(ctx.chat.id, 'typing').catch(() => {
      });
    } catch {
    }
  }, 4000);
  return t;
}

export function stopTyping(t) {
  try {
    if (t) clearInterval(t);
  } catch {
  }
}

function lockReport(userId, ms = 30000) {
  reportInFlight.set(userId, {until: Date.now() + ms, typingTimer: null, startedMsgId: null});
}

function unlockReport(userId) {
  const s = reportInFlight.get(userId);
  if (s?.typingTimer) stopTyping(s.typingTimer);
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
  const create = isEn ? '➕ Create alert' : '➕ Создать алерт';
  const my = isEn ? '📋 My alerts' : '📋 Мои алерты';
  const shortBtn = isEn ? '📈 Short market report' : '📈 Краткий отчёт';
  const fullBtn = isEn ? '📊 Full report' : '📊 Полный отчёт';
  const history = isEn ? '📜 Alerts history' : '📜 История алертов';
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
      await ctx.telegram.sendChatAction(ctx.chat.id, 'upload_photo').catch(() => {
      });
    } catch {
    }
    const dateStr = new Date().toLocaleDateString('sv-SE', {timeZone: KYIV_TZ});
    const ok = await sendDailyToUser(bot, ctx.from.id, dateStr, {
      disableNotification: false,
      forceRefresh: false
    }).catch(() => false);
    if (!ok) await ctx.reply(isEn ? '⚠️ Could not send motivation now.' : '⚠️ Не удалось отправить мотивацию сейчас.');
  } catch {
    try {
      await ctx.reply('⚠️ Внутренняя ошибка при отправке мотивации.');
    } catch {
    }
  }
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

  const dateStrNow = new Date().toLocaleDateString('sv-SE', {timeZone: KYIV_TZ});
  try {
    await fetchAndStoreDailyMotivation(dateStrNow).catch(() => {
    });
  } catch {
  }

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
          const dateStr = day;
          const {usersCollection, pendingDailySendsCollection} = await import('./db.js');
          const already = await pendingDailySendsCollection.find({
            date: dateStr,
            sent: true
          }, {projection: {userId: 1}}).toArray();
          const sentSet = new Set((already || []).map(r => r.userId));
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
                } catch {
                }
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
              } catch {
              }
            }));
          }
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
