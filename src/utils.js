// src/utils.js
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

/**
 * safeSendTelegram — helper for centralized Telegram send handling.
 * - bot: telegraf bot instance
 * - method: string, e.g. 'sendMessage' | 'sendPhoto'
 * - args: array of arguments to pass to bot.telegram[method]
 *
 * On Telegram 403 (bot blocked) it will:
 *  - delete all alerts for the user (best-effort)
 *  - invalidate user alerts cache (best-effort)
 *  - mark user { botBlocked: true } in users collection (best-effort)
 *
 * It rethrows the original error so callers can handle it (and record statuses).
 */
export async function safeSendTelegram(bot, method, args = [], on403 = async () => {}) {
  try {
    if (!bot || !bot.telegram || typeof bot.telegram[method] !== 'function') {
      throw new Error('safeSendTelegram: invalid bot/method');
    }
    return await bot.telegram[method](...args);
  } catch (err) {
    const code = err?.response?.error_code || err?.statusCode || null;
    const desc = err?.response?.description || err?.message || '';
    if (code === 403 || /bot was blocked/i.test(String(desc))) {
      try {
        const uid = (Array.isArray(args) && args.length) ? args[0] : null;
        if (uid) {
          try {
            const db = await import('./db.js');
            if (db && db.alertsCollection) {
              await db.alertsCollection.deleteMany({ userId: uid }).catch(()=>{});
            }
          } catch (e) { /* ignore */ }

          try {
            const cacheMod = await import('./cache.js');
            if (cacheMod && typeof cacheMod.invalidateUserAlertsCache === 'function') {
              cacheMod.invalidateUserAlertsCache(uid);
            }
          } catch (e) { /* ignore */ }

          try {
            const db = await import('./db.js');
            if (db && db.usersCollection) {
              await db.usersCollection.updateOne({ userId: uid }, { $set: { botBlocked: true } }, { upsert: true }).catch(()=>{});
            }
          } catch (e) { /* ignore */ }
        }
      } catch (e) { /* swallow — cleanup should not crash caller */ }
      try { await on403(); } catch (e) {}
    }
    throw err;
  }
}
