// src/cleanup.js
import { usersCollection, alertsCollection, lastViewsCollection, pendingDailySendsCollection, dailyQuoteRetryCollection } from './db.js';
import { INACTIVE_DAYS, DAY_MS } from './constants.js';
export async function removeInactive() {
  try {
    const cutoff30 = new Date(Date.now() - INACTIVE_DAYS * DAY_MS);
    const cutoff90 = new Date(Date.now() - 90 * DAY_MS);
    const cursor = usersCollection.find({ lastActive: { $lt: cutoff30 } }, { projection: { userId: 1, lastActive: 1 } });
    const toDeleteSet = new Set();
    while (await cursor.hasNext()) {
      const u = await cursor.next();
      if (!u || !u.userId) continue;
      const uid = u.userId;
      try {
        if (u.lastActive && (new Date(u.lastActive) < cutoff90)) { toDeleteSet.add(uid); continue; }
        const alertsCount = await alertsCollection.countDocuments({ userId: uid });
        if (!alertsCount) { toDeleteSet.add(uid); }
      } catch (e) { console.warn('removeInactive: error checking user', uid, e?.message || e); }
    }
    const toDelete = Array.from(toDeleteSet);
    if (!toDelete.length) return;
    const BATCH = 200;
    for (let i = 0; i < toDelete.length; i += BATCH) {
      const batch = toDelete.slice(i, i + BATCH);
      try {
        await alertsCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        await lastViewsCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        await pendingDailySendsCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        await dailyQuoteRetryCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        await usersCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
      } catch (e) { console.error('removeInactive: batch deletion error', e?.message || e); }
    }
  } catch (e) { console.error('removeInactive error', e?.message || e); }
}
