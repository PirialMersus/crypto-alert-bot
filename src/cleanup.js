// src/cleanup.js
import { usersCollection, alertsCollection, lastViewsCollection, pendingDailySendsCollection, dailyQuoteRetryCollection, alertsArchiveCollection } from './db/db.js';
import { INACTIVE_DAYS, DAY_MS } from './constants.js';
import { invalidateUserAlertsCache } from './cache.js';


export async function removeInactive() {
  try {
    if (!usersCollection || !alertsCollection) {
      console.warn('removeInactive: mongo not connected, skipping');
      return;
    }
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
        try {
          const docs = await alertsCollection.find({ userId: { $in: batch } }).toArray();
          if (docs && docs.length && alertsArchiveCollection) {
            const archived = docs.map(d => ({ ...d, deletedAt: new Date(), deleteReason: 'user_inactive_cleanup', archivedAt: new Date() }));
            await alertsArchiveCollection.insertMany(archived).catch(()=>{});
          }
        } catch (e) { console.warn('archive during cleanup failed', e?.message || e); }

        await alertsCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        if (lastViewsCollection) await lastViewsCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        if (pendingDailySendsCollection) await pendingDailySendsCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        if (dailyQuoteRetryCollection) await dailyQuoteRetryCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        await usersCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        for (const uid of batch) { try { invalidateUserAlertsCache(uid); } catch (e) {} }
      } catch (e) { console.error('removeInactive: batch deletion error', e?.message || e); }
    }
  } catch (e) { console.error('removeInactive error', e?.message || e); }
}
