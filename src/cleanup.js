// src/cleanup.js
import { usersCollection, alertsCollection, lastViewsCollection, pendingDailySendsCollection, dailyQuoteRetryCollection, alertsArchiveCollection } from './db.js';
import { INACTIVE_DAYS, DAY_MS } from './constants.js';
import { invalidateUserAlertsCache } from './cache.js';

function toValidDate(d) {
  if (!d) return null;
  if (d instanceof Date) {
    if (!Number.isFinite(d.getTime())) return null;
    return d;
  }
  try {
    const dd = new Date(d);
    if (Number.isFinite(dd.getTime())) return dd;
  } catch (e) {}
  return null;
}

function normalizeUserIdVariants(userId) {
  const ids = new Set();
  ids.add(userId);
  try { ids.add(String(userId)); } catch (e) {}
  try { const n = Number(userId); if (!Number.isNaN(n)) ids.add(n); } catch (e) {}
  return Array.from(ids);
}

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
        try {
          const docs = await alertsCollection.find({ userId: { $in: batch } }).toArray();
          if (docs && docs.length) {
            const archived = docs.map(d => ({ ...d, deletedAt: new Date(), deleteReason: 'user_inactive_cleanup', archivedAt: new Date() }));
            await alertsArchiveCollection.insertMany(archived).catch(()=>{});
          }
        } catch (e) { console.warn('archive during cleanup failed', e?.message || e); }

        await alertsCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        await lastViewsCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        await pendingDailySendsCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        await dailyQuoteRetryCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        await usersCollection.deleteMany({ userId: { $in: batch } }).catch(()=>{});
        for (const uid of batch) { try { invalidateUserAlertsCache(uid); } catch (e) {} }
      } catch (e) { console.error('removeInactive: batch deletion error', e?.message || e); }
    }
  } catch (e) { console.error('removeInactive error', e?.message || e); }
}

export async function purgeAlertsOlderThanDays(days = 90) {
  try {
    const cutoff = new Date(Date.now() - (Number(days) || 90) * DAY_MS);

    const allCandidates = await alertsArchiveCollection.find({}).project({ _id: 1, firedAt: 1, deletedAt: 1, createdAt: 1 }).toArray();
    const toDeleteIds = [];
    for (const d of allCandidates) {
      const dates = [toValidDate(d.firedAt), toValidDate(d.deletedAt), toValidDate(d.createdAt)].filter(Boolean);
      const maxDate = dates.length ? new Date(Math.max(...dates.map(x => x.getTime()))) : new Date(0);
      if (maxDate < cutoff) toDeleteIds.push(d._id);
    }
    let deletedFromArchive = 0;
    if (toDeleteIds.length) {
      const res = await alertsArchiveCollection.deleteMany({ _id: { $in: toDeleteIds } });
      deletedFromArchive = res?.deletedCount || toDeleteIds.length;
    }

    const activeQ = { createdAt: { $lt: cutoff } };
    const activeDocs = await alertsCollection.find(activeQ).toArray();
    let archivedAndDeletedActive = 0;
    if (activeDocs && activeDocs.length) {
      const archived = activeDocs.map(d => ({ ...d, deletedAt: new Date(), deleteReason: 'auto_purge', archivedAt: new Date() }));
      await alertsArchiveCollection.insertMany(archived).catch(()=>{});
      const r = await alertsCollection.deleteMany(activeQ).catch(()=>({ deletedCount: 0 }));
      archivedAndDeletedActive = r?.deletedCount || archived.length;
    }

    try {
      const cacheMod = await import('./cache.js').catch(()=>null);
      if (cacheMod && cacheMod.allAlertsCache) cacheMod.allAlertsCache.time = 0;
    } catch (e) {}

    return { deletedFromArchive, archivedAndDeletedActive };
  } catch (e) {
    console.error('purgeAlertsOlderThanDays error', e?.message || e);
    throw e;
  }
}

export async function clearUserOldAlerts(userId, days = 30) {
  try {
    const cutoff = new Date(Date.now() - (Number(days) || 30) * DAY_MS);
    const userVariants = normalizeUserIdVariants(userId);

    const archCandidates = await alertsArchiveCollection.find({ userId: { $in: userVariants } }).project({ _id: 1, firedAt: 1, deletedAt: 1, createdAt: 1, userId: 1 }).toArray();

    const toDeleteArchiveIds = [];
    for (const d of archCandidates) {
      const dates = [toValidDate(d.firedAt), toValidDate(d.deletedAt), toValidDate(d.createdAt)].filter(Boolean);
      const maxDate = dates.length ? new Date(Math.max(...dates.map(x => x.getTime()))) : new Date(0);
      if (maxDate < cutoff) toDeleteArchiveIds.push(d._id);
    }

    let deletedArchiveCount = 0;
    if (toDeleteArchiveIds.length) {
      const delRes = await alertsArchiveCollection.deleteMany({ _id: { $in: toDeleteArchiveIds } });
      deletedArchiveCount = delRes?.deletedCount || toDeleteArchiveIds.length;
    }

    const activeQ = { userId: { $in: userVariants }, createdAt: { $lt: cutoff } };
    const activeDocs = await alertsCollection.find(activeQ).toArray();
    let archivedActiveCount = 0;
    if (activeDocs && activeDocs.length) {
      const archivedDocs = activeDocs.map(d => ({ ...d, deletedAt: new Date(), deleteReason: 'user_cleared_old', archivedAt: new Date() }));
      await alertsArchiveCollection.insertMany(archivedDocs, { ordered: false }).catch(()=>{});
      const r = await alertsCollection.deleteMany(activeQ).catch(()=>({ deletedCount: 0 }));
      archivedActiveCount = r?.deletedCount || archivedDocs.length;
    }

    try { await lastViewsCollection.deleteMany({ userId: { $in: userVariants } }); } catch (e) {}
    try { invalidateUserAlertsCache(userId); } catch (e) {}

    return { archivedActive: archivedActiveCount, deletedFromArchive: deletedArchiveCount, checkedArchiveCandidates: archCandidates.length };
  } catch (e) {
    console.error('clearUserOldAlerts error', e?.message || e);
    throw e;
  }
}

export async function deleteAllUserArchived(userId) {
  try {
    const ids = new Set();
    ids.add(userId);
    try { ids.add(String(userId)); } catch (e) {}
    try { const n = Number(userId); if (!Number.isNaN(n)) ids.add(n); } catch (e) {}
    const variants = Array.from(ids);
    const candidates = await alertsArchiveCollection.find({ userId: { $in: variants } }).project({ _id: 1 }).toArray();
    const toDel = candidates.map(d => d._id);
    if (!toDel.length) return { deleted: 0 };
    const res = await alertsArchiveCollection.deleteMany({ _id: { $in: toDel } });
    try { await lastViewsCollection.deleteMany({ userId: { $in: variants } }); } catch (e) {}
    try { invalidateUserAlertsCache(userId); } catch (e) {}
    try { const cacheMod = await import('./cache.js').catch(()=>null); if (cacheMod && cacheMod.allAlertsCache) cacheMod.allAlertsCache.time = 0; } catch (e) {}
    return { deleted: res?.deletedCount || toDel.length };
  } catch (e) {
    console.error('deleteAllUserArchived error', e?.message || e);
    throw e;
  }
}
