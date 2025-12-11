// src/cache.js
import { CACHE_TTL, RECENT_SYMBOLS_MAX } from './constants.js';
import { alertsCollection, lastViewsCollection, usersCollection } from './db/db.js';

export const tickersCache = { time: 0, map: new Map() };
export const pricesCache = new Map();
export const alertsCache = new Map();
export const lastViewsCache = new Map();
export let allAlertsCache = { alerts: null, time: 0 };
export let statsCache = { count: null, time: 0 };
export const dailyCache = { date: null, doc: null, imageBuffer: null };

export const DEFAULT_ALERT_LIMIT = 1000000000;

export async function getUserAlertsCached(userId) {
  try {
    const now = Date.now();
    const c = alertsCache.get(userId);
    if (c && (now - c.time) < CACHE_TTL) return c.alerts;
    if (!alertsCollection) {
      // DB unavailable — return empty list and cache briefly
      const alerts = [];
      alertsCache.set(userId, { alerts, time: now });
      return alerts;
    }
    const alerts = await alertsCollection.find({ userId }).toArray();
    alertsCache.set(userId, { alerts, time: now });
    return alerts;
  } catch (e) {
    console.warn('getUserAlertsCached error', e?.message || e);
    return [];
  }
}

export function invalidateUserAlertsCache(userId) {
  try {
    alertsCache.delete(userId);
    allAlertsCache.time = 0;
  } catch (e) {}
}

export async function getAllAlertsCached() {
  try {
    const now = Date.now();
    if (allAlertsCache.alerts && (now - allAlertsCache.time) < CACHE_TTL) return allAlertsCache.alerts;
    if (!alertsCollection) {
      allAlertsCache = { alerts: [], time: now };
      return [];
    }
    const all = await alertsCollection.find({}).toArray();
    allAlertsCache = { alerts: all, time: Date.now() };
    return all;
  } catch (e) {
    console.warn('getAllAlertsCached error', e?.message || e);
    return [];
  }
}

export async function getUserLastViews(userId) {
  try {
    const now = Date.now();
    const cached = lastViewsCache.get(userId);
    if (cached && (now - cached.time) < CACHE_TTL) return cached.map;
    if (!lastViewsCollection) {
      const map = {};
      lastViewsCache.set(userId, { map, time: now });
      return map;
    }
    const rows = await lastViewsCollection.find({ userId }).toArray();
    const map = Object.fromEntries(rows.map(r => [r.symbol, (typeof r.lastPrice === 'number') ? r.lastPrice : null]));
    lastViewsCache.set(userId, { map, time: now });
    return map;
  } catch (e) {
    console.warn('getUserLastViews error', e?.message || e);
    return {};
  }
}

export async function setUserLastViews(userId, updates) {
  if (!updates || !Object.keys(updates).length) return;
  try {
    if (!lastViewsCollection) return;
    const ops = Object.entries(updates).map(([symbol, lastPrice]) => ({
      updateOne: { filter: { userId, symbol }, update: { $set: { lastPrice } }, upsert: true }
    }));
    await lastViewsCollection.bulkWrite(ops);
    lastViewsCache.delete(userId);
  } catch (e) {
    console.warn('setUserLastViews error', e?.message || e);
  }
}

export async function getUserRecentSymbols(userId) {
  try {
    if (!usersCollection) return [];
    const u = await usersCollection.findOne({ userId }, { projection: { recentSymbols: 1 } });
    return Array.isArray(u?.recentSymbols) ? u.recentSymbols.slice(-RECENT_SYMBOLS_MAX).reverse() : [];
  } catch (e) {
    console.warn('getUserRecentSymbols error', e?.message || e);
    return [];
  }
}

export async function pushRecentSymbol(userId, symbol) {
  try {
    if (!usersCollection) return;
    await usersCollection.updateOne({ userId }, { $pull: { recentSymbols: symbol } }).catch(()=>{});
    await usersCollection.updateOne(
      { userId },
      { $push: { recentSymbols: { $each: [symbol], $slice: -RECENT_SYMBOLS_MAX } } },
      { upsert: true }
    );
  } catch (e) {
    console.warn('pushRecentSymbol failed', e?.message || e);
  }
}

export async function getUserAlertsOrder(userId) {
  try {
    if (!usersCollection) return 'new_bottom';
    const u = await usersCollection.findOne({ userId }, { projection: { alertsOrder: 1 } });
    return u?.alertsOrder || 'new_bottom';
  } catch (e) {
    console.warn('getUserAlertsOrder error', e?.message || e);
    return 'new_bottom';
  }
}

export async function setUserAlertsOrder(userId, order) {
  try {
    if (!usersCollection) return;
    await usersCollection.updateOne({ userId }, { $set: { alertsOrder: order } }, { upsert: true });
  } catch (e) {
    console.warn('setUserAlertsOrder error', e?.message || e);
  }
}

export async function resolveUserLang(userId, ctxLang = null, ctxFromLang = null) {
  try {
    if (!usersCollection) {
      if (ctxLang) return String(ctxLang).split('-')[0];
      if (ctxFromLang) return String(ctxFromLang).split('-')[0];
      return 'ru';
    }
    const u = await usersCollection.findOne({ userId }, { projection: { preferredLang: 1, language_code: 1 } });
    if (u?.preferredLang) return String(u.preferredLang).split('-')[0];
    if (ctxLang) return String(ctxLang).split('-')[0];
    if (ctxFromLang) return String(ctxFromLang).split('-')[0];
    if (u?.language_code) return String(u.language_code).split('-')[0];
  } catch (e) {
    console.warn('resolveUserLang error', e?.message || e);
  }
  return 'ru';
}

export async function getUserAlertLimit(userId) {
  try {
    if (!usersCollection) return DEFAULT_ALERT_LIMIT;
    const u = await usersCollection.findOne({ userId }, { projection: { alertLimit: 1 } });
    const val = u?.alertLimit;
    if (typeof val === 'number' && Number.isFinite(val) && val >= 0) return Math.max(0, Math.floor(val));
  } catch (e) { /* ignore */ }
  return DEFAULT_ALERT_LIMIT;
}
