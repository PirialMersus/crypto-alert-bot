// src/cache.js
import { CACHE_TTL, RECENT_SYMBOLS_MAX } from './constants.js';
import { alertsCollection, lastViewsCollection, usersCollection } from './db.js';

export const tickersCache = { time: 0, map: new Map() };
export const pricesCache = new Map();
export const alertsCache = new Map();
export const lastViewsCache = new Map();
export let allAlertsCache = { alerts: null, time: 0 };
export let statsCache = { count: null, time: 0 };
export const dailyCache = { date: null, doc: null, imageBuffer: null };

// Временно снимаем лимит — можно вернуть позже
export const DEFAULT_ALERT_LIMIT = Number.MAX_SAFE_INTEGER;

export async function getUserAlertsCached(userId) {
  const now = Date.now();
  const c = alertsCache.get(userId);
  if (c && (now - c.time) < CACHE_TTL) return c.alerts;
  const alerts = await alertsCollection.find({ userId }).toArray();
  alertsCache.set(userId, { alerts, time: now });
  return alerts;
}

export function invalidateUserAlertsCache(userId) {
  alertsCache.delete(userId);
  allAlertsCache.time = 0;
}

export async function getAllAlertsCached() {
  const now = Date.now();
  if (allAlertsCache.alerts && (now - allAlertsCache.time) < CACHE_TTL) return allAlertsCache.alerts;
  const all = await alertsCollection.find({}).toArray();
  allAlertsCache = { alerts: all, time: Date.now() };
  return all;
}

export async function getUserLastViews(userId) {
  const now = Date.now();
  const cached = lastViewsCache.get(userId);
  if (cached && (now - cached.time) < CACHE_TTL) return cached.map;
  const rows = await lastViewsCollection.find({ userId }).toArray();
  const map = Object.fromEntries(rows.map(r => [r.symbol, (typeof r.lastPrice === 'number') ? r.lastPrice : null]));
  lastViewsCache.set(userId, { map, time: now });
  return map;
}

export async function setUserLastViews(userId, updates) {
  if (!updates || !Object.keys(updates).length) return;
  const ops = Object.entries(updates).map(([symbol, lastPrice]) => ({
    updateOne: { filter: { userId, symbol }, update: { $set: { lastPrice } }, upsert: true }
  }));
  await lastViewsCollection.bulkWrite(ops);
  lastViewsCache.delete(userId);
}

export async function getUserRecentSymbols(userId) {
  try {
    const u = await usersCollection.findOne({ userId }, { projection: { recentSymbols: 1 } });
    return Array.isArray(u?.recentSymbols) ? u.recentSymbols.slice(-6).reverse() : [];
  } catch {
    return [];
  }
}

export async function pushRecentSymbol(userId, symbol) {
  try {
    await usersCollection.updateOne({ userId }, { $pull: { recentSymbols: symbol } });
    await usersCollection.updateOne(
      { userId },
      { $push: { recentSymbols: { $each: [symbol], $slice: -RECENT_SYMBOLS_MAX } } },
      { upsert: true }
    );
  } catch (e) {}
}

export async function getUserAlertsOrder(userId) {
  try {
    const u = await usersCollection.findOne({ userId }, { projection: { alertsOrder: 1 } });
    return u?.alertsOrder || 'new_bottom';
  } catch (e) {
    return 'new_bottom';
  }
}

export async function setUserAlertsOrder(userId, order) {
  try {
    await usersCollection.updateOne({ userId }, { $set: { alertsOrder: order } }, { upsert: true });
  } catch (e) {}
}

export async function resolveUserLang(userId, ctxLang = null, ctxFromLang = null) {
  try {
    const u = await usersCollection.findOne({ userId }, { projection: { preferredLang: 1, language_code: 1 } });
    if (u?.preferredLang) return String(u.preferredLang).split('-')[0];
    if (ctxLang) return String(ctxLang).split('-')[0];
    if (ctxFromLang) return String(ctxFromLang).split('-')[0];
    if (u?.language_code) return String(u.language_code).split('-')[0];
  } catch (e) {}
  return 'ru';
}

/**
 * Alert limit helpers
 */
export async function getUserAlertLimit(userId) {
  try {
    const u = await usersCollection.findOne({ userId }, { projection: { alertLimit: 1 } });
    const val = u?.alertLimit;
    if (typeof val === 'number' && Number.isFinite(val) && val >= 0) return Math.max(0, Math.floor(val));
  } catch (e) { /* ignore */ }
  return DEFAULT_ALERT_LIMIT;
}

export async function setUserAlertLimit(userId, limit) {
  try {
    const lim = (typeof limit === 'number' && Number.isFinite(limit)) ? Math.max(0, Math.floor(limit)) : DEFAULT_ALERT_LIMIT;
    await usersCollection.updateOne({ userId }, { $set: { alertLimit: lim } }, { upsert: true });
    return lim;
  } catch (e) {
    console.error('setUserAlertLimit error', e);
    return null;
  }
}
