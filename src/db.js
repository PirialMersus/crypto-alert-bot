// src/db.js
import { MongoClient, ObjectId as MongoObjectId } from 'mongodb';
import EventEmitter from 'events';
import dotenv from 'dotenv';
dotenv.config();

const MONGO_URI = process.env.MONGO_URI;
if (!MONGO_URI) throw new Error('MONGO_URI не задан в окружении');

export const client = new MongoClient(MONGO_URI, {
  serverSelectionTimeoutMS: 5000,
  connectTimeoutMS: 5000,
  socketTimeoutMS: 30000,
});

export const dbEvents = new EventEmitter();

let _isConnected = false;
let reconnectTimer = null;
let currentDbName = null;

export let alertsCollection = null;
export let alertsArchiveCollection = null;
export let usersCollection = null;
export let lastViewsCollection = null;
export let dailyMotivationCollection = null;
export let dailyQuoteRetryCollection = null;
export let pendingDailySendsCollection = null;
export let marketSnapshotsCollection = null;

async function initCollectionsAndIndexes() {
  const isDev = String(process.env.NODE_ENV || '').toLowerCase() === 'development';
  const devDbName = 'crypto_alert_dev';
  const db = isDev ? client.db(devDbName) : client.db();
  currentDbName = isDev ? devDbName : null;

  alertsCollection = db.collection('alerts');
  alertsArchiveCollection = db.collection('alerts_archive');
  usersCollection = db.collection('users');
  lastViewsCollection = db.collection('last_alerts_view');
  dailyMotivationCollection = db.collection('daily_motivation');
  dailyQuoteRetryCollection = db.collection('daily_quote_retry');
  pendingDailySendsCollection = db.collection('pending_daily_sends');
  marketSnapshotsCollection = db.collection('market_snapshots');

  try {
    await alertsCollection.createIndex({ userId: 1 });
    await alertsCollection.createIndex({ symbol: 1 });
    await alertsCollection.createIndex({ userId: 1, symbol: 1 });
    await alertsArchiveCollection.createIndex({ userId: 1 });
    await alertsArchiveCollection.createIndex({ firedAt: 1 });
    await alertsArchiveCollection.createIndex({ deletedAt: 1 });
    await usersCollection.createIndex({ userId: 1 }, { unique: true });
    await usersCollection.createIndex({ lastActive: 1 });
    await lastViewsCollection.createIndex({ userId: 1, symbol: 1 }, { unique: true });
    await dailyMotivationCollection.createIndex({ date: 1 }, { unique: true });
    await dailyQuoteRetryCollection.createIndex({ date: 1 }, { unique: true });
    await pendingDailySendsCollection.createIndex({ userId: 1, date: 1 }, { unique: true });
    await marketSnapshotsCollection.createIndex({ date: 1 }, { unique: true });
  } catch (e) {
    console.error('ensureIndexes error', e?.message || e);
  }
}

async function tryConnectOnce() {
  await client.connect();
  _isConnected = true;
  await initCollectionsAndIndexes();
  dbEvents.emit('connected');

  if (reconnectTimer) {
    clearInterval(reconnectTimer);
    reconnectTimer = null;
  }

  console.log('✅ Mongo connected' + (currentDbName ? ` (db: ${currentDbName})` : ''));
}

async function tryConnectWithRetries(initialAttempts = 3) {
  let attempt = 0;
  while (attempt < initialAttempts) {
    attempt++;
    try {
      await tryConnectOnce();
      return true;
    } catch (err) {
      console.error(`Mongo connect attempt ${attempt} failed:`, err?.message || err);
      const delay = Math.min(1000 * Math.pow(2, attempt), 8000);
      await new Promise(r => setTimeout(r, delay));
    }
  }
  return false;
}

export async function connectToMongo() {
  const ok = await tryConnectWithRetries(3);
  if (!ok) {
    console.error('Failed to connect to MongoDB after initial retries');

    if (!reconnectTimer) {
      reconnectTimer = setInterval(async () => {
        console.log('Attempting background reconnect to Mongo...');
        try {
          await tryConnectOnce();
          console.log('✅ Background reconnect to Mongo succeeded');
        } catch (err) {
          console.error('Background reconnect failed:', err?.message || err);
        }
      }, 30_000);
    }
  }
}

export function isDbConnected() {
  try {
    if (client && client.topology && typeof client.topology.isConnected === 'function') {
      return client.topology.isConnected();
    }
    return !!_isConnected;
  } catch (e) {
    return !!_isConnected;
  }
}

export const ObjectId = MongoObjectId;

export async function countDocumentsWithTimeout(collectionName, filter = {}, ms = 7000) {
  try {
    if (!collectionName || !client) return 0;
    const dbHandle = currentDbName ? client.db(currentDbName) : client.db();
    if (!dbHandle) return 0;
    const coll = dbHandle.collection(collectionName);
    if (!coll || typeof coll.countDocuments !== 'function') return 0;

    const countPromise = coll.countDocuments(filter);
    const timeoutPromise = new Promise((resolve) => setTimeout(() => resolve(0), ms));
    return await Promise.race([countPromise, timeoutPromise]);
  } catch (e) {
    console.warn('countDocumentsWithTimeout error', e?.message || e);
    return 0;
  }
}
