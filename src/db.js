// src/db.js
import { MongoClient, ObjectId } from 'mongodb';
import dotenv from 'dotenv';
dotenv.config();

const MONGO_URI = process.env.MONGO_URI;
if (!MONGO_URI) throw new Error('MONGO_URI не задан в окружении');

export const client = new MongoClient(MONGO_URI, {
  // короткие таймауты, чтобы быстро понять, что сеть недоступна
  serverSelectionTimeoutMS: 5000,
  connectTimeoutMS: 5000,
  socketTimeoutMS: 30000,
});

export let alertsCollection, usersCollection, lastViewsCollection, dailyMotivationCollection, dailyQuoteRetryCollection, pendingDailySendsCollection;
let currentDbName = null;

async function tryConnectWithRetries(attempts = 3) {
  let attempt = 0;
  while (attempt < attempts) {
    try {
      await client.connect();
      return;
    } catch (e) {
      attempt++;
      console.warn(`Mongo connect attempt ${attempt} failed:`, e.message || e);
      if (attempt >= attempts) throw e;
      const delay = Math.min(1000 * Math.pow(2, attempt), 8000);
      await new Promise(r => setTimeout(r, delay));
    }
  }
}

export async function connectToMongo() {
  try {
    await tryConnectWithRetries(3);
  } catch (e) {
    console.error('Failed to connect to MongoDB after retries:', e?.message || e);
    // rethrow so caller sees problem; background tasks should handle absence of DB gracefully
    throw e;
  }

  const isDev = String(process.env.NODE_ENV || '').toLowerCase() === 'development';
  const devDbName = 'crypto_alert_dev';
  const db = isDev ? client.db(devDbName) : client.db();
  currentDbName = isDev ? devDbName : null;

  alertsCollection = db.collection('alerts');
  usersCollection = db.collection('users');
  lastViewsCollection = db.collection('last_alerts_view');
  dailyMotivationCollection = db.collection('daily_motivation');
  dailyQuoteRetryCollection = db.collection('daily_quote_retry');
  pendingDailySendsCollection = db.collection('pending_daily_sends');

  try {
    await alertsCollection.createIndex({ userId: 1 });
    await alertsCollection.createIndex({ symbol: 1 });
    await alertsCollection.createIndex({ userId: 1, symbol: 1 });
    await usersCollection.createIndex({ userId: 1 }, { unique: true });
    await usersCollection.createIndex({ lastActive: 1 });
    await lastViewsCollection.createIndex({ userId: 1, symbol: 1 }, { unique: true });
    await dailyMotivationCollection.createIndex({ date: 1 }, { unique: true });
    await dailyQuoteRetryCollection.createIndex({ date: 1 }, { unique: true });
    await pendingDailySendsCollection.createIndex({ userId: 1, date: 1 }, { unique: true });
  } catch (e) { console.error('ensureIndexes error', e); }

  console.log('Connected to MongoDB and indexes are ready 🚀', currentDbName ? `(db: ${currentDbName})` : '');
}

export { ObjectId };

export async function countDocumentsWithTimeout(collectionName, filter, ms = 7000) {
  if (!collectionName) throw new Error('collectionName required');
  const dbToUse = currentDbName ? client.db(currentDbName) : client.db();
  const coll = dbToUse.collection(collectionName);
  return await Promise.race([
    coll.countDocuments(filter),
    new Promise((_, reject) => setTimeout(() => reject(new Error('mongo_timeout')), ms))
  ]);
}
