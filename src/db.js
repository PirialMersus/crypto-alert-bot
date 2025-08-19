// src/db.js
import dotenv from 'dotenv';
dotenv.config();

import { MongoClient } from 'mongodb';
const MONGO_URI = process.env.MONGO_URI;
if (!MONGO_URI) throw new Error('MONGO_URI not set in .env');

let client = null;
let db = null;
let alertsCollection, usersCollection, lastViewsCollection, dailyMotivationCollection, dailyQuoteRetryCollection, pendingDailySendsCollection;

export async function connectDb() {
  if (client) return;
  client = new MongoClient(MONGO_URI);
  await client.connect();
  db = client.db();
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
  } catch (e) {
    console.error('ensureIndexes error', e);
  }

  console.log('Connected to MongoDB and indexes are ready');
}

export function getCollections() {
  if (!client) throw new Error('DB not connected — call connectDb() first');
  return {
    alertsCollection,
    usersCollection,
    lastViewsCollection,
    dailyMotivationCollection,
    dailyQuoteRetryCollection,
    pendingDailySendsCollection
  };
}
