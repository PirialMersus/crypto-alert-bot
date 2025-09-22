// src/server.js
import express from 'express';
import { KYIV_TZ } from './constants.js';
import { fetchQuoteFromAny } from './daily.js';
import { dailyMotivationCollection } from './db.js';
export function createServer() {
  const app = express();
  app.get('/', (_req, res) => res.send('Бот работает!'));
  app.get('/debug/quote-test', async (_req, res) => { try { const q = await fetchQuoteFromAny(); res.json({ ok: true, q }); } catch (e) { res.json({ ok: false, err: String(e) }); } });
  app.get('/debug/daily-doc', async (_req, res) => { try { const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ }); const doc = await dailyMotivationCollection.findOne({ date: dateStr }); res.json({ ok: true, date: dateStr, doc }); } catch (e) { res.json({ ok: false, err: String(e) }); } });
  return app;
}
