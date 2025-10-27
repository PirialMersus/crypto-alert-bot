// src/server.js
import express from 'express';
import { KYIV_TZ } from './constants.js';
import { fetchQuoteFromAny } from './daily.js';
import { dailyMotivationCollection, client } from './db.js';
import { getLastHeartbeat } from './monitor.js';

function withTimeout(p, ms) {
  return Promise.race([
    p,
    new Promise((_, r) => setTimeout(() => r(new Error('timeout')), ms))
  ]);
}

export function createServer() {
  const app = express();

  app.get('/', (_req, res) => res.send('Бот работает!'));

  app.get('/debug/quote-test', async (_req, res) => {
    try {
      const q = await fetchQuoteFromAny();
      res.json({ ok: true, q });
    } catch (e) {
      res.json({ ok: false, err: String(e) });
    }
  });

  app.get('/debug/daily-doc', async (_req, res) => {
    try {
      const dateStr = new Date().toLocaleDateString('sv-SE', { timeZone: KYIV_TZ });
      const doc = await dailyMotivationCollection.findOne({ date: dateStr }).catch(()=>null);
      res.json({ ok: true, date: dateStr, doc });
    } catch (e) {
      res.json({ ok: false, err: String(e) });
    }
  });

  app.get('/health', async (_req, res) => {
    const now = Date.now();
    const heartbeat = getLastHeartbeat();
    let mongoConnected = false;
    let pingTimedOut = false;
    try {
      if (client && typeof client.db === 'function') {
        await withTimeout(client.db().command({ ping: 1 }), 700);
        mongoConnected = true;
      }
    } catch (e) {
      pingTimedOut = String(e?.message || e) === 'timeout';
      mongoConnected = false;
    }
    res.json({
      ok: true,
      time: now,
      lastHeartbeat: heartbeat,
      mongoConnected,
      pingTimedOut
    });
  });

  return app;
}
