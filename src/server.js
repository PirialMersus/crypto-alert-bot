// src/server.js
import express from 'express';
import { KYIV_TZ } from './constants.js';
import { fetchQuoteFromAny } from './daily.js';
import { dailyMotivationCollection, client } from './db.js';
import { getLastHeartbeat } from './monitor.js';

function timeout(promise, ms = 2000) {
  return Promise.race([
    promise,
    new Promise((_, rej) => setTimeout(() => rej(new Error('mongo_ping_timeout')), ms))
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

  // ВАЖНО: healthcheck не должен валиться из-за Mongo.
  // Возвращаем 200 мгновенно; mongoConnected — просто поле наблюдения.
  app.get('/health', async (_req, res) => {
    const now = Date.now();
    const heartbeat = getLastHeartbeat();
    let mongoConnected = false;
    try {
      if (client && typeof client.db === 'function') {
        // ограничим ping по времени, чтобы health не висел
        await timeout(client.db().command({ ping: 1 }), 1500);
        mongoConnected = true;
      }
    } catch {
      mongoConnected = false;
    }
    res.json({
      ok: true,                // <- всегда true: сервис жив
      time: now,
      lastHeartbeat: heartbeat,
      mongoConnected           // <- для наблюдения, но не критично для статуса
    });
  });

  return app;
}
