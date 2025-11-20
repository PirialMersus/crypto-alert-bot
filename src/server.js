// src/server.js
import express from 'express';
import axios from 'axios';
import { KYIV_TZ } from './constants.js';
import { fetchQuoteFromAny } from './daily.js';
import { dailyMotivationCollection } from './db.js';

async function loadLlamaCexDataset(slug) {
  const trimmed = String(slug || '').trim().toLowerCase();
  const candidates = [
    `https://api.llama.fi/cex/reserves/${trimmed}`,
    `https://api.llama.fi/cex/reserves/${trimmed}-cex`,
    `https://preview.dl.llama.fi/cex/${trimmed}`,
    `https://preview.dl.llama.fi/cex/${trimmed}-cex`
  ];

  for (const url of candidates) {
    try {
      const { data } = await axios.get(url, {
        params: { _t: Date.now() },
        headers: {
          'User-Agent': 'crypto-alert-bot/1.0',
          Accept: 'application/json'
        },
        timeout: 15000
      });

      if (data) {
        const keys = typeof data === 'object' && data !== null ? Object.keys(data) : [];
        console.log('[llama.cex] ok', {
          slug: trimmed,
          url,
          type: Array.isArray(data) ? 'array' : typeof data,
          keysSample: keys.slice(0, 20)
        });
        return data;
      }

      console.log('[llama.cex] empty', { slug: trimmed, url });
    } catch (e) {
      console.log('[llama.cex] fail', {
        slug: trimmed,
        url,
        err: e?.message || String(e)
      });
    }
  }

  console.log('[llama.cex] all_failed', { slug: trimmed });
  return null;
}

export function createServer() {
  const app = express();

  app.get('/', (_req, res) => {
    res.send('Бот работает!');
  });

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
      const doc = await dailyMotivationCollection.findOne({ date: dateStr }).catch(() => null);
      res.json({ ok: true, date: dateStr, doc });
    } catch (e) {
      res.json({ ok: false, err: String(e) });
    }
  });

  app.get('/debug/cex-reserves', async (req, res) => {
    const slug = String(req.query.slug || 'binance').trim().toLowerCase();
    const symbol = String(req.query.symbol || 'BTC').trim().toUpperCase();

    try {
      const data = await loadLlamaCexDataset(slug);

      if (!data) {
        res.json({
          ok: false,
          slug,
          symbol,
          error: 'no_data'
        });
        return;
      }

      let topLevelType = typeof data;
      if (Array.isArray(data)) {
        topLevelType = 'array';
      }

      const topKeys =
        data && typeof data === 'object'
          ? Object.keys(data).slice(0, 30)
          : [];

      let firstEntry = null;

      if (Array.isArray(data)) {
        firstEntry = data[0] || null;
      } else if (data && typeof data === 'object') {
        const firstKey = Object.keys(data)[0];
        if (firstKey !== undefined) {
          firstEntry = data[firstKey];
        }
      }

      let firstEntryKeys = [];
      let firstEntrySample = null;
      let firstSymbolHint = null;

      if (firstEntry && typeof firstEntry === 'object') {
        firstEntryKeys = Object.keys(firstEntry);
        firstSymbolHint =
          firstEntry.symbol ||
          firstEntry.token ||
          firstEntry.name ||
          firstEntry.asset ||
          firstEntry.coin ||
          null;

        try {
          const json = JSON.stringify(firstEntry, null, 2);
          firstEntrySample = json.length > 2000 ? json.slice(0, 2000) + '...<truncated>' : json;
        } catch {
          firstEntrySample = null;
        }
      }

      console.log('[llama.debug]', {
        slug,
        symbol,
        topLevelType,
        topKeys,
        firstEntryKeys,
        firstSymbolHint
      });

      res.json({
        ok: true,
        slug,
        symbol,
        topLevelType,
        topKeys,
        firstEntryKeys,
        firstSymbolHint,
        firstEntrySample
      });
    } catch (e) {
      console.log('[llama.debug] error', {
        slug,
        symbol,
        err: e?.message || String(e)
      });

      res.json({
        ok: false,
        slug,
        symbol,
        error: String(e?.message || e)
      });
    }
  });

  return app;
}
