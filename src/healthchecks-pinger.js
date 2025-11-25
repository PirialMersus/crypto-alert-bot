// src/healthchecks-pinger.js
import { httpClient } from './httpClient.js';
import https from "https";
const HC_PING_URL = process.env.HC_PING_URL || null;
let lastSent = 0;
const MIN_INTERVAL = 10_000;
const NODE_ENV = String(process.env.NODE_ENV || '').toLowerCase();
const HC_PING_ENABLED_ENV = String(process.env.HC_PING_ENABLED || '').toLowerCase();
const HC_ENABLED = (NODE_ENV !== 'development') || HC_PING_ENABLED_ENV === 'true';
export async function pingHealthchecksOnce() {
  if (!HC_ENABLED) return false;
  if (!HC_PING_URL) return false;
  const now = Date.now();
  if (now - lastSent < MIN_INTERVAL) return true;
  try {
    await httpClient.get(HC_PING_URL, { timeout: 5000, maxRedirects: 5 });
    lastSent = now;
    console.info('[healthchecks] ping sent OK', new Date().toISOString());

    try {
      https.get('https://telegram-todo-bot-k6bl.onrender.com/').on('error', () => {});
    } catch {}

    try {
      https.get('https://liq-bridge.onrender.com/').on('error', () => {});
    } catch {}

    return true;
  } catch (e) {
    console.warn('[healthchecks] ping failed', e?.message || e);
    return false;
  }
}
