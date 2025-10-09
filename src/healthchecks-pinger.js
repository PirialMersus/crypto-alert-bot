// src/healthchecks-pinger.js

import { httpClient } from './httpClient.js';

const HC_PING_URL = process.env.HC_PING_URL || null;
let lastSent = 0;
const MIN_INTERVAL = 10000;

export async function pingHealthchecksOnce() {
  if (!HC_PING_URL) return false;
  const now = Date.now();
  if (now - lastSent < MIN_INTERVAL) return true;
  try {
    await httpClient.get(HC_PING_URL, { timeout: 5000, maxRedirects: 5 });
    lastSent = now;
    console.info('[healthchecks] ping sent OK', new Date().toISOString());
    return true;
  } catch (e) {
    console.warn('[healthchecks] ping failed', e?.message || e);
    return false;
  }
}

