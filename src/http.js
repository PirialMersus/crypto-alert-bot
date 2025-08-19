// src/http.js
import axios from 'axios';
import { AXIOS_TIMEOUT } from './config.js';

export const httpClient = axios.create({
  timeout: AXIOS_TIMEOUT,
  headers: { 'User-Agent': 'crypto-alert-bot/1.0' }
});

export async function httpGetWithRetry(url, retries = 2, opts = {}) {
  let attempt = 0;
  let lastErr = null;
  while (attempt <= retries) {
    try {
      return await httpClient.get(url, opts);
    } catch (e) {
      lastErr = e;
      const delay = Math.min(500 * Math.pow(2, attempt), 2000);
      await new Promise(r => setTimeout(r, delay));
      attempt++;
    }
  }
  throw lastErr;
}
