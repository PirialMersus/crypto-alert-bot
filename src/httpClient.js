// src/httpClient.js
import axios from 'axios'
import dotenv from 'dotenv'
dotenv.config()

const AXIOS_TIMEOUT = process.env.AXIOS_TIMEOUT_MS ? parseInt(process.env.AXIOS_TIMEOUT_MS, 10) : 10000

export const httpClient = axios.create({
  timeout: AXIOS_TIMEOUT,
  headers: {
    'User-Agent': 'crypto-alert-bot/1.0',
    'Accept': 'application/json'
  }
})

export async function httpGetWithRetry(url, retries = 2, opts = {}) {
  let attempt = 0, lastErr = null
  while (attempt <= retries) {
    try {
      return await httpClient.get(url, opts)
    } catch (e) {
      lastErr = e
      const delay = Math.min(500 * Math.pow(1.6, attempt), 4000)
      await new Promise(r => setTimeout(r, delay))
      attempt++
    }
  }
  throw lastErr
}
