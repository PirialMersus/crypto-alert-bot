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

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }

export async function httpGetWithRetry(url, configOrRetries = {}, maybeOpts = {}) {
  let config = {}
  let retries = 2
  let opts = {}

  if (typeof configOrRetries === 'number') {
    retries = Number.isFinite(configOrRetries) ? configOrRetries : 2
    config = maybeOpts || {}
    opts = {}
  } else {
    config = configOrRetries || {}
    opts = maybeOpts || {}
    if (Number.isFinite(opts.retries)) retries = opts.retries
  }

  const timeout = Number.isFinite(opts.timeout) ? opts.timeout : AXIOS_TIMEOUT
  const backoffMs = Number.isFinite(opts.backoffMs) ? opts.backoffMs : 300

  let attempt = 0, lastErr = null
  while (attempt <= retries) {
    const t0 = Date.now()
    try {
      return await httpClient.get(url, { timeout, ...config })
    } catch (e) {
      lastErr = e
      const dur = Date.now() - t0
      const status = e?.response?.status ?? '-'
      const code = e?.code ?? '-'
      const msg = e?.message ?? String(e)
      let hint = ''
      const data = e?.response?.data
      if (data) {
        hint = typeof data === 'string' ? data.slice(0, 160) : JSON.stringify(data).slice(0, 160)
      }
      console.log(`[http] ERROR GET ${url} code=${code} status=${status} ${dur}ms ${hint || msg}`)
      if (attempt < retries) await sleep(backoffMs * (attempt + 1))
      attempt++
    }
  }
  throw lastErr
}
