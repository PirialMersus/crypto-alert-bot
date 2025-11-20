// src/index.js
import dotenv from 'dotenv'
dotenv.config()

import { startBot, bot } from './bot.js'

let serverRef = null

async function main() {
  try {
    const res = await startBot()
    serverRef = res?.server || null

    process.on('unhandledRejection', (err) => {
      try { console.error('[unhandledRejection]', err?.stack || String(err)) } catch {}
    })
    process.on('uncaughtException', (err) => {
      try { console.error('[uncaughtException]', err?.stack || String(err)) } catch {}
    })

    const graceful = async (sig) => {
      try { console.log(`signal ${sig}`) } catch {}
      try { await bot?.stop?.(sig) } catch {}
      try {
        if (serverRef) {
          await new Promise((r) => serverRef.close(() => r()))
        }
      } catch {}
      process.exit(0)
    }

    process.on('SIGINT', () => graceful('SIGINT'))
    process.on('SIGTERM', () => graceful('SIGTERM'))

  } catch (e) {
    try { console.error('fatal start error', e?.stack || String(e)) } catch {}
    process.exit(1)
  }
}

main()
