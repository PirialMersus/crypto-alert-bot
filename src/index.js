// src/index.js
import { startBot } from './bot.js';
import { connectToMongo } from './db.js';
import { notifyAdmin } from './adminNotify.js';

process.on('uncaughtException', async (err) => {
  try { console.error('uncaughtException', err); } catch (e) {}
  try { await notifyAdmin(`🚨 Uncaught exception: ${String(err?.stack || err?.message || err)}`); } catch (e) {}
});

process.on('unhandledRejection', async (reason) => {
  try { console.error('unhandledRejection', reason); } catch (e) {}
  try { await notifyAdmin(`🚨 Unhandled rejection: ${String(reason)}`); } catch (e) {}
});

async function main() {
  try {
    await connectToMongo();
  } catch (e) {
    try { console.warn('Initial connectToMongo caught error (continuing):', e?.message || e); } catch (ee) {}
  }

  try {
    await startBot();
    console.log('Bot started.');
  } catch (err) {
    try { console.error('startup error', err); } catch (e) {}
    try { await notifyAdmin(`❌ Bot startup failed: ${String(err?.message || err)}`); } catch (e) {}
  }
}

main();
