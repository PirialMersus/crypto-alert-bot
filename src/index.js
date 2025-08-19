// src/index.js
import dotenv from 'dotenv';
dotenv.config();

import express from 'express';
import { Telegraf } from 'telegraf';
import { connectDb } from './db.js';
import { setupBot } from './bot.js';
import { startMotivationSchedulers, stopMotivationSchedulers } from './motivation.js';

const BOT_TOKEN = process.env.BOT_TOKEN;
if (!BOT_TOKEN) throw new Error('BOT_TOKEN not set in .env');

async function main() {
  await connectDb();

  const bot = new Telegraf(BOT_TOKEN);

  await setupBot(bot);

  // launch bot
  let botLaunched = false;
  try {
    await bot.launch();
    botLaunched = true;
    console.log('Bot started (launched)');
  } catch (e) {
    console.error('bot.launch error', e);
    // 409 conflict (another instance) часто бывает при разработке — логируем и продолжаем.
    // Telegram HTTP methods (bot.telegram.sendMessage) всё ещё доступны даже если polling failed,
    // поэтому планировщики можно запускать; если хочешь — можно process.exit(1) здесь.
  }

  // start motivation schedulers (prepare 06:00, send 07:00 kyiv)
  try {
    startMotivationSchedulers(bot);
    console.log('Motivation schedulers started');
  } catch (e) {
    console.error('startMotivationSchedulers error', e);
  }

  // express health
  const app = express();
  app.get('/', (_req, res) => res.send('Бот работает!'));
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, () => console.log(`HTTP server on ${PORT}`));

  // graceful shutdown
  const shutdown = async (signal) => {
    console.log(`Received ${signal}, shutting down...`);
    try {
      stopMotivationSchedulers();
    } catch (e) { console.error('stopMotivationSchedulers error', e); }
    try {
      if (botLaunched) {
        await bot.stop(signal);
      } else {
        // if bot wasn't launched, still try to stop middleware/cleanup
        try { bot.stop(signal); } catch (e) {}
      }
    } catch (e) {
      console.error('bot.stop error', e);
    }
    process.exit(0);
  };

  process.once('SIGINT', () => shutdown('SIGINT'));
  process.once('SIGTERM', () => shutdown('SIGTERM'));
}

main().catch((e) => { console.error('Startup error', e); process.exit(1); });
