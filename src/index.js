// src/index.js
import { startBot } from './bot.js';
startBot().catch(err => { console.error('startup error', err); process.exit(1); });
