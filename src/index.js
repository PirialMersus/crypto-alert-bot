// src/index.js
import dotenv from 'dotenv';
dotenv.config();

try {
  const mod = await import('./bot.js');

  if (mod && typeof mod.startBot === 'function') {
    await mod.startBot();
  } else if (mod && mod.default && typeof mod.default.startBot === 'function') {
    await mod.default.startBot();
  } else {
    console.error("❌ './bot.js' does not export a function named 'startBot'. Make sure you have:\n\nexport async function startBot() { /* ... */ }\n");
    process.exit(1);
  }
} catch (e) {
  console.error('❌ Failed to load ./bot.js:', e?.stack || e?.message || e);
  process.exit(1);
}
