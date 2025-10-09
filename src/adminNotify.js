// src/adminNotify.js
import axios from 'axios';
import dotenv from 'dotenv';
dotenv.config();
const BOT_TOKEN = process.env.BOT_TOKEN;
const CREATOR_ID = process.env.CREATOR_ID;
export async function notifyAdmin(text) {
  if (!BOT_TOKEN || !CREATOR_ID) return false;
  try {
    await axios.post(
      `https://api.telegram.org/bot${BOT_TOKEN}/sendMessage`,
      { chat_id: Number(CREATOR_ID) || CREATOR_ID, text, parse_mode: 'Markdown' },
      { timeout: 5000 }
    );
    return true;
  } catch (e) {
    try { console.warn('notifyAdmin failed', e?.message || e); } catch (ee) {}
    return false;
  }
}
