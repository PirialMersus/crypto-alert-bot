// src/utils.js
import { IMAGE_SIZE } from './config.js';

export function fmtNum(n) {
  if (!Number.isFinite(n)) return '—';
  if (n >= 1000 || n === Math.floor(n)) return String(Math.round(n));
  if (n >= 1) return n.toFixed(4).replace(/\.?0+$/, '');
  return n.toPrecision(6).replace(/\.?0+$/, '');
}

export function padLabel(text, targetLen = 20) {
  const cur = String(text);
  if (cur.length >= targetLen) return cur;
  return cur + '\u00A0'.repeat(targetLen - cur.length);
}

export function escapeTextForTelegram(text) {
  if (!text) return text;
  const t = String(text).replace(/[\x00-\x1F\x7F]/g, '');
  // we'll not rely on MarkdownV2 escaping here; use plain text send for safety where needed
  return t;
}

export function kyivNow() {
  const s = new Date().toLocaleString('en-US', { timeZone: 'Europe/Kyiv' });
  return new Date(s);
}

export function todayDateStrKyiv() {
  return kyivNow().toLocaleDateString('sv-SE');
}

export function randomNatureImageUrl(w = IMAGE_SIZE.w, h = IMAGE_SIZE.h) {
  return `https://picsum.photos/${w}/${h}`;
}
