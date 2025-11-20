// /mnt/data/liqBridgeApi.js
import 'dotenv/config';

const BASE = (process.env.LIQ_BRIDGE_URL || 'http://localhost:3000').replace(/\/+$/,'');

async function getJson(url) {
  const r = await fetch(url);
  const text = await r.text();
  let data;
  try { data = JSON.parse(text); } catch { throw new Error(text); }
  return data;
}

export async function getLiqMapInfo(symbol) {
  const base = String(symbol || '').trim().toUpperCase();
  const url = `${BASE}/heatmap?symbol=${encodeURIComponent(base)}`;
  const d = await getJson(url);
  if (d && d.ok && d.file_id) {
    return {
      file_id: d.file_id,
      pair: d.pair || base,
      ttl_ms: typeof d.ttl_ms === 'number' ? d.ttl_ms : null,
      snapshot_ts: typeof d.snapshot_ts === 'number' ? d.snapshot_ts : null,
      age_ms: typeof d.age_ms === 'number' ? d.age_ms : null,
      source: d.source || null
    };
  }
  if (d && d.ok === false && d.error_text) throw new Error(String(d.error_text));
  throw new Error('service error');
}
