import { httpGetWithRetry, httpClient } from './httpClient.js';
const LIBRE_ENDPOINTS = ['https://libretranslate.de/translate', 'https://libretranslate.com/translate'];

async function translateViaGoogle(text, target) {
  if (!text) return null;
  try {
    const t = String(target).split('-')[0];
    const url = `https://translate.googleapis.com/translate_a/single?client=gtx&sl=auto&tl=${encodeURIComponent(t)}&dt=t&q=${encodeURIComponent(text)}`;
    const res = await httpGetWithRetry(url, 0);
    const data = res?.data;
    if (Array.isArray(data) && Array.isArray(data[0])) {
      const out = data[0].map(seg => (Array.isArray(seg) ? seg[0] : '')).join('');
      if (out && out.trim() && !isProbablyHtml(out)) return out.trim();
    }
  } catch (e) {}
  return null;
}
async function translateViaLibre(text, target) {
  if (!text) return null;
  const t = String(target).split('-')[0].toLowerCase();
  for (const endpoint of LIBRE_ENDPOINTS) {
    try {
      const resp = await httpClient.post(endpoint, { q: text, source: 'auto', target: t, format: 'text' }, { headers: { 'Content-Type': 'application/json' }, timeout: 7000 });
      const d = resp?.data;
      const cand = d?.translatedText || d?.result || d?.translated_text || (typeof d === 'string' ? d : null);
      if (cand && String(cand).trim()) {
        const cleaned = stripHtml(String(cand).trim());
        if (cleaned && !isProbablyHtml(cleaned)) return cleaned;
      }
    } catch (e) {}
  }
  return null;
}
function containsScript(text, script) {
  if (!text) return false;
  if (script === 'cyrillic') return /\p{Script=Cyrillic}/u.test(text);
  if (script === 'latin') return /\p{Script=Latin}/u.test(text);
  return false;
}
function normalizePunctuation(s) {
  if (!s) return s;
  let out = s.replace(/\r\n|\r/g, '\n');
  out = out.replace(/\s+([,.:;!?])/g, '$1');
  out = out.replace(/([,.:;!?])([^\s\n])/g, '$1 $2');
  out = out.replace(/\s{2,}/g, ' ');
  out = out.replace(/[ \t]+\n/g, '\n');
  out = out.replace(/«\s+/g, '«').replace(/\s+»/g, '»');
  out = out.trim();
  return out;
}
function humanizeForRu(s) {
  if (!s) return s;
  let out = s;
  out = out.replace(/^"(.*)"$/, '«$1»');
  out = out.replace(/"([^"]+)"/g, '«$1»');
  out = out.replace(/(^|\s)-\s+/g, '$1— ');
  out = out.replace(/``|''/g, '"');
  out = normalizePunctuation(out);
  out = out.replace(/\s+—\s+/g, ' — ');
  out = out.replace(/«\s+/g, '«').replace(/\s+»/g, '»');
  out = out.charAt(0).toUpperCase() + out.slice(1);
  return out;
}
function humanizeForUk(s) {
  return humanizeForRu(s);
}
function humanizeForEn(s) {
  if (!s) return s;
  let out = s;
  out = normalizePunctuation(out);
  out = out.replace(/\s+—\s+/g, ' — ');
  out = out.charAt(0).toUpperCase() + out.slice(1);
  return out;
}
function postEditByLang(s, lang) {
  if (!s) return s;
  const t = String(lang).split('-')[0].toLowerCase();
  if (t === 'ru') return humanizeForRu(s);
  if (t === 'uk') return humanizeForUk(s);
  return humanizeForEn(s);
}
function scoreCandidate(candidate, targetLang, original) {
  if (!candidate) return -9999;
  let score = 0;
  const t = String(targetLang).split('-')[0].toLowerCase();
  if (t === 'ru' || t === 'uk') {
    if (containsScript(candidate, 'cyrillic')) score += 50;
    if (!containsScript(candidate, 'latin')) score += 10;
  } else {
    if (containsScript(candidate, 'latin')) score += 50;
  }
  const lenRatio = candidate.length / Math.max(1, (original || '').length);
  score += Math.max(0, 20 - Math.abs(lenRatio - 1) * 20);
  const junkMatches = candidate.match(/�|&#\d+;|\\u[0-9a-fA-F]{4}/g);
  if (junkMatches && junkMatches.length) score -= 40;
  const punctuationCount = (candidate.match(/[.,:;!?—«»"]/g) || []).length;
  score += Math.min(10, punctuationCount);
  return score;
}
function isProbablyHtml(s) {
  if (!s || typeof s !== 'string') return false;
  const low = s.slice(0, 200).toLowerCase();
  if (low.includes('<!doctype') || low.includes('<html') || low.includes('<head') || low.includes('<body')) return true;
  // if contains many tags
  const tags = (s.match(/<[^>]+>/g) || []).length;
  if (tags >= 3) return true;
  return false;
}
function stripHtml(s) {
  if (!s) return s;
  return s.replace(/<[^>]*>/g, ' ').replace(/\s{2,}/g, ' ').trim();
}
export async function translateOrNull(text, targetLang) {
  if (!text) return null;
  if (!targetLang) return null;
  const t = String(targetLang).split('-')[0].toLowerCase();
  if (!t || t === 'en') return text;
  let candidates = [];
  try {
    const g = await translateViaGoogle(text, t).catch(()=>null);
    if (g) candidates.push({ src: 'google', text: g, score: scoreCandidate(g, t, text) });
  } catch (e) {}
  try {
    const l = await translateViaLibre(text, t).catch(()=>null);
    if (l) candidates.push({ src: 'libre', text: l, score: scoreCandidate(l, t, text) });
  } catch (e) {}
  if (!candidates.length) {
    try {
      const en = await translateViaGoogle(text, 'en').catch(()=>null);
      if (en) {
        const back = await translateViaGoogle(en, t).catch(()=>null);
        if (back && !isProbablyHtml(back)) candidates.push({ src: 'back_google', text: back, score: scoreCandidate(back, t, text) });
        const backL = await translateViaLibre(en, t).catch(()=>null);
        if (backL && !isProbablyHtml(backL)) candidates.push({ src: 'back_libre', text: backL, score: scoreCandidate(backL, t, text) });
      }
    } catch (e) {}
  }
  candidates = candidates.filter(c => c && c.text && !isProbablyHtml(c.text)).sort((a,b) => b.score - a.score);
  let best = candidates.length ? candidates[0].text : null;
  if (best) {
    const after = postEditByLang(best, t);
    return after;
  }
  return null;
}
