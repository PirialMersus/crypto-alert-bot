// src/capitalTopBlock.js
import { connectToMongo, isDbConnected, client } from './db.js'
import { resolveUserLang } from './cache.js'

const DB_NAME = process.env.DB_NAME || 'crypto_alert_dev'
const COLLECTION = process.env.COLLECTION || 'marketSnapshots'

const esc = (s) => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
const fmtUsd = (x) => {
  if (x == null || !Number.isFinite(x)) return '—'
  const a = Math.abs(x)
  const sign = x >= 0 ? '' : '-'
  if (a >= 1_000_000) return `${sign}$${(a/1_000_000).toFixed(2)} M`
  if (a >= 1_000) return `${sign}$${(a/1_000).toFixed(2)} K`
  return `${sign}$${a.toFixed(2)}`
}
const classify = (oiPct, cvdUsd, lang) => {
  if (oiPct == null || cvdUsd == null || !Number.isFinite(oiPct) || !Number.isFinite(cvdUsd)) return lang==='en' ? '⚪️ no data' : '⚪️ нет данных'
  if (oiPct > 0 && cvdUsd > 0) return lang==='en' ? '🟢 long inflow' : '🟢 приток лонгов'
  if (oiPct < 0 && cvdUsd < 0) return lang==='en' ? '🔴 short inflow' : '🔴 приток шортов'
  return lang==='en' ? '🟠 absorption' : '🟠 впитывание'
}

export async function buildCapitalTopSection(ctx) {
  const lang = (await resolveUserLang(ctx)) === 'en' ? 'en' : 'ru'
  if (!isDbConnected()) await connectToMongo()
  const db = client.db(DB_NAME)
  const doc = await db.collection(COLLECTION)
    .find({}, { projection: { atIsoKyiv:1, capitalTop10:1, oiCvdPeriod:1, oiCvdLimit:1 } })
    .sort({ at:-1 }).limit(1).next()

  if (!doc || !doc.capitalTop10 || !Array.isArray(doc.capitalTop10.inflow) || !doc.capitalTop10.inflow.length) {
    return { title: lang==='en' ? '9) Capital inflow leaders' : '9) 🎯 Лидеры притока капитала', lines: [lang==='en'?'No fresh data.':'Нет свежих данных.'], footer: '' }
  }

  const w = doc.capitalTop10.windowLabel || `${doc.oiCvdLimit}×${doc.oiCvdPeriod}`
  const tTitle = lang==='en' ? '9) Capital inflow leaders' : '9) 🎯 Лидеры притока капитала'
  const tIntro = lang==='en'
    ? `Top-10 by OI Δ and CVD for ${esc(w)}`
    : `Топ-10 по сочетанию OI Δ и CVD за окно ${esc(w)}`
  const lines = doc.capitalTop10.inflow.map(it => {
    const sym = esc(String(it.symbol||'—').toUpperCase())
    const oi  = (it.oiPct==null||!Number.isFinite(it.oiPct)) ? '—' : `${it.oiPct.toFixed(2)}%`
    const cvd = (it.cvdUSD==null||!Number.isFinite(it.cvdUSD)) ? '—' : fmtUsd(it.cvdUSD)
    const mark = classify(it.oiPct, it.cvdUSD, lang)
    return `• <b>${sym}</b>: OI Δ (${esc(w)}): <b>${oi}</b> | CVD (${esc(w)}): <b>${cvd}</b> — ${mark}`
  })
  const footer = lang==='en'
    ? `Data: ${esc(doc.atIsoKyiv||'—')} (Europe/Kyiv)\nSource: Binance Futures public data`
    : `Данные на: ${esc(doc.atIsoKyiv||'—')} (Europe/Kyiv)\nИсточник: Binance Futures public data`
  return { title: tTitle, intro: tIntro, lines, footer }
}
