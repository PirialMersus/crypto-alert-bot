// src/riskEngine.js
export function clamp01(x){ if(!Number.isFinite(x)) return 0; if(x<0) return 0; if(x>1) return 1; return x }
function abs(x){ return Math.abs(Number(x)||0) }
function lsRisk(longPct){ const v=Number(longPct); if(!Number.isFinite(v)) return 0; if(v>=60) return clamp01((v-60)/15); if(v<=40) return clamp01((40-v)/15); return 0 }
function priceRisk(pct24){ const v=abs(pct24); return clamp01(v/8) }
function fundingRisk(f){ const v=abs(f)*10000; return clamp01(v/30) }
function fgiRisk(fgi){ const v=Number(fgi); if(!Number.isFinite(v) || v<=50) return 0; return clamp01((v-50)/50*0.6) }
function breadthRisk(tot){ if(!tot) return 0; const d1=Number(tot.d1), d2=Number(tot.d2), d3=Number(tot.d3); const worst=Math.max(0, -(Number.isFinite(d1)?d1:0), -(Number.isFinite(d2)?d2:0), -(Number.isFinite(d3)?d3:0)); return clamp01(worst/5*0.6) }
function oiCvdRisk(verdict){ const t=String(verdict||''); if(t.includes('впитывание')) return 0.6; if(t.includes('short-cover')) return 0.4; if(t.includes('приток лонгов')) return 0.2; return 0.1 }
function modeBy(oiVerdict, rsi, volUp){
  const t=String(oiVerdict||'')
  let mode='neutral', label='Нейтрально'
  if(t.includes('приток лонгов')){ mode='trend_long_light'; label='Тренд-лонг (лёгкий)' }
  if(t.includes('short-cover')){ mode='short_cover'; label='Short-cover (шорты не гнаться)' }
  if(t.includes('впитывание')){ mode='absorption'; label='Впитывание (осторожно с пробоями)' }
  if(t.includes('охлаждение')||t.includes('нейтрально')){ mode='neutral'; label='Нейтрально' }
  const r=Number(rsi)
  if(Number.isFinite(r) && r>=70){ if(mode==='trend_long_light') { mode='trend_long_cool'; label='Перегрев — смягчить лонги' } }
  if(Number.isFinite(r) && r<=30){ if(mode!=='absorption'){ mode='knife_risk'; label='Риск «ножа» — без догоняющих шортов' } }
  if(mode==='trend_long_light' && !volUp){ mode='trend_long_watch'; label='Лонг по тренду (ждём подтверждения объёма)' }
  return { mode, label }
}
function actions(mode, riskPct, funding, lsLongPct){
  const out=[]
  const r=Number(riskPct)||0
  const f=Number(funding)||0
  const lp=Number(lsLongPct)
  if(mode==='trend_long_light'||mode==='trend_long_watch'){
    out.push('Размер: базовый')
    out.push('Плечо: не повышать')
    out.push('Вход: откат к VWAP/MA20 либо ретест уровня')
    out.push('Стоп: за свинг-лоу')
    out.push('Фиксация: частями по плану')
  }else if(mode==='short_cover'){
    out.push('Не шортить в догонку')
    out.push('Лонг: только после отката и подтверждения')
    out.push('Плечо: не повышать, стоп короткий')
  }else if(mode==='absorption'){
    out.push('Избегать пробойных лонгов')
    out.push('Контртренд от сопротивлений с узким риском')
    out.push('Пробой: вход только после ретеста')
  }else if(mode==='knife_risk'){
    out.push('Не ловить ножи')
    out.push('Шорт: только по сигналу силы продавца')
    out.push('Размер: пониженный, стоп обязателен')
  }else{
    out.push('Работать по базовому плану')
    out.push('Входы: только по A/B сетапам')
    out.push('Плечо: не разгонять')
  }
  if(Math.abs(f)>0.0003) out.push('Фандинг повышен — резать плечо')
  if(Number.isFinite(lp) && lp>65) out.push('Перекос в лонги — риск лонг-сквиза')
  if(Number.isFinite(lp) && lp<45) out.push('Перекос в шорты — риск шорт-сквиза')
  if(r>=60){ out.push('Высокий риск — снижать экспозицию, частично фиксировать') }
  else if(r>=30){ out.push('Средний риск — сокращать плечо, тянуть стопы') }
  else { out.push('Низкий риск — аккуратно, без повышения плеча') }
  return out
}
