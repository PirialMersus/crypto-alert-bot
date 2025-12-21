// src/surpriseGemini.js
import 'dotenv/config'
import axios from 'axios'

const GEMINI_KEYS_RAW =
  process.env.GEMINI_API_KEY ||
  ''

const GEMINI_KEYS = GEMINI_KEYS_RAW
  .split(',')
  .map(k => k.trim())
  .filter(Boolean)

if (!GEMINI_KEYS.length) {
  throw new Error('GEMINI API key not set')
}

const GEMINI_MODEL = process.env.GEMINI_MODEL || 'gemini-2.5-flash'
const GEMINI_TIMEOUT_MS = 60000

let keyIndex = 0

function nextKey() {
  const key = GEMINI_KEYS[keyIndex]
  keyIndex = (keyIndex + 1) % GEMINI_KEYS.length
  return key
}

function buildPrompt() {
  return `
Ты генерируешь редкий, интересный и вирусный контент для Telegram-бота про деньги и криптовалюты.

Сгенерируй ОДИН объект контента, который хочется переслать другим людям.

Категории (выбери одну):
юмор, психология, история, паттерны, цитаты великих, мотивация, удивительный факт, анекдот, хитрости, читкоды, hybrid 

Правила:
- без инвестиционных советов
- без прогнозов цен
- без призывов к действию
- старайся не повторяться в ответах для меня
- чередуй темы

Ответ верни СТРОГО валидный JSON, без текста вокруг.

Формат:
{
  "category": "...",
  "ru": {
    "title": "...",
    "text": "..."
  },
  "en": {
    "title": "...",
    "text": "..."
  }
}

Заголовок — одна строка.
Текст — 1–4 коротких предложения.
RU и EN — один смысл, не дословный перевод.
`.trim()
}

export async function generateSurprise() {
  const key = nextKey()

  const url =
    `https://generativelanguage.googleapis.com/v1beta/models/` +
    `${encodeURIComponent(GEMINI_MODEL)}:generateContent?key=${encodeURIComponent(key)}`

  const body = {
    contents: [
      {
        role: 'user',
        parts: [{ text: buildPrompt() }]
      }
    ],
    generationConfig: {
      response_mime_type: 'application/json'
    }
  }

  let res
  try {
    res = await axios.post(url, body, { timeout: GEMINI_TIMEOUT_MS })
  } catch (e) {
    console.error('[GEMINI HTTP ERROR]', {
      message: e.message,
      status: e.response?.status,
      data: e.response?.data
    })
    throw e
  }

  const parts = res?.data?.candidates?.[0]?.content?.parts || []
  const raw = parts.map(p => p.text || '').join('').trim()

  if (!raw) {
    console.error('[GEMINI EMPTY RESPONSE]', res.data)
    throw new Error('Gemini returned empty response')
  }

  try {
    return JSON.parse(raw)
  } catch (e) {
    console.error('[GEMINI INVALID JSON]', raw)
    throw e
  }
}
