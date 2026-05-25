import 'dotenv/config'
import axios from 'axios'
import { GEMINI_MOTIVATION_TIMEOUT_MS } from './constants.js'

const GEMINI_KEYS_RAW = process.env.GEMINI_API_KEY || ''

const GEMINI_KEYS = GEMINI_KEYS_RAW
  .split(',')
  .map(k => k.trim())
  .filter(Boolean)

const GEMINI_TEXT_MODEL = process.env.GEMINI_MODEL || 'gemini-2.5-flash'

let keyIndex = 0

function nextKey() {
  const key = GEMINI_KEYS[keyIndex]
  keyIndex = (keyIndex + 1) % GEMINI_KEYS.length
  return key
}

function buildVisionMotivationPrompt() {
  return `
Ты — генератор утренней мотивации для Telegram-бота.

Перед тобой — фотография. Внимательно рассмотри её: определи атмосферу, настроение, тематику.

Задача: Напиши ОДНУ короткую мотивационную фразу, которая идеально подходит под настроение этой картинки.

Стиль (чередуй):
- Резкие наблюдения, парадоксы, неприятная правда о жизни, деньгах, мышлении
- Цитаты в стиле известных людей (философы, предприниматели, учёные) — с указанием автора

Правила:
- Максимум 2–3 предложения
- Без банальщины и морализаторства
- Вызывает узнавание и желание переслать
- Язык: русский
- Текст должен резонировать с тем, что изображено на фото

Формат:
Текст мотивации.
Если есть автор — напиши на отдельной строке: "— Имя Автора"
`.trim()
}

function buildMotivationTextOnlyPrompt() {
  return `
Ты — генератор утренней мотивации для Telegram-бота.

Сгенерируй ОДНУ короткую мотивационную фразу.

Стиль (чередуй):
- Резкие наблюдения, парадоксы, неприятная правда о жизни, деньгах, мышлении
- Цитаты в стиле известных людей (философы, предприниматели, учёные) — с указанием автора

Правила:
- Максимум 2–3 предложения
- Без банальщины и морализаторства
- Вызывает узнавание и желание переслать
- Язык: русский

Формат:
Текст мотивации.
Если есть автор — напиши на отдельной строке: "— Имя Автора"
`.trim()
}

function parseMotivationText(rawText) {
  if (!rawText || typeof rawText !== 'string') return { text: null, author: '' }
  const trimmed = rawText.trim()
  const authorRegex = /\n\s*—\s*(.+)$/
  const authorMatch = trimmed.match(authorRegex)
  if (authorMatch) {
    const authorName = authorMatch[1].trim()
    const textWithoutAuthor = trimmed.replace(authorRegex, '').trim()
    return { text: textWithoutAuthor, author: authorName }
  }
  return { text: trimmed, author: '' }
}

export async function generateMotivationForImage(imageBuffer) {
  if (!GEMINI_KEYS.length) {
    console.warn('[geminiMotivation] GEMINI_API_KEY not set, skipping vision')
    return null
  }

  if (!imageBuffer || !Buffer.isBuffer(imageBuffer) || imageBuffer.length < 1000) {
    console.warn('[geminiMotivation] Invalid or too small imageBuffer for vision')
    return null
  }

  const key = nextKey()
  const encodedModel = encodeURIComponent(GEMINI_TEXT_MODEL)
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodedModel}:generateContent?key=${encodeURIComponent(key)}`

  const imageBase64 = imageBuffer.toString('base64')

  const body = {
    contents: [
      {
        role: 'user',
        parts: [
          {
            inlineData: {
              mimeType: 'image/jpeg',
              data: imageBase64
            }
          },
          { text: buildVisionMotivationPrompt() }
        ]
      }
    ]
  }

  let res
  try {
    res = await axios.post(url, body, { timeout: GEMINI_MOTIVATION_TIMEOUT_MS })
  } catch (httpError) {
    console.error('[geminiMotivation] HTTP error in vision request', {
      status: httpError.response?.status,
      message: httpError.message
    })
    return null
  }

  const parts = res?.data?.candidates?.[0]?.content?.parts || []
  const rawText = parts.map(p => p.text || '').join('').trim()

  if (!rawText) {
    console.warn('[geminiMotivation] Empty vision response')
    return null
  }

  const { text, author } = parseMotivationText(rawText)
  return { text, author, source: 'gemini_vision' }
}

export async function generateMotivationTextOnly() {
  if (!GEMINI_KEYS.length) {
    console.warn('[geminiMotivation] GEMINI_API_KEY not set, skipping text-only')
    return null
  }

  const key = nextKey()
  const encodedModel = encodeURIComponent(GEMINI_TEXT_MODEL)
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodedModel}:generateContent?key=${encodeURIComponent(key)}`

  const body = {
    contents: [
      {
        role: 'user',
        parts: [{ text: buildMotivationTextOnlyPrompt() }]
      }
    ]
  }

  let res
  try {
    res = await axios.post(url, body, { timeout: 30000 })
  } catch (httpError) {
    console.error('[geminiMotivation] HTTP error generating text-only', {
      status: httpError.response?.status,
      message: httpError.message
    })
    return null
  }

  const parts = res?.data?.candidates?.[0]?.content?.parts || []
  const rawText = parts.map(p => p.text || '').join('').trim()

  if (!rawText) {
    console.warn('[geminiMotivation] Empty text-only response')
    return null
  }

  const { text, author } = parseMotivationText(rawText)
  return { text, author, source: 'gemini_text' }
}
