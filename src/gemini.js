// src/gemini.js
import 'dotenv/config';
import axios from 'axios';

const GEMINI_KEY = process.env.GEMINI_KEY || process.env.GEMINI_API_KEY || '';
const MODEL = 'gemini-2.5-pro';
const GEMINI_TIMEOUT_MS = process.env.GEMINI_TIMEOUT_MS
  ? parseInt(process.env.GEMINI_TIMEOUT_MS, 10)
  : 120000;

export async function askGemini(prompt) {
  if (!GEMINI_KEY) {
    console.error('[gemini] GEMINI_KEY is not set');
    throw new Error('GEMINI_KEY is not set');
  }

  const url = `https://generativelanguage.googleapis.com/v1beta/models/${MODEL}:generateContent`;

  const text = String(prompt || '');
  const sliced = text;

  const startedAt = new Date().toISOString();
  console.log('[gemini] request start', {
    startedAt,
    url,
    model: MODEL,
    promptLength: text.length,
    promptPreview: sliced.slice(0, 200)
  });

  try {
    const { data, status } = await axios.post(
      url,
      {
        contents: [
          {
            role: 'user',
            parts: [{ text: sliced }]
          }
        ]
      },
      {
        headers: {
          'Content-Type': 'application/json',
          'x-goog-api-key': GEMINI_KEY
        },
        timeout: GEMINI_TIMEOUT_MS
      }
    );

    console.log('[gemini] response raw', {
      status,
      hasCandidates: !!(data && Array.isArray(data.candidates)),
      candidatesCount: data && Array.isArray(data.candidates) ? data.candidates.length : 0
    });

    const candidates = data && Array.isArray(data.candidates) ? data.candidates : [];
    if (!candidates.length) {
      console.error('[gemini] empty candidates array', {
        dataPreview: JSON.stringify(data).slice(0, 500)
      });
      throw new Error('Empty response from Gemini');
    }

    const parts = (candidates[0].content && candidates[0].content.parts) || [];
    const answer = parts.map(p => (p && p.text ? p.text : '')).join('\n').trim();

    console.log('[gemini] parsed answer', {
      answerLength: answer.length,
      answerPreview: answer.slice(0, 200)
    });

    if (!answer) {
      console.error('[gemini] no text in first candidate', {
        firstCandidatePreview: JSON.stringify(candidates[0]).slice(0, 500)
      });
      throw new Error('No text in Gemini response');
    }

    return answer;
  } catch (e) {
    const status = e?.response?.status;
    const data = e?.response?.data;

    console.error('[gemini] request error', {
      message: e?.message,
      status,
      dataPreview: data ? JSON.stringify(data).slice(0, 500) : null,
      stack: e?.stack?.split('\n').slice(0, 3).join('\n')
    });

    throw e;
  }
}
