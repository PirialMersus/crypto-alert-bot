// src/surpriseService.js
import {surprisesCollection} from './db/db.js'
import {generateSurprise} from './surpriseGemini.js'

const TTL_MS = 15 * 60 * 1000

export async function getOrCreateSurprise({ forceFresh = false } = {}) {
  const now = Date.now()

  if (!surprisesCollection) {
    for (let databaseCheckAttempts = 0; databaseCheckAttempts < 10; databaseCheckAttempts++) {
      if (surprisesCollection) {
        break;
      }
      await new Promise((resolve) => setTimeout(resolve, 500));
    }
  }

  if (!surprisesCollection) {
    throw new Error('Database connection is not established yet');
  }

  if (!forceFresh) {
    const last = await surprisesCollection.findOne(
      {},
      { sort: { createdAt: -1 } }
    )

    if (last?.createdAt) {
      const ageMs = now - last.createdAt

      if (ageMs < TTL_MS) {
        return {
          surprise: last,
          remainingMs: TTL_MS - ageMs
        }
      }
    }
  }

  const ai = await generateSurprise()

  const doc = {
    createdAt: now,
    category: ai.category,
    content: {
      ru: ai.ru,
      en: ai.en
    }
  }

  await surprisesCollection.insertOne(doc)

  return {
    surprise: doc,
    remainingMs: TTL_MS
  }
}

