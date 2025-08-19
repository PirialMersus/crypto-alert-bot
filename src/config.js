// src/config.js
export const ENTRIES_PER_PAGE = 20;
export const TICKERS_TTL = 10_000;
export const CACHE_TTL = 20_000;
export const BG_CHECK_INTERVAL = 60_000;
export const AXIOS_TIMEOUT = 7_000;
export const AXIOS_RETRIES = 2;
export const POPULAR_COINS = ['BTC','ETH','SOL','BNB','XRP','DOGE'];
export const DELETE_MENU_LABEL = '❌ Удалить пару № ...';
export const IMAGE_SIZE = { w: 1200, h: 800 };
export const QUOTE_RETRY_DELAY_MS = 5 * 60 * 1000; // 5 minutes
export const MAX_QUOTE_ATTEMPTS = 12; // 1 hour attempts
export const CONCURRENCY_SEND = 8;
