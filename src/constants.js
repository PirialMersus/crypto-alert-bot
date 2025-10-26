// src/constants.js
export const KYIV_TZ = 'Europe/Kyiv';

export const IMAGE_FETCH_HOUR = 6;
export const PREPARE_SEND_HOUR = 7;

export const TICKERS_REFRESH_INTERVAL = 60_000;
export const TICKERS_TTL = 55_000;
export const CACHE_TTL = 20_000;
export const BG_CHECK_INTERVAL = 60_000;

export const POPULAR_COINS = ['BTC','ETH','SOL','BNB','XRP','DOGE'];

export const ENTRIES_PER_PAGE = 20;
export const TELEGRAM_MAX_MESSAGE = 3800;
export const RECENT_SYMBOLS_MAX = 20;

export const DELETE_MENU_LABEL = '❌ Удалить пару № ...';
export const DELETE_LABEL_TARGET_LEN = DELETE_MENU_LABEL.length;

export const INACTIVE_DAYS = 30;
export const DAY_MS = 24 * 60 * 60 * 1000;
export const RETRY_INTERVAL_MS = 5 * 60 * 1000;
export const MAX_RETRY_ATTEMPTS = 12;

export const QUOTE_CAPTION_MAX = 1024;
export const MESSAGE_TEXT_MAX = 4000;

export const MONTH_MS = 30 * DAY_MS;

// === Добавлено для утренней рассылки рыночного отчёта ===
export const MARKET_SEND_HOUR = 7;          // час отправки (Europe/Kyiv)
export const MARKET_SEND_MIN = 30;          // минута отправки
export const MARKET_BATCH_SIZE = 25;        // размер батча при массовой отправке
export const MARKET_BATCH_PAUSE_MS = 400;   // пауза между батчами, мс
