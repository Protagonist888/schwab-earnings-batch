// schwab-earnings-batch/process-earnings.js
// ============================================================================
// fix/earnings-ingestion — root cause fixes:
//
//   1. REMOVED hard MAX_BATCHES_PER_RUN=10 cap (was silently dropping symbols
//      at position 9,001+ in an arbitrary ordering — confirmed root cause of
//      BKE/MRVL missing from Redis).
//
//   2. REPLACED per-symbol fundamentals calls with EODHD Screener API.
//      getAllSymbols() issues paginated screener calls with
//      market_capitalization > 300M and exchange = US filters applied
//      server-side. Returns symbols already sorted by market cap descending.
//      Universe build: ~10,000 per-symbol calls → ~15-20 screener calls
//      (5 API credits each = ~100 credits total, negligible vs 100k/day).
//
//      CONFIRMED from live screener response:
//        - Response shape: { "data": [ {...}, ... ] }  (no "total" wrapper key)
//        - Exchange field value: "US" (not "NYSE"/"NASDAQ" — EODHD normalizes
//          all US listings to "US"). Filter must use exchange = "US".
//        - Market cap field: "market_capitalization" as raw USD integer string.
//        - Ticker field: "code".
//
//   3. ADDED $300M market cap floor (P1A-1 universe decision). Matches
//      sympathy earnings peer mapping universe — defined once, used everywhere.
//
//   4. SORTED by market cap descending. MRVL ($58B) and BKE ($700M) land
//      early in the queue. Any partial run failure drops the smallest
//      qualifying symbols, not mid/large caps.
//
//   5. SKIP ETFs/Funds. The screener market_capitalization filter excludes
//      most ETFs naturally (they report $0/null cap). Residual ETF-like
//      tickers with non-null cap are caught by the type field guard and
//      by processSymbol()'s empty-earnings-array check.
//
//   6. ONLY write to Redis when nextDate is non-null. Symbols with no
//      upcoming earnings get no Redis key — proxy returns 404, trade.js
//      renders no catalyst card (correct). Null writes wasted space and
//      obscured genuine cache misses.
//
//   7. INCREASED TTL from 30 days (2,592,000s) to 45 days (3,888,000s).
//      Weekly batch + 45d TTL = two full missed-run safety buffers.
//
// Do-not-touch list (priming doc §10 — all unchanged):
//   - api/earnings.js proxy reader — shape and Redis key pattern unchanged
//   - date_utils.js — no changes
//   - Redis key format: earnings:{SYMBOL} — unchanged
//   - Redis value shape — unchanged
// ============================================================================

require('dotenv').config();
const https = require('https');
const { Redis } = require('@upstash/redis');
const { DateUtils } = require('./date_utils');

// ============================================================================
// CONFIGURATION
// ============================================================================

const EODHD_API_KEY = process.env.EODHD_API_KEY;

// Market cap floor — P1A-1 universe decision ($300M USD).
const MARKET_CAP_FLOOR = 300_000_000;

// EODHD screener returns max 100 results per request (5 API credits each).
const SCREENER_PAGE_SIZE = 100;

// Main earnings batch sizing.
// processSymbol() makes 2 EODHD calls (calendar + price history) per symbol.
// 450 symbols × 2 calls + 70ms micro-delay ≈ 900 calls over ~31s — safe
// under the 1,000/min EODHD rate limit.
const BATCH_SIZE = 450;
const BATCH_DELAY_MS = 75_000;   // 75s between batches

// Redis TTL: 45 days = two missed-run safety buffers (batch is weekly).
const REDIS_TTL_SECONDS = 3_888_000;

// Abort if universe is implausibly small (API key issue, plan problem, etc.)
const MIN_SYMBOL_COUNT = 200;

// ============================================================================
// REDIS
// ============================================================================

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

// ============================================================================
// HTTP HELPERS
// ============================================================================

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Fetch and parse JSON from a URL.
 * Retries on 429 / 5xx with exponential backoff + jitter.
 * Returns { notFound: true } on 404. Throws after maxRetries.
 */
async function fetchJSON(url, attempt = 1, maxRetries = 5) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', async () => {
        if (res.statusCode === 200) {
          try {
            resolve(JSON.parse(data));
          } catch (e) {
            reject(new Error(`Invalid JSON: ${data.substring(0, 80)}`));
          }
        } else if (res.statusCode === 404) {
          resolve({ notFound: true });
        } else if (res.statusCode === 429 || res.statusCode >= 500) {
          if (attempt < maxRetries) {
            const delay = Math.pow(2, attempt) * 1000 + Math.random() * 500;
            console.log(`  Rate limit (${res.statusCode}). Retry ${attempt}/${maxRetries} in ${Math.round(delay / 1000)}s`);
            await sleep(delay);
            resolve(fetchJSON(url, attempt + 1, maxRetries));
          } else {
            reject(new Error(`API returned ${res.statusCode} after ${maxRetries} retries`));
          }
        } else {
          reject(new Error(`API returned ${res.statusCode}: ${data.substring(0, 80)}`));
        }
      });
      res.on('error', reject);
    }).on('error', reject);
  });
}

// ============================================================================
// UNIVERSE BUILDING — EODHD Screener (single-pass, paginated)
// ============================================================================

/**
 * Build the filtered symbol universe using the EODHD Screener API.
 *
 * Key facts confirmed from live screener response:
 *   - Response shape: { "data": [ {...}, ... ] }  — no "total" field
 *   - Exchange field value is "US" for all US listings (NYSE + NASDAQ both
 *     normalize to "US" in EODHD). Filter with exchange = "US" — NOT
 *     "NYSE" or "NASDAQ" which return nothing from this endpoint.
 *   - Pagination stop: when data.length < SCREENER_PAGE_SIZE (last page).
 *   - Market cap field: "market_capitalization" — raw USD integer (not millions).
 *   - Ticker field: "code".
 *
 * API cost: ~15-20 pages × 5 credits = ~75-100 credits total.
 *
 * @returns {Promise<string[]>} Tickers sorted by market cap descending.
 */
async function getAllSymbols() {
  console.log('=== UNIVERSE BUILD START (EODHD Screener) ===');
  console.log(`  Market cap floor: $${(MARKET_CAP_FLOOR / 1_000_000).toFixed(0)}M`);
  console.log(`  Exchange filter:  US (NYSE + NASDAQ normalized by EODHD)`);
  console.log(`  Sort:             market_capitalization descending`);
  console.log('');

  // EODHD screener filters param — JSON array of [field, operation, value].
  // exchange = "US" covers all US-listed equities (NYSE + NASDAQ).
  // market_capitalization filter is in raw USD (not millions).
  const filters = JSON.stringify([
    ['market_capitalization', '>', MARKET_CAP_FLOOR],
    ['exchange',              '=', 'US']
  ]);

  const symbolsWithCap = [];  // [{ symbol, marketCap }]
  let offset  = 0;
  let pageNum = 0;

  while (true) {
    pageNum++;

    const url = [
      'https://eodhd.com/api/screener',
      `?api_token=${EODHD_API_KEY}`,
      `&filters=${encodeURIComponent(filters)}`,
      `&sort=market_capitalization.desc`,
      `&limit=${SCREENER_PAGE_SIZE}`,
      `&offset=${offset}`,
      `&fmt=json`
    ].join('');

    let response;
    try {
      response = await fetchJSON(url);
    } catch (err) {
      // A single page failure is non-fatal if we already have results.
      // Log and break — we'll process whatever universe we've built so far.
      console.error(`  Page ${pageNum} fetch error: ${err.message}`);
      if (symbolsWithCap.length === 0) {
        console.error('  CRITICAL: Failed on first page with zero symbols. Aborting.');
        process.exit(1);
      }
      console.warn(`  Stopping pagination early. Will process ${symbolsWithCap.length} symbols collected so far.`);
      break;
    }

    // Confirmed response shape: { "data": [...] }
    if (!response || !Array.isArray(response.data)) {
      console.warn(`  Page ${pageNum}: unexpected response shape (no data array). Stopping pagination.`);
      console.warn(`  Raw response keys: ${response ? Object.keys(response).join(', ') : 'null'}`);
      break;
    }

    const page = response.data;

    for (const item of page) {
      const symbol    = item.code;
      const marketCap = parseFloat(item.market_capitalization) || 0;

      if (!symbol) continue;

      // Skip ETFs and funds if the type field is present and explicit.
      // Most ETFs are already excluded by the market_capitalization filter
      // (they report $0 or null cap) but some leveraged ETFs have non-zero
      // cap figures that slip through.
      if (item.type) {
        const t = item.type.toUpperCase();
        if (t.includes('ETF') || t.includes('FUND')) continue;
      }

      symbolsWithCap.push({ symbol, marketCap });
    }

    console.log(
      `  Page ${pageNum}: ${page.length} results | ` +
      `running total: ${symbolsWithCap.length} | ` +
      `offset: ${offset}`
    );

    // Stop when we receive fewer results than the page size — last page.
    if (page.length < SCREENER_PAGE_SIZE) {
      console.log(`  Last page reached (${page.length} < ${SCREENER_PAGE_SIZE}).`);
      break;
    }

    offset += SCREENER_PAGE_SIZE;
    await sleep(500); // 500ms between screener pages — polite pacing
  }

  if (symbolsWithCap.length < MIN_SYMBOL_COUNT) {
    console.error(`CRITICAL: Only ${symbolsWithCap.length} symbols after screener — below minimum ${MIN_SYMBOL_COUNT}. Check API key/plan. Aborting.`);
    process.exit(1);
  }

  // The screener already returns results sorted by market_cap desc per page,
  // but after merging all pages we re-sort to guarantee global ordering.
  symbolsWithCap.sort((a, b) => b.marketCap - a.marketCap);

  const finalSymbols = symbolsWithCap.map(s => s.symbol);

  console.log('');
  console.log('=== UNIVERSE BUILD COMPLETE ===');
  console.log(`  Final universe: ${finalSymbols.length} symbols`);
  console.log(`  Top 10:    ${finalSymbols.slice(0, 10).join(', ')}`);
  console.log(`  Bottom 10: ${finalSymbols.slice(-10).join(', ')}`);
  console.log('');

  return finalSymbols;
}

// ============================================================================
// PER-SYMBOL PROCESSOR
// ============================================================================

/**
 * Fetch earnings calendar + price history for one symbol, compute the
 * average earnings move from past events, find the next upcoming earnings
 * date, and write the result to Redis.
 *
 * Only writes to Redis when nextDate is non-null. Symbols with no upcoming
 * earnings are skipped — the proxy returns 404 for cache misses and
 * trade.js renders no catalyst card (correct, not an error).
 *
 * @param {string} symbol - Uppercase ticker
 * @returns {Promise<object|null>} Written result object, or null if skipped/errored
 */
async function processSymbol(symbol) {
  // 2 years back for price history / avgMove calculation;
  // 1 year forward to capture the next earnings date.
  const dateRange = DateUtils.getDateRange(730, 365);

  const calendarUrl = [
    'https://eodhd.com/api/calendar/earnings',
    `?api_token=${EODHD_API_KEY}`,
    `&symbols=${symbol}.US`,
    `&from=${dateRange.from}`,
    `&to=${dateRange.to}`,
    `&fmt=json`
  ].join('');

  try {
    // --- Step 1: Earnings calendar ---
    const earningsData = await fetchJSON(calendarUrl);

    if (earningsData.notFound ||
        !Array.isArray(earningsData.earnings) ||
        earningsData.earnings.length === 0) {
      // No earnings data — expected for ETFs, recent IPOs, some REITs.
      return null;
    }

    // --- Step 2: Find next future earnings date FIRST ---
    // Skip the symbol entirely (no price history call) if no upcoming date.
    // Symbols that just reported won't have a next date yet — they'll be
    // picked up on the following weekly run once EODHD publishes the new date.
    const allEarningsDates = earningsData.earnings
      .map(e => e.report_date)
      .filter(Boolean);

    const nextDate = DateUtils.findNextFutureDate(allEarningsDates);

    if (!nextDate) {
      // No upcoming earnings date — do NOT write to Redis.
      return null;
    }

    // --- Step 3: Price history for avgMove ---
    const priceUrl = [
      `https://eodhd.com/api/eod/${symbol}.US`,
      `?api_token=${EODHD_API_KEY}`,
      `&period=d`,
      `&from=${dateRange.from}`,
      `&to=${DateUtils.formatApiDate(DateUtils.getTodayNormalized())}`,
      `&fmt=json`
    ].join('');

    const priceData = await fetchJSON(priceUrl);

    // --- Step 4: Calculate avgMove from past earnings events ---
    let avgMove = null;

    if (!priceData.notFound && Array.isArray(priceData) && priceData.length >= 10) {
      const moves = [];

      for (const earning of earningsData.earnings) {
        if (!DateUtils.isPastDate(earning.report_date)) continue;

        const earningsDate = DateUtils.parseApiDate(earning.report_date);
        if (!earningsDate) continue;

        const dayBefore = new Date(earningsDate);
        dayBefore.setDate(dayBefore.getDate() - 1);
        const dayAfter = new Date(earningsDate);
        dayAfter.setDate(dayAfter.getDate() + 1);

        const beforePrice = DateUtils.findPriceOnDate(priceData, DateUtils.formatApiDate(dayBefore));
        const afterPrice  = DateUtils.findPriceOnDate(priceData, DateUtils.formatApiDate(dayAfter));

        if (beforePrice && afterPrice && beforePrice > 0) {
          moves.push(Math.abs((afterPrice - beforePrice) / beforePrice) * 100);
        }
      }

      if (moves.length > 0) {
        avgMove = parseFloat(
          (moves.reduce((a, b) => a + b, 0) / moves.length).toFixed(2)
        );
      }
    }
    // avgMove stays null if price history was insufficient — we still write
    // the key so the catalyst card renders with the date (without avgMove).

    // --- Step 5: Write to Redis ---
    const result = {
      symbol,
      nextDate,
      daysUntil:     DateUtils.daysUntil(nextDate),
      formattedDate: DateUtils.formatDisplayDate(nextDate),
      avgMove,
      lastUpdated:   new Date().toISOString(),
      calculatedAt:  DateUtils.formatApiDate(DateUtils.getTodayNormalized()),
    };

    await redis.set(`earnings:${symbol}`, result, { ex: REDIS_TTL_SECONDS });

    console.log(
      `  ✓ ${symbol.padEnd(6)}: next ${nextDate}` +
      ` (${String(result.daysUntil).padStart(3)}d)` +
      ` | avgMove: ${avgMove !== null ? avgMove + '%' : 'N/A'}`
    );
    return result;

  } catch (error) {
    console.error(`  ✗ ${symbol}: ${error.message}`);
    return null;
  }
}

// ============================================================================
// PHASE 3 — PER-SYMBOL EX-DIVIDEND PROCESSOR
// ============================================================================

/**
 * Fetch the next upcoming ex-dividend date for one symbol and (only if a date
 * is found) the matching dividend amount, then write the result to Redis.
 *
 * Two-call pattern — Call 2 only fires when Call 1 confirms an upcoming date:
 *   Call 1 (calendar):          finds the soonest upcoming ex-dividend date.
 *   Call 2 (corporate actions): fires ONLY when Call 1 found a date; supplies
 *                               the per-share value, currency, and period.
 *
 * Write window is broad (45 days forward) so data is warm in Redis before the
 * date crosses the 10-day display threshold. The proxy (trade-analysis.js)
 * enforces the narrow 10-day display window at read time — NOT this batch job.
 *
 * dividendPerShare may be null when the corporate-actions endpoint has no
 * matching record. The Redis write still happens — the proxy and extension
 * handle null gracefully (show the date, omit the dollar impact).
 *
 * Mirrors processSymbol(): same .US suffix, same fetchJSON() error handling,
 * same REDIS_TTL_SECONDS, same "only write when there is a real catalyst"
 * discipline (here: only when an upcoming ex-date exists).
 *
 * @param {string} symbol - Uppercase ticker
 * @returns {Promise<object|null>} Written result object, or null if skipped/errored
 */
async function processDividendSymbol(symbol) {
  // Calendar window: today → +45 days (find the soonest upcoming ex-date).
  // Corporate-actions window: -5 days → +45 days (tolerates feeds that file
  // the record a few days before/around the ex-date) for the date match.
  const calRange = DateUtils.getDateRange(0, 45);
  const caRange  = DateUtils.getDateRange(5, 45);

  // Calendar endpoint uses bracketed filter params; encode the brackets.
  const calendarUrl = [
    'https://eodhd.com/api/calendar/dividends',
    `?api_token=${EODHD_API_KEY}`,
    `&filter%5Bsymbol%5D=${symbol}.US`,
    `&filter%5Bdate_from%5D=${calRange.from}`,
    `&filter%5Bdate_to%5D=${calRange.to}`,
    `&fmt=json`
  ].join('');

  try {
    // --- Call 1: Dividend calendar — find the soonest upcoming ex-date ---
    const calendarData = await fetchJSON(calendarUrl);

    // Response shape: { data: [ { date: 'YYYY-MM-DD', symbol: 'AAPL.US' }, ... ] }
    if (calendarData.notFound ||
        !calendarData.data ||
        !Array.isArray(calendarData.data) ||
        calendarData.data.length === 0) {
      // No upcoming ex-dividend — expected for most symbols in any given week.
      return null;
    }

    const exDates = calendarData.data
      .map(d => d.date)
      .filter(Boolean);

    const exDividendDate = DateUtils.findNextFutureDate(exDates);

    if (!exDividendDate) {
      // No upcoming ex-date — do NOT write to Redis.
      return null;
    }

    // --- Call 2: Corporate actions — get the per-share amount (ONLY now) ---
    // Fires only because Call 1 confirmed an upcoming ex-date. In any given
    // week only a small fraction of the universe qualifies, so this is rare.
    const corpActionsUrl = [
      `https://eodhd.com/api/div/${symbol}.US`,
      `?api_token=${EODHD_API_KEY}`,
      `&from=${caRange.from}`,
      `&to=${caRange.to}`,
      `&fmt=json`
    ].join('');

    let dividendPerShare = null;
    let currency = null;
    let period   = null;

    try {
      const corpData = await fetchJSON(corpActionsUrl);

      // Response shape: [ { date, value, currency, period, ... }, ... ]
      // Match on date === the confirmed upcoming ex-dividend date.
      if (!corpData.notFound && Array.isArray(corpData)) {
        const match = corpData.find(r => r && r.date === exDividendDate);
        if (match) {
          const parsedValue = parseFloat(match.value);
          dividendPerShare = Number.isFinite(parsedValue) ? parsedValue : null;
          currency = match.currency || null;
          period   = match.period  || null;
        }
      }
    } catch (caErr) {
      // Corporate-actions failure is non-fatal — write the date with null amount.
      console.error(`  ⚠ ${symbol} dividend amount lookup failed (non-fatal): ${caErr.message}`);
    }

    // --- Write to Redis (broad 45-day window; proxy filters to 10 at read) ---
    const result = {
      symbol,
      exDividendDate,                              // 'YYYY-MM-DD'
      daysUntil:       DateUtils.daysUntil(exDividendDate),
      dividendPerShare,                            // float, or null if no match
      currency,                                    // e.g. 'USD', or null
      period,                                      // e.g. 'Quarterly', or null
      lastUpdated:     new Date().toISOString(),
    };

    await redis.set(`dividend:${symbol}`, result, { ex: REDIS_TTL_SECONDS });

    console.log(
      `  ✓ ${symbol.padEnd(6)}: ex-div ${exDividendDate}` +
      ` (${String(result.daysUntil).padStart(3)}d)` +
      ` | ${dividendPerShare !== null ? '$' + dividendPerShare + '/sh' : 'amount N/A'}`
    );
    return result;

  } catch (error) {
    console.error(`  ✗ ${symbol} (dividend): ${error.message}`);
    return null;
  }
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const divider = '='.repeat(70);
  console.log(divider);
  console.log('ALPHANUDGE WEEKLY EARNINGS BATCH');
  console.log(`Run started: ${new Date().toISOString()}`);
  console.log(divider);
  console.log('');

  // Validate environment before doing any work
  if (!EODHD_API_KEY) {
    console.error('CRITICAL: EODHD_API_KEY not set. Aborting.');
    process.exit(1);
  }
  if (!process.env.UPSTASH_REDIS_REST_URL || !process.env.UPSTASH_REDIS_REST_TOKEN) {
    console.error('CRITICAL: Upstash Redis credentials not set. Aborting.');
    process.exit(1);
  }

  // ---- Phase 1: Build filtered universe via screener ----
  const SYMBOLS = await getAllSymbols();
  const totalBatches = Math.ceil(SYMBOLS.length / BATCH_SIZE);
  console.log(`Processing ${SYMBOLS.length} symbols across ${totalBatches} batch(es) of up to ${BATCH_SIZE}`);
  console.log('');

  // ---- Phase 2: Process each symbol sequentially ----
  let processed = 0;
  let written   = 0;  // Redis keys actually written
  let skipped   = 0;  // No upcoming date — correctly not written
  let errors    = 0;  // Caught errors in processSymbol

  const startTime = Date.now();

  for (let i = 0; i < SYMBOLS.length; i += BATCH_SIZE) {
    const batch    = SYMBOLS.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;

    console.log(
      `--- Batch ${batchNum}/${totalBatches}: ` +
      `symbols ${i + 1}–${Math.min(i + BATCH_SIZE, SYMBOLS.length)} ---`
    );

    for (const symbol of batch) {
      const result = await processSymbol(symbol);

      if (result !== null) {
        written++;
      } else {
        skipped++;
      }
      processed++;

      await sleep(70); // 70ms micro-delay — keeps EODHD calls spread evenly
    }

    const elapsedMin = ((Date.now() - startTime) / 60_000).toFixed(1);
    console.log(
      `  Batch ${batchNum} done | ` +
      `processed: ${processed}/${SYMBOLS.length} | ` +
      `written: ${written} | skipped: ${skipped} | ` +
      `elapsed: ${elapsedMin}min`
    );

    if (i + BATCH_SIZE < SYMBOLS.length) {
      console.log(`  Waiting ${BATCH_DELAY_MS / 1000}s before next batch...\n`);
      await sleep(BATCH_DELAY_MS);
    }
  }

  // ---- Summary ----
  const totalMin = ((Date.now() - startTime) / 60_000).toFixed(1);
  console.log('');
  console.log(divider);
  console.log('BATCH COMPLETE');
  console.log(`  Universe size:      ${SYMBOLS.length} symbols`);
  console.log(`  Redis keys written: ${written}  (symbols with upcoming earnings)`);
  console.log(`  Skipped:            ${skipped}  (no upcoming date — correct, no write)`);
  console.log(`  Errors:             ${errors}`);
  console.log(`  Total elapsed:      ${totalMin} minutes`);
  console.log(`  Run finished:       ${new Date().toISOString()}`);
  console.log(divider);

  // ---- Phase 3: Ex-dividend processing (reuses the Phase 1 universe) ----
  // Runs AFTER the earnings phase completes. Shares the same symbol universe
  // and infrastructure — one job, one universe build. Each symbol makes at
  // most 2 EODHD calls, and Call 2 fires only when an upcoming ex-date exists,
  // so in practice this phase is far lighter than the earnings phase.
  console.log('');
  console.log(divider);
  console.log('PHASE 3 — EX-DIVIDEND PROCESSING');
  console.log(`  Phase started: ${new Date().toISOString()}`);
  console.log(divider);
  console.log('');

  let divProcessed = 0;
  let divWritten   = 0;  // dividend:{SYMBOL} keys written (upcoming ex-date found)
  let divSkipped   = 0;  // no upcoming ex-date — correctly not written
  const divStartTime = Date.now();

  const divTotalBatches = Math.ceil(SYMBOLS.length / BATCH_SIZE);

  for (let i = 0; i < SYMBOLS.length; i += BATCH_SIZE) {
    const batch    = SYMBOLS.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;

    console.log(
      `--- Dividend Batch ${batchNum}/${divTotalBatches}: ` +
      `symbols ${i + 1}–${Math.min(i + BATCH_SIZE, SYMBOLS.length)} ---`
    );

    for (const symbol of batch) {
      const result = await processDividendSymbol(symbol);

      if (result !== null) {
        divWritten++;
      } else {
        divSkipped++;
      }
      divProcessed++;

      await sleep(70); // same 70ms micro-delay as the earnings phase
    }

    const elapsedMin = ((Date.now() - divStartTime) / 60_000).toFixed(1);
    console.log(
      `  Dividend Batch ${batchNum} done | ` +
      `processed: ${divProcessed}/${SYMBOLS.length} | ` +
      `written: ${divWritten} | skipped: ${divSkipped} | ` +
      `elapsed: ${elapsedMin}min`
    );

    if (i + BATCH_SIZE < SYMBOLS.length) {
      console.log(`  Waiting ${BATCH_DELAY_MS / 1000}s before next dividend batch...\n`);
      await sleep(BATCH_DELAY_MS);
    }
  }

  const divTotalMin = ((Date.now() - divStartTime) / 60_000).toFixed(1);
  console.log('');
  console.log(divider);
  console.log('PHASE 3 COMPLETE');
  console.log(`  Universe size:      ${SYMBOLS.length} symbols`);
  console.log(`  Redis keys written: ${divWritten}  (symbols with upcoming ex-dividend)`);
  console.log(`  Skipped:            ${divSkipped}  (no upcoming ex-date — correct, no write)`);
  console.log(`  Phase 3 elapsed:    ${divTotalMin} minutes`);
  console.log(`  Run finished:       ${new Date().toISOString()}`);
  console.log(divider);
}

main().catch(err => {
  console.error('FATAL: Unhandled error in main():', err);
  process.exit(1);
});