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

// Phase 4 (sympathy peers): peers:{SYMBOL} TTL is 7 days (P1A-SYM-5), NOT the
// 45-day earnings/dividend TTL. Peer sets are recomputed every weekly run, and
// a 7-day TTL matched to the cadence is the locked decision (an 8-day buffer was
// flagged but not adopted). Deliberately separate from REDIS_TTL_SECONDS.
const PEERS_TTL_SECONDS = 604_800;

// Phase 4 peer band: peers must fall within [cap/RATIO, cap*RATIO].
//
// FINDING B RESOLUTION — INDUSTRY-ONLY PEERS (supersedes P1A-1 Option C).
// The sector fallback has been REMOVED. It was the sole source of false sympathy
// signals: when a symbol's industry was thin, the old code rebuilt the candidate
// set on the whole SECTOR, which admitted large, business-unrelated same-sector
// names (e.g. for UAL/Airlines it surfaced MMM, Mitsubishi, Itochu, Emerson from
// the Industrials sector). Those peers do not co-move with the traded symbol, so
// a "peer reports soon" card built on them is actively misleading. Coverage
// bought with false signals is negative value.
//
// Peers are now ALWAYS same-industry. To recover the coverage the fallback used
// to provide, the cap band is widened 2.0x -> 3.0x. This admits genuinely-related
// smaller/larger names from the SAME industry (e.g. LUV ~$20B now qualifies for
// UAL ~$42B) instead of unrelated same-size names from a different industry. A
// smaller airline is a far better sympathy proxy for an airline than an
// identically-sized conglomerate.
//
// Symbols with zero in-band same-industry peers now correctly fall to CASE 1
// ("no comparable peers"), which the extension already renders gracefully via
// peerComparisonNote. Going dark honestly beats showing a wrong peer.
//
// FUTURE (paid tier): EODHD Fundamentals exposes GICS (GicSector/GicGroup/
// GicIndustry/GicSubIndustry), enabling a graduated relaxation ladder
// (sub-industry -> industry -> industry group) instead of this single flat
// industry match, plus correlation-ranked peer selection. Deferred: Fundamentals
// is a per-symbol call and would roughly double API cost. The flat screener
// `industry` string is sufficient for the industry-only design and costs $0.
const PEER_CAP_RATIO       = 3.0;
const PEER_LIST_MAX        = 8;       // max peers stored per symbol

// Phase 4 (sympathy peers): full universe metadata captured during the Phase 1
// screener build ({ symbol, marketCap, sector, industry }). Populated by
// getAllSymbols() and read only by computePeers(). Kept module-level so Phase 2
// and Phase 3 keep consuming the plain symbol-string array unchanged — their
// signatures and behavior are untouched.
let universeMeta = [];

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

      // Phase 4 (sympathy peers): capture sector/industry off the SAME screener
      // rows the earnings universe already pulls — +0 EODHD calls. These fields
      // are read only by computePeers(); Phase 2/3 ignore the extra properties.
      // The unfiltered global screen returns these fields per row, so no
      // per-sector Screener loop (and no two-word `match`-vs-`=` filter trap) is
      // needed — we never FILTER on sector/industry, only READ the values.
      const sector   = (typeof item.sector === 'string' && item.sector.trim() !== '')
        ? item.sector.trim() : null;
      const industry = (typeof item.industry === 'string' && item.industry.trim() !== '')
        ? item.industry.trim() : null;

      symbolsWithCap.push({ symbol, marketCap, sector, industry });
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

  // Phase 4 (sympathy peers): stash the full sorted universe metadata for
  // computePeers(). Phase 2/3 still receive `finalSymbols` (plain strings) —
  // this is a side-channel that changes nothing about the existing return.
  universeMeta = symbolsWithCap;

  // Quick coverage signal: how many rows actually carried a sector/industry.
  // If EODHD ever stops returning these on the screener, this surfaces it
  // loudly rather than silently degrading peer coverage.
  const withSector   = symbolsWithCap.filter(s => s.sector).length;
  const withIndustry = symbolsWithCap.filter(s => s.industry).length;

  console.log('');
  console.log('=== UNIVERSE BUILD COMPLETE ===');
  console.log(`  Final universe: ${finalSymbols.length} symbols`);
  console.log(`  With sector:    ${withSector} | with industry: ${withIndustry}`);
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
// PHASE 4 — SYMPATHY-EARNINGS PEER COMPUTATION
// ============================================================================
//
// Computes, for every in-universe symbol, the set of size-comparable peers in
// the SAME INDUSTRY that have an upcoming earnings date, and writes
// peers:{SYMBOL} to Redis for the extension's sympathy-earnings catalyst.
//
// ALL-LOCAL bucketing — ZERO new EODHD calls:
//   - Sector/industry/marketCap come from `universeMeta`, captured off the SAME
//     screener rows the Phase 1 earnings universe already pulled (+0 calls).
//   - Each peer's next earnings date is read back from the earnings:{SYMBOL}
//     keys Phase 2 just wrote (+0 EODHD calls — Redis reads only).
//
// Algorithm (INDUSTRY-ONLY — supersedes P1A-1 Option C / Backend Guide §1.4;
// see the PEER_CAP_RATIO comment for the Finding B rationale):
//   1. Candidate set = universe rows with the SAME industry as S. Industry is
//      MANDATORY — a symbol with no industry is peerless. There is NO sector
//      fallback (removed: it admitted business-unrelated same-sector names).
//   2. Cap band: keep peers within [C/3.0, C*3.0]  (PEER_CAP_RATIO, widened from
//      2.0 to recover the coverage the sector fallback used to provide — but
//      from the RIGHT pool: same-industry names).
//   3. Exclude S itself.
//   4. Sort by market cap descending; take top PEER_LIST_MAX (8). Market cap is
//      the SELECTION criterion (bigger same-industry names are more material
//      movers).
//   5. Join each peer's next earnings date from earnings:{SYMBOL} (Redis).
//   6. Drop peers with no upcoming earnings date.
//   7. Re-sort survivors SOONEST-EARNINGS FIRST, market-cap desc as tiebreaker
//      (catalyst proximity is what makes a peer actionable). Then:
//        - >=1 peer with an upcoming date  -> write full block (qualified:true).
//        - structurally peerless (0 in-band same-industry peers, excluding self)
//                                          -> CASE 1: write {qualified:true,
//                                             peers:[]} (the note fires).
//        - has comparable peers but none reporting soon -> write NO key
//          (silent; NOT case 1 — comparable peers exist, just no imminent
//          catalyst). Distinguishing these two empties is a deliberate decision
//          (confirmed 2026-07-08): the "no comparable peers" note must only fire
//          when there are genuinely no comparable peers, never when peers exist
//          but happen to have no upcoming earnings.
//
// Redis value shape (authoritative — proxy/extension depend on it; Guide §1.6):
//   peers:{SYMBOL} = {
//     symbol, qualified: true, matchLevel: "industry",   // "sector" NO LONGER EMITTED
//     peers: [ { symbol, name, marketCap, nextEarningsDate }, ... up to 8 ],
//     computedAt
//   }
// TTL: PEERS_TTL_SECONDS (7 days). Peer ordering as STORED is soonest-earnings
// first (market-cap desc tiebreak); the extension applies the identical ordering
// client-side after re-validating each date, so the two never disagree.
//
// SCHEMA NOTE: matchLevel remains in the shape for compatibility, but is now
// always "industry" (or null when peerless). Consumers must not assume "sector"
// can appear. The extension treats matchLevel as bag-internal (not displayed).

/**
 * Build an in-memory index of the universe by industry and by sector, plus a
 * per-symbol metadata lookup. Done once, reused for every symbol's bucketing.
 *
 * @param {Array<{symbol,marketCap,sector,industry}>} meta
 * @returns {{ byIndustry: Map<string,Array>, bySector: Map<string,Array>, bySymbol: Map<string,object> }}
 */
function buildUniverseIndex(meta) {
  const byIndustry = new Map();
  const bySector   = new Map();
  const bySymbol   = new Map();

  for (const row of meta) {
    if (!row || typeof row.symbol !== 'string') continue;
    bySymbol.set(row.symbol, row);

    if (row.industry) {
      if (!byIndustry.has(row.industry)) byIndustry.set(row.industry, []);
      byIndustry.get(row.industry).push(row);
    }
    if (row.sector) {
      if (!bySector.has(row.sector)) bySector.set(row.sector, []);
      bySector.get(row.sector).push(row);
    }
  }

  return { byIndustry, bySector, bySymbol };
}

/**
 * Apply the cap band and exclude the traded symbol.
 * Keeps candidates whose marketCap is within [C/ratio, C*ratio] and > 0.
 *
 * @param {Array<{symbol,marketCap}>} candidates
 * @param {string} selfSymbol
 * @param {number} C   traded symbol's market cap (> 0)
 * @returns {Array} in-band candidates, self excluded
 */
function applyCapBand(candidates, selfSymbol, C) {
  const lo = C / PEER_CAP_RATIO;
  const hi = C * PEER_CAP_RATIO;
  return candidates.filter(row =>
    row.symbol !== selfSymbol &&
    row.marketCap > 0 &&
    row.marketCap >= lo &&
    row.marketCap <= hi
  );
}

/**
 * Compute the peer bucket for a single symbol using the pre-built index.
 * Pure/local — no I/O. Returns the structural peer set (before the earnings
 * join) plus the matchLevel, or a structurally-peerless marker.
 *
 * @returns {{ matchLevel: 'industry'|'sector'|null, peers: Array }}
 *   peers is the top-8 in-band structural peer rows (market-cap desc), self
 *   excluded. Empty array => structurally peerless (case-1 candidate).
 */
function computeStructuralPeers(row, index) {
  const C = row.marketCap;

  // No usable cap, or no INDUSTRY classification => cannot bucket. Treat as
  // peerless. NOTE: `sector` alone is no longer sufficient to bucket a symbol —
  // industry is now mandatory, because sector-level peers are exactly the noise
  // this design removes. A symbol with a sector but no industry is peerless.
  if (!(C > 0) || !row.industry) {
    return { matchLevel: null, peers: [] };
  }

  if (!index.byIndustry.has(row.industry)) {
    return { matchLevel: null, peers: [] };
  }

  // Same-industry candidates within the (now 3.0x) cap band. Self-exclusion and
  // the marketCap > 0 guard are handled inside applyCapBand.
  const inBand = applyCapBand(index.byIndustry.get(row.industry), row.symbol, C);

  if (inBand.length === 0) {
    // Structurally peerless: no same-industry name survives the band. This is a
    // CASE 1 candidate — the extension shows "no comparable peers". We do NOT
    // fall back to sector; a wrong peer is worse than no peer.
    return { matchLevel: null, peers: [] };
  }

  // Trim to the top PEER_LIST_MAX by market cap descending. Market cap is the
  // right SELECTION criterion (bigger same-industry names are the more material
  // sympathy movers), even though the stored/display ORDER is soonest-earnings
  // first — that ordering is applied later, in processPeerSymbol(), once each
  // peer's earnings date is known.
  inBand.sort((a, b) => b.marketCap - a.marketCap);
  const top = inBand.slice(0, PEER_LIST_MAX);

  // matchLevel is always "industry" now (or null when peerless). Retained in the
  // stored block for schema compatibility and analytics; the extension treats it
  // as bag-internal.
  return { matchLevel: 'industry', peers: top };
}

/**
 * Read a peer's upcoming earnings date from the earnings:{SYMBOL} key Phase 2
 * wrote. Returns a normalized "YYYY-MM-DD" upcoming date, or null when there is
 * no key, no nextDate, or the date is not in the future. Non-fatal on error.
 *
 * @param {string} peerSymbol
 * @returns {Promise<string|null>}
 */
async function getPeerNextEarningsDate(peerSymbol) {
  try {
    const cached = await redis.get(`earnings:${peerSymbol}`);
    if (!cached || typeof cached !== 'object') return null;

    const nextDate = cached.nextDate;
    if (!DateUtils.isValidDateFormat(nextDate)) return null;

    // Re-validate freshness at compute time — the cached daysUntil can be stale.
    // Keep only genuinely upcoming dates (today or later). Past dates mean the
    // peer already reported; it is not an upcoming sympathy catalyst.
    const d = DateUtils.daysUntil(nextDate);
    if (d === null || d < 0) return null;

    return nextDate;
  } catch (err) {
    console.error(`  ⚠ peer earnings read failed for ${peerSymbol} (non-fatal): ${err.message}`);
    return null;
  }
}

/**
 * Compute + write peers:{SYMBOL} for one symbol. Reuses the pre-built universe
 * index (structural peers) and reads earnings:{SYMBOL} for each peer's date.
 *
 * @param {{symbol,marketCap,sector,industry}} row  traded symbol's universe row
 * @param {object} index  output of buildUniverseIndex()
 * @returns {Promise<'written'|'case1'|'skipped'|'error'>}
 */
async function processPeerSymbol(row, index) {
  const symbol = row.symbol;
  try {
    const { matchLevel, peers: structuralPeers } = computeStructuralPeers(row, index);

    // CASE 1: structurally peerless — no comparable peers even after fallback.
    // Write an explicit qualified-but-empty block so the extension shows the
    // "no comparable peers" note. This is the meaningful empty.
    if (structuralPeers.length === 0) {
      const case1 = {
        symbol,
        qualified: true,
        matchLevel: null,
        peers: [],
        computedAt: new Date().toISOString(),
      };
      await redis.set(`peers:${symbol}`, case1, { ex: PEERS_TTL_SECONDS });
      console.log(`  ○ ${symbol.padEnd(6)}: case-1 (no comparable peers) — qualified:[] written`);
      return 'case1';
    }

    // Step 6-7: join each structural peer's upcoming earnings date (Redis read),
    // dropping peers with no upcoming date. Sequential to stay well under any
    // Upstash rate ceiling; the structural set is <= 8, so this is cheap.
    const peersWithEarnings = [];
    for (const peer of structuralPeers) {
      const nextEarningsDate = await getPeerNextEarningsDate(peer.symbol);
      if (!nextEarningsDate) continue;
      peersWithEarnings.push({
        symbol: peer.symbol,
        name: peer.symbol,             // screener rows carry no company name; use ticker
        marketCap: peer.marketCap,
        nextEarningsDate,              // "YYYY-MM-DD"; extension re-validates + orders
      });
    }

    // Has comparable peers but NONE reporting soon -> no key (silent). NOT case
    // 1: comparable peers exist, they just have no imminent catalyst. Writing an
    // empty-qualified block here would misfire the "no comparable peers" note.
    if (peersWithEarnings.length === 0) {
      return 'skipped';
    }

    // ORDERING: soonest-earnings first, market-cap DESC as the tiebreaker.
    // Catalyst proximity is what makes a peer actionable — a peer reporting in
    // 2 days outranks a larger one reporting in 5. Market cap breaks ties only
    // among peers reporting on the SAME date (the bigger name is the more
    // material mover). Dates are zero-padded "YYYY-MM-DD", so lexicographic
    // string compare is a correct chronological compare. The extension applies
    // the identical ordering client-side after re-validating the dates, so the
    // two never disagree.
    peersWithEarnings.sort((a, b) => {
      if (a.nextEarningsDate !== b.nextEarningsDate) {
        return a.nextEarningsDate < b.nextEarningsDate ? -1 : 1;
      }
      return b.marketCap - a.marketCap;
    });

    // >=1 peer with an upcoming earnings date -> write the full block.
    const result = {
      symbol,
      qualified: true,
      matchLevel: matchLevel || null,  // always "industry" now (never "sector")
      peers: peersWithEarnings,        // soonest-first, market-cap desc tiebreak
      computedAt: new Date().toISOString(),
    };
    await redis.set(`peers:${symbol}`, result, { ex: PEERS_TTL_SECONDS });

    console.log(
      `  ✓ ${symbol.padEnd(6)}: ${peersWithEarnings.length} peer(s)` +
      ` [${matchLevel}]` +
      ` | nearest ${peersWithEarnings[0].nextEarningsDate}`   // already soonest-first
    );
    return 'written';

  } catch (error) {
    console.error(`  ✗ ${symbol} (peers): ${error.message}`);
    return 'error';
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

  // ---- Phase 4: Sympathy-earnings peer computation (reuses Phase 1 universe) ----
  // Runs AFTER Phase 2, because it reads back the earnings:{SYMBOL} keys Phase 2
  // wrote to join each peer's next earnings date. ALL-LOCAL bucketing off
  // `universeMeta` (captured during the Phase 1 screener build) + Redis reads of
  // already-written earnings keys — ZERO new EODHD calls for the whole phase.
  console.log('');
  console.log(divider);
  console.log('PHASE 4 — SYMPATHY-EARNINGS PEER COMPUTATION');
  console.log(`  Phase started: ${new Date().toISOString()}`);
  console.log(divider);
  console.log('');

  // Guard: universeMeta must be populated with sector/industry. If it is empty
  // (screener stopped returning classification, or an upstream change), skip the
  // phase loudly rather than writing garbage / empty peer keys everywhere.
  if (!Array.isArray(universeMeta) || universeMeta.length === 0) {
    console.error('  Phase 4 SKIPPED: universeMeta is empty — no sector/industry captured. Peers not computed.');
    console.log(divider);
    return;
  }

  const peerIndex = buildUniverseIndex(universeMeta);
  console.log(
    `  Universe index built: ${peerIndex.byIndustry.size} industries, ` +
    `${peerIndex.bySector.size} sectors, ${peerIndex.bySymbol.size} symbols`
  );
  console.log('');

  let peerProcessed = 0;
  let peerWritten   = 0;  // full peers:{SYMBOL} blocks written (>=1 peer reporting)
  let peerCase1     = 0;  // qualified-but-empty blocks (structurally peerless)
  let peerSkipped   = 0;  // has comparable peers but none reporting — no key
  let peerErrors    = 0;
  const peerStartTime = Date.now();

  const peerTotalBatches = Math.ceil(universeMeta.length / BATCH_SIZE);

  for (let i = 0; i < universeMeta.length; i += BATCH_SIZE) {
    const batch    = universeMeta.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;

    console.log(
      `--- Peer Batch ${batchNum}/${peerTotalBatches}: ` +
      `symbols ${i + 1}–${Math.min(i + BATCH_SIZE, universeMeta.length)} ---`
    );

    for (const row of batch) {
      const outcome = await processPeerSymbol(row, peerIndex);
      if      (outcome === 'written') peerWritten++;
      else if (outcome === 'case1')   peerCase1++;
      else if (outcome === 'skipped') peerSkipped++;
      else                            peerErrors++;
      peerProcessed++;

      // Light micro-delay: Phase 4 is Redis-only (no EODHD), but each symbol
      // does up to 8 reads + 1 write. 20ms keeps well under Upstash limits
      // without the 75s EODHD inter-batch waits the other phases need.
      await sleep(20);
    }

    const elapsedMin = ((Date.now() - peerStartTime) / 60_000).toFixed(1);
    console.log(
      `  Peer Batch ${batchNum} done | ` +
      `processed: ${peerProcessed}/${universeMeta.length} | ` +
      `written: ${peerWritten} | case1: ${peerCase1} | ` +
      `skipped: ${peerSkipped} | errors: ${peerErrors} | ` +
      `elapsed: ${elapsedMin}min`
    );
    // No EODHD inter-batch delay — Phase 4 makes no live external calls.
  }

  const peerTotalMin = ((Date.now() - peerStartTime) / 60_000).toFixed(1);
  console.log('');
  console.log(divider);
  console.log('PHASE 4 COMPLETE');
  console.log(`  Universe size:      ${universeMeta.length} symbols`);
  console.log(`  Peer blocks written:${peerWritten}  (>=1 comparable peer reporting soon)`);
  console.log(`  Case-1 (peerless):  ${peerCase1}  (qualified:[] — "no comparable peers" note)`);
  console.log(`  Skipped (no key):   ${peerSkipped}  (peers exist but none reporting — correct silence)`);
  console.log(`  Errors:             ${peerErrors}`);
  console.log(`  Phase 4 elapsed:    ${peerTotalMin} minutes`);
  console.log(`  Run finished:       ${new Date().toISOString()}`);
  console.log(divider);
}

main().catch(err => {
  console.error('FATAL: Unhandled error in main():', err);
  process.exit(1);
});