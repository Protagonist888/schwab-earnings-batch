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

// ---------------------------------------------------------------------------
// FINDING D — issuer duplication that ISN'T a ticker-suffix variant.
//
// Observed in production (2026-07-12), peers:WFC:
//   BAC, HSBC, HBCYF, RY, IDCBF, CICHF, CICHY   — "7 peers"
// but HSBC and HBCYF are the SAME issuer (HSBC Holdings: NYSE ADR + OTC
// ordinary), and CICHF/CICHY are the same issuer (China Construction Bank:
// two OTC lines). The real issuer count was 5, not 7. Two peer slots were
// spent naming one company twice.
//
// splitTicker() cannot catch this: HSBC and HBCYF share no ticker root. The
// same mechanism produces GOOG/GOOGL, FOX/FOXA, UA/UAA, MITSF/MITSY.
//
// The discriminator is the COMPANY NAME, which the screener returns and which
// this batch was discarding (see Finding E). "HSBC Holdings plc ADR" and
// "HSBC Holdings plc" share their leading significant tokens; so do
// "Alphabet Inc Class A" and "Alphabet Inc Class C".
//
// TWO INDEPENDENT DEFENSES (deliberate; they fail differently):
//
//   1. ISSUER-NAME DEDUPE (computeStructuralPeers): group in-band candidates by
//      a normalized issuer key derived from `name`; keep ONE line per issuer.
//      This is the only thing that catches a LIQUID duplicate pair such as
//      GOOG/GOOGL, which no volume floor would ever remove.
//
//   2. LIQUIDITY FLOOR (applyCapBand): drop candidates whose average dollar
//      volume is below MIN_DOLLAR_VOLUME. Grey-market OTC lines (HBCYF, CICHF,
//      IDCBF) trade in the thousands of dollars a day. They do not "move in
//      sympathy" with anything on a US earnings print because they barely move
//      at all — nobody is trading them. This catches thin duplicate lines even
//      when the name heuristic misses, and independently closes the
//      illiquid-peer hole (a peer nobody trades is not a tradeable signal).
//
// WHICH LINE SURVIVES DEDUPE: the one with the highest average DOLLAR VOLUME,
// not the highest market cap. Market cap is a property of the ISSUER and is
// therefore near-identical across an issuer's share lines — it cannot rank
// them. Dollar volume is a property of the LINE, and answers the question that
// actually matters: which ticker do people actually trade? That is the one a
// sympathy move would show up in.
// ---------------------------------------------------------------------------

// Minimum average dollar volume (adjusted_close x avgvol_200d) for a candidate
// to be eligible as a peer. 200-day average is used rather than 1-day because
// a single session's volume is noisy (one block trade in an otherwise dead OTC
// line would clear a 1-day floor).
//
// $10M/day is deliberately permissive: it admits genuinely small but real
// listings while excluding grey-market lines by two or more orders of
// magnitude. This is a DIAL — if peer coverage proves too thin, this is the
// first thing to relax, and it can be relaxed safely because unlike the sector
// fallback it does not admit business-unrelated names.
const MIN_DOLLAR_VOLUME = 10_000_000;

// ---------------------------------------------------------------------------
// §4.5 — SAME-DATE COLLISION.
//
// A structural peer that reports on (or within a day of) the traded symbol's
// OWN earnings date is not a "sympathy" catalyst. The user's own earnings print
// dominates the session; a co-reporting peer's move is drowned out by — and
// confounded with — the user's own reaction. Telling a JPM holder that "BAC's
// earnings on Oct 14 may move JPM in sympathy" is misleading when JPM ALSO
// reports Oct 14: JPM will move on its OWN print, not in sympathy with BAC.
//
// The peer is NOT dropped. BAC is still JPM's closest real peer, and a user
// looking at JPM that week wants to see it. Only the FRAMING is wrong. The
// batch therefore sets a `sameDateAsSelf` boolean on the peer and leaves the
// presentation choice to the extension (e.g. "also reports Oct 14" instead of
// "possible sympathy move"). This mirrors how matchLevel is bag-internal data
// the client interprets — the batch states the fact, the UI decides the words.
//
// Tolerance is 1 day, not 0: earnings dates from different sources drift by a
// day (before/after close, timezone of the source), and two banks reporting
// "the morning of the 14th" vs "after close on the 13th" are the same event for
// this purpose. Widening beyond 1 would start flagging genuinely independent
// catalysts in the same reporting week, which we do NOT want to suppress.
const SAME_DATE_TOLERANCE_DAYS = 1;

// Tokens stripped when deriving an issuer key from a company name. These are
// legal-form suffixes, share-class markers and listing-type markers — none of
// them distinguish one ISSUER from another, and all of them are exactly what
// differs between two lines of the SAME issuer ("... plc" vs "... plc ADR",
// "... Inc Class A" vs "... Inc Class C").
const ISSUER_NAME_NOISE = new Set([
  // legal forms
  'inc','incorporated','corp','corporation','co','company','companies',
  'ltd','limited','llc','lp','plc','ag','sa','se','nv','ab','as','oyj',
  'spa','kgaa','gmbh','pte','pty','bhd','sdn','kk','holding','holdings',
  'group','grp','the','and','of',
  // listing / share-class markers
  'adr','ads','ord','ordinary','shares','share','cls','class',
  'sponsored','unsponsored','depositary','receipt','receipts',
  'common','stock','units','unit','new','reg','registered',
  'a','b','c','d',
]);

// Number of leading significant name tokens that define an issuer. Two is the
// deliberate choice:
//   - ONE would over-merge: "First Republic" / "First Horizon" / "First Solar"
//     all collapse to "first" — three unrelated issuers merged into one.
//   - THREE would under-merge: it re-admits tokens that legitimately differ
//     between an issuer's own lines.
// Two tokens correctly separate "hsbc holdings" from "royal bank", while
// correctly merging "hsbc holdings plc adr" with "hsbc holdings plc".
const ISSUER_KEY_TOKENS = 2;

// ---------------------------------------------------------------------------
// FINDING A — non-common-equity share lines must never appear as peers.
//
// Observed in production (2026-07-11): a JPM sympathy card listed BAC, JPM-PC,
// and JPM-PD as "peers". JPM-PC and JPM-PD are JPMorgan's own PREFERRED share
// series — i.e. the traded company itself. The card told the user that JPM's
// earnings might move JPM in sympathy. Circular, and visibly broken.
//
// The old self-exclusion in applyCapBand compared the exact symbol string
// (row.symbol !== selfSymbol), so "JPM-PC" !== "JPM" passed straight through.
//
// TWO INDEPENDENT DEFENSES (deliberate; they fail differently):
//
//   1. UNIVERSE FILTER (getAllSymbols): drop non-common-equity lines outright.
//      Preferred shares, warrants, rights and units do NOT trade on earnings
//      the way common stock does — a preferred line is a fixed-income-like
//      instrument whose price is driven by rates and credit, not by the
//      quarterly print. It is a bad sympathy peer for ANY symbol, not just its
//      own issuer. This is the broad fix.
//
//   2. ISSUER-ROOT SELF-EXCLUSION (applyCapBand): never let a candidate that
//      shares the traded symbol's ISSUER ROOT be its own peer, even if it
//      survived (1). This catches legitimate multi-class COMMON lines, which
//      (1) intentionally does NOT drop — e.g. BRK-A/BRK-B, GOOG/GOOGL are real
//      common shares of one issuer, and must not be peers of each other.
//
// NOTE (Finding D): defense 2 excludes an issuer's other lines from ITS OWN
// peer list, by ticker root. It does NOT stop two lines of a THIRD issuer from
// both appearing in someone else's list (GOOG and GOOGL as peers of META), and
// it does not catch same-issuer lines with unrelated roots (HSBC/HBCYF). That
// is what the Finding D issuer-name dedupe above is for. The two rules are
// complements, not duplicates.
//
// Suffix conventions vary by vendor; EODHD/Schwab US lines use a hyphen:
//   PREFERRED : JPM-PC, JPM-PD, BAC-PB, WFC-PL   (root + "-P" + series letter)
//   WARRANT   : ABC-WT, ABC-W
//   RIGHT     : ABC-R      UNIT: ABC-U
//   CLASS     : BRK-A, BRK-B                     (COMMON — kept by (1), caught by (2))
// Some feeds use "." instead ("BRK.B"); both separators are handled.
// ---------------------------------------------------------------------------

// Suffixes that denote a NON-common-equity line. Class suffixes (A/B/C) are
// deliberately ABSENT: those are real common shares and are handled by the
// issuer-root rule instead, not by dropping them from the universe.
const NON_COMMON_SUFFIXES = new Set([
  'P',                                      // preferred (bare)
  'PA','PB','PC','PD','PE','PF','PG','PH',  // preferred, series A-P
  'PI','PJ','PK','PL','PM','PN','PO','PP',
  'PQ','PR','PS','PT','PU','PV','PW','PX','PY','PZ',
  'W','WT','WS',                            // warrants
  'R','RT',                                 // rights
  'U','UN',                                 // units
]);

/**
 * Split a ticker into its issuer root and suffix.
 * "JPM-PC" -> { root: "JPM", suffix: "PC" };  "BRK.B" -> { root: "BRK", suffix: "B" }
 * "JPM"    -> { root: "JPM", suffix: null }
 *
 * Only the FIRST separator is honored, and only when a non-empty root precedes
 * it, so an oddly-formed symbol degrades to root=itself rather than throwing.
 *
 * @param {string} symbol
 * @returns {{root: string, suffix: ?string}}
 */
function splitTicker(symbol) {
  if (typeof symbol !== 'string' || symbol === '') {
    return { root: '', suffix: null };
  }
  const m = symbol.match(/^([^-.]+)[-.](.+)$/);
  if (!m) {
    return { root: symbol.toUpperCase(), suffix: null };
  }
  return { root: m[1].toUpperCase(), suffix: m[2].toUpperCase() };
}

/**
 * True when a ticker denotes a NON-common-equity line (preferred, warrant,
 * right, unit) that should never enter the peer universe. Multi-class COMMON
 * lines (BRK-A, BRK-B) return FALSE — they are real common shares and are
 * excluded from their own issuer's peer list by the issuer-root rule instead.
 *
 * @param {string} symbol
 * @returns {boolean}
 */
function isNonCommonEquity(symbol) {
  const { suffix } = splitTicker(symbol);
  if (!suffix) return false;
  return NON_COMMON_SUFFIXES.has(suffix);
}

/**
 * Derive a normalized ISSUER KEY from a company name, for grouping share lines
 * of the same company (Finding D). Lowercases, strips punctuation, removes
 * legal-form / share-class / listing-type noise tokens, and keeps the first
 * ISSUER_KEY_TOKENS significant tokens.
 *
 *   "HSBC Holdings plc ADR"   -> "hsbc"           (holdings, plc, adr all noise)
 *   "HSBC Holdings plc"       -> "hsbc"           ... same key, correctly merged
 *   "Alphabet Inc Class A"    -> "alphabet"
 *   "Alphabet Inc Class C"    -> "alphabet"       ... same key, correctly merged
 *   "Bank of America Corp"    -> "bank america"   (of, corp noise)
 *   "JPMorgan Chase & Co"     -> "jpmorgan chase"
 *
 * Returns null when no significant token survives — the caller MUST treat null
 * as "cannot group" and keep the row rather than merging it into a bucket with
 * every other unnamed row. Silently collapsing all no-name rows into one issuer
 * would delete real peers.
 *
 * @param {?string} name
 * @returns {string|null}
 */
function issuerKey(name) {
  if (typeof name !== 'string' || name.trim() === '') return null;

  const tokens = name
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')   // & . , - / ' etc -> space
    .split(/\s+/)
    .filter(t => t !== '' && !ISSUER_NAME_NOISE.has(t));

  if (tokens.length === 0) return null;
  return tokens.slice(0, ISSUER_KEY_TOKENS).join(' ');
}

/**
 * Average DOLLAR volume for a universe row: price x 200-day average share
 * volume. Both fields come free on the screener row (Finding E).
 *
 * Returns 0 when either input is missing or non-finite, which makes an
 * unmeasurable row fail the liquidity floor. That is the intended direction:
 * a candidate we cannot show is liquid is not admitted as a peer.
 *
 * @param {{price: ?number, avgVolume: ?number}} row
 * @returns {number} average dollar volume, or 0 when unmeasurable
 */
function dollarVolume(row) {
  if (!row) return 0;
  const p = Number(row.price);
  const v = Number(row.avgVolume);
  if (!Number.isFinite(p) || !Number.isFinite(v)) return 0;
  if (p <= 0 || v <= 0) return 0;
  return p * v;
}

// Phase 4 (sympathy peers): full universe metadata captured during the Phase 1
// screener build ({ symbol, marketCap, sector, industry }). Populated by
// getAllSymbols() and read only by computePeers(). Kept module-level so Phase 2
// and Phase 3 keep consuming the plain symbol-string array unchanged — their
// signatures and behavior are untouched.
let universeMeta = [];

// Phase 4 diagnostics (Finding D): running total of candidate rows discarded as
// duplicate share lines of an issuer already present in the same peer list
// (HSBC/HBCYF, GOOG/GOOGL, CICHF/CICHY).
//
// Module-level ON PURPOSE. processPeerSymbol() returns a bare outcome STRING
// ('written'|'case1'|'skipped'|'error') that the Phase 4 loop switches on;
// widening that return to an object would force a restructure of the loop and
// of every branch reading it. Accumulating here keeps that contract intact.
//
// MUST be reset at the top of the Phase 4 block (not declared inside the loop —
// a `let` inside the loop body is re-created and zeroed every iteration, never
// accumulates, and is out of scope in the summary).
let peerDupesDropped = 0;

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
 * Key facts CONFIRMED from a live screener response (re-verified 2026-07-14
 * against the raw endpoint — see Finding E; do not trust this list without
 * re-dumping a row, an earlier version of it was wrong):
 *   - Response shape: { "data": [ {...}, ... ] }  — no "total" field
 *   - Exchange field value is "US" for all US listings (NYSE + NASDAQ both
 *     normalize to "US" in EODHD). Filter with exchange = "US" — NOT
 *     "NYSE" or "NASDAQ" which return nothing from this endpoint.
 *   - Pagination stop: when data.length < SCREENER_PAGE_SIZE (last page).
 *   - Ticker field:     "code"                   e.g. "NVDA"
 *   - Company name:     "name"                   e.g. "NVIDIA Corporation"
 *   - Market cap:       "market_capitalization"  raw USD integer (not millions)
 *   - Sector/industry:  "sector" / "industry"    flat strings
 *   - Last price:       "adjusted_close"
 *   - Avg volume:       "avgvol_1d", "avgvol_200d"   (SHARES, not dollars)
 *   - There is NO "type" field. See the ETF note below.
 *
 * FINDING E — fields that were arriving and being THROWN AWAY.
 * This function previously read only code / market_capitalization / sector /
 * industry. The screener was, on the same rows and at the same cost, also
 * returning `name`, `adjusted_close` and `avgvol_200d`. Downstream code had
 * written `name: peer.symbol` with the comment "screener rows carry no company
 * name" — an assumption that was never checked against the payload and was
 * false. Capturing these three fields costs ZERO additional API calls and is
 * what makes the Finding D issuer dedupe and the liquidity floor possible.
 *
 * Fields are captured DEFENSIVELY (null / 0 when absent or unparseable) rather
 * than assumed present, so that if EODHD ever drops one, this degrades to the
 * old behavior instead of throwing. The per-field coverage counters logged at
 * the end of this function exist to surface exactly that.
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

  // [{ symbol, name, marketCap, sector, industry, price, avgVolume }]
  const symbolsWithCap = [];
  let nonCommonSkipped = 0;   // Finding A: preferred/warrant/right/unit lines dropped
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

      // NOTE — there is NO `type` field on this endpoint. A previous version of
      // this loop guarded `if (item.type) { ...skip ETF/FUND... }`, which was
      // dead code: the branch never executed because the property is always
      // undefined. It has been removed rather than left in place looking like a
      // working guard.
      //
      // ETFs are excluded in practice by the market_capitalization filter (funds
      // report $0/null cap and never clear MARKET_CAP_FLOOR). Any leveraged ETF
      // that reports a non-zero cap figure and slips through is now additionally
      // caught downstream: it will carry no `industry` string (peerless), and the
      // liquidity floor / issuer dedupe operate on it like any other row. If ETF
      // contamination is ever OBSERVED in a peer list, the fix is a real one —
      // an explicit exclude-list or a Fundamentals type lookup — not a resurrected
      // check against a field the API does not send.

      // FINDING A, defense 1 — drop NON-COMMON-EQUITY lines (preferred shares,
      // warrants, rights, units). Two reasons:
      //   (i) They are not sympathy peers for ANYONE. A preferred line such as
      //       JPM-PC behaves like fixed income — priced off rates and credit,
      //       not off the quarterly earnings print — so it cannot "move in
      //       sympathy" with a peer's earnings in the way common stock does.
      //  (ii) Observed in production: JPM's peer card listed JPM-PC and JPM-PD,
      //       i.e. the traded company itself, because the old self-exclusion
      //       compared exact symbol strings.
      // Multi-class COMMON lines (BRK-A, BRK-B) are deliberately NOT dropped
      // here — they are genuine common shares. They are prevented from being
      // peers of their own issuer by the issuer-root rule in applyCapBand, and
      // from double-occupying a THIRD symbol's peer list by the Finding D
      // issuer-name dedupe in computeStructuralPeers.
      if (isNonCommonEquity(symbol)) {
        nonCommonSkipped++;
        continue;
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

      // FINDING E — company name. The discriminator for the Finding D issuer
      // dedupe: "HSBC Holdings plc ADR" vs "HSBC Holdings plc" is the ONLY
      // signal that HSBC and HBCYF are one company, since their ticker roots
      // share nothing. Null (not the ticker) when absent — issuerKey() treats
      // null as "cannot group / keep the row", whereas a ticker placeholder
      // would look like a real name and defeat the grouping.
      const name = (typeof item.name === 'string' && item.name.trim() !== '')
        ? item.name.trim() : null;

      // FINDING E — liquidity inputs. `avgvol_200d` is in SHARES; multiplying by
      // `adjusted_close` gives average DOLLAR volume, which is what the peer
      // liquidity floor tests (see dollarVolume() / MIN_DOLLAR_VOLUME).
      //
      // 200-day is used in preference to avgvol_1d because a single session is
      // noisy: one block trade in an otherwise dead OTC line would clear a
      // 1-day floor. Both are stored as null when unparseable so that
      // dollarVolume() returns 0 and the row FAILS the floor — a candidate we
      // cannot prove is liquid is not admitted as a peer.
      const priceRaw     = parseFloat(item.adjusted_close);
      const avgVolumeRaw = parseFloat(item.avgvol_200d);
      const price     = Number.isFinite(priceRaw)     && priceRaw     > 0 ? priceRaw     : null;
      const avgVolume = Number.isFinite(avgVolumeRaw) && avgVolumeRaw > 0 ? avgVolumeRaw : null;

      symbolsWithCap.push({ symbol, name, marketCap, sector, industry, price, avgVolume });
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

  // The screener already returns results sorted by market_cap desc per page,
  // but after merging all pages we re-sort to guarantee global ordering.
  symbolsWithCap.sort((a, b) => b.marketCap - a.marketCap);

  const finalSymbols = symbolsWithCap.map(s => s.symbol);

  // Phase 4 (sympathy peers): stash the full sorted universe metadata for
  // computePeers(). Phase 2/3 still receive `finalSymbols` (plain strings) —
  // this is a side-channel that changes nothing about the existing return.
  universeMeta = symbolsWithCap;

  // Quick coverage signal: how many rows actually carried each optional field.
  // If EODHD ever stops returning one, this surfaces it immediately in the log
  // instead of silently degrading peer quality weeks later. `name` and
  // `avgVolume` are load-bearing for Finding D / the liquidity floor — a sharp
  // drop in either means peers will start duplicating issuers or vanishing.
  const withSector    = symbolsWithCap.filter(s => s.sector).length;
  const withIndustry  = symbolsWithCap.filter(s => s.industry).length;
  const withName      = symbolsWithCap.filter(s => s.name).length;
  const withLiquidity = symbolsWithCap.filter(s => dollarVolume(s) > 0).length;

  console.log(`  Non-common lines skipped (pref/warrant/right/unit): ${nonCommonSkipped}`);
  console.log(`  With sector:    ${withSector} | with industry: ${withIndustry}`);
  console.log(`  With name:      ${withName} | with liquidity data: ${withLiquidity}`);

  if (withName < symbolsWithCap.length * 0.9) {
    console.warn(`  ⚠ name coverage is ${withName}/${symbolsWithCap.length} — issuer dedupe (Finding D) will be degraded.`);
  }
  if (withLiquidity < symbolsWithCap.length * 0.9) {
    console.warn(`  ⚠ liquidity coverage is ${withLiquidity}/${symbolsWithCap.length} — peers will be dropped by the floor. Check adjusted_close/avgvol_200d.`);
  }

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
 * Apply the per-row peer eligibility filters:
 *   1. ISSUER-ROOT self-exclusion  (Finding A, defense 2)
 *   2. Market-cap band             [C/ratio, C*ratio], cap > 0
 *   3. Liquidity floor             (Finding D, defense 2)
 *
 * All three are ROW-LOCAL tests — each candidate is judged on its own, with no
 * reference to the other candidates. That is why the Finding D issuer DEDUPE is
 * NOT here: dedupe is a SET operation (it compares candidates to each other) and
 * lives in computeStructuralPeers().
 *
 * @param {Array<{symbol,name,marketCap,price,avgVolume}>} candidates
 * @param {string} selfSymbol
 * @param {number} C   traded symbol's market cap (> 0)
 * @returns {Array} eligible candidates, all lines of self excluded
 */
function applyCapBand(candidates, selfSymbol, C) {
  const lo = C / PEER_CAP_RATIO;
  const hi = C * PEER_CAP_RATIO;

  // ISSUER-ROOT self-exclusion. The old test was `row.symbol !== selfSymbol`,
  // which let JPM-PC and JPM-PD through as "peers" of JPM ("JPM-PC" !== "JPM"
  // is true). Comparing ISSUER ROOTS excludes every share line of the traded
  // issuer — preferred, warrant, or a second common class (BRK-A vs BRK-B).
  // A company can never be its own sympathy peer.
  //
  // NOTE: this is a TICKER-ROOT test and only catches self-lines that share a
  // root. It does NOT catch a same-issuer line with an unrelated root (the
  // HSBC/HBCYF shape). For the traded symbol itself that gap is closed by the
  // issuer-NAME check below; for THIRD-party issuers it is closed by the dedupe
  // in computeStructuralPeers().
  const selfRoot = splitTicker(selfSymbol).root;

  // Issuer-NAME self-exclusion (Finding D, applied to SELF). Belt-and-braces on
  // top of the root test: if the traded symbol is HSBC and the universe also
  // carries HBCYF, the root test passes HBCYF through (no shared root) and the
  // card would list the company as its own peer under a different ticker. The
  // name key catches it. Null-safe: when either name is missing, issuerKey
  // returns null and this test is skipped rather than merging unnamed rows.
  const selfMeta = candidates.find(r => r && r.symbol === selfSymbol);
  const selfKey  = selfMeta ? issuerKey(selfMeta.name) : null;

  return candidates.filter(row => {
    if (!row || typeof row.symbol !== 'string') return false;

    // 1. Never the traded issuer itself — by ticker root...
    if (splitTicker(row.symbol).root === selfRoot) return false;

    // ...or by issuer name, when both names are known.
    if (selfKey !== null) {
      const k = issuerKey(row.name);
      if (k !== null && k === selfKey) return false;
    }

    // 2. Market-cap band.
    if (!(row.marketCap > 0))    return false;
    if (row.marketCap < lo)      return false;
    if (row.marketCap > hi)      return false;

    // 3. LIQUIDITY FLOOR (Finding D, defense 2). A peer nobody trades is not a
    //    tradeable signal. Grey-market OTC lines (HBCYF ~$10k/day, CICHF,
    //    IDCBF) clear the cap band trivially — market cap is a property of the
    //    ISSUER, so an OTC line of a $340B bank "has" a $340B cap while trading
    //    four figures a day. Dollar volume is a property of the LINE, and is
    //    the only field here that can tell them apart.
    //
    //    Fails CLOSED: dollarVolume() returns 0 when price or volume is missing
    //    or unparseable, so a row we cannot PROVE is liquid is not admitted.
    if (dollarVolume(row) < MIN_DOLLAR_VOLUME) return false;

    return true;
  });
}

/**
 * Collapse candidate rows that are the SAME ISSUER down to a single line
 * (Finding D, defense 1).
 *
 * Observed in production: peers:WFC listed HSBC and HBCYF (both HSBC Holdings)
 * and CICHF and CICHY (both China Construction Bank) — 7 "peers" that were
 * really 5 companies. Two of eight peer slots named one company twice.
 *
 * WHICH LINE SURVIVES: the one with the highest average DOLLAR VOLUME. Market
 * cap CANNOT rank an issuer's lines against each other — it is a property of
 * the issuer and is near-identical across them (HSBC and HBCYF both report
 * ~$340B). Dollar volume is a property of the LINE and answers the question
 * that actually matters: which ticker do people actually trade? A sympathy move
 * shows up in the liquid line, not in the grey-market one.
 *
 * NULL CONTRACT — load-bearing. issuerKey() returns null when a row has no
 * usable name. Such rows are KEPT UNCONDITIONALLY and are never grouped. The
 * naive implementation buckets them all under a single falsy key and dedupes
 * them down to ONE row — silently deleting real, distinct peers and making
 * coverage WORSE than before the fix. If name coverage ever degrades (see the
 * withName warning in getAllSymbols), this path is what keeps the failure
 * graceful instead of destructive.
 *
 * Input order is not relied upon. Ties in dollar volume are broken by market
 * cap, then by symbol, so the output is deterministic run-to-run.
 *
 * @param {Array<{symbol,name,marketCap,price,avgVolume}>} candidates
 * @returns {{ deduped: Array, dropped: number }}
 */
function dedupeByIssuer(candidates) {
  const best     = new Map();  // issuerKey -> winning row
  const ungroup  = [];         // rows with no usable name — never merged
  let   dropped  = 0;

  for (const row of candidates) {
    const key = issuerKey(row.name);

    // NULL CONTRACT: cannot identify the issuer -> keep the row as-is.
    if (key === null) {
      ungroup.push(row);
      continue;
    }

    const incumbent = best.get(key);
    if (!incumbent) {
      best.set(key, row);
      continue;
    }

    // Same issuer, two lines. Keep the one that actually trades.
    dropped++;
    if (isMoreTradedLine(row, incumbent)) {
      best.set(key, row);
    }
  }

  return { deduped: [...best.values(), ...ungroup], dropped };
}

/**
 * True when line `a` is the better-traded line of an issuer than line `b`.
 * Ordered: dollar volume DESC, then market cap DESC, then symbol ASC. The
 * trailing symbol compare exists purely to make the outcome deterministic when
 * two lines are otherwise indistinguishable — without it, the surviving ticker
 * could flip between runs on the same data.
 *
 * @returns {boolean}
 */
function isMoreTradedLine(a, b) {
  const dva = dollarVolume(a);
  const dvb = dollarVolume(b);
  if (dva !== dvb) return dva > dvb;

  const ca = a.marketCap || 0;
  const cb = b.marketCap || 0;
  if (ca !== cb) return ca > cb;

  return a.symbol < b.symbol;
}

/**
 * Compute the peer bucket for a single symbol using the pre-built index.
 * Pure/local — no I/O. Returns the structural peer set (before the earnings
 * join) plus the matchLevel, or a structurally-peerless marker.
 *
 * PIPELINE ORDER IS DELIBERATE:
 *   industry bucket -> applyCapBand (row-local) -> dedupeByIssuer (set) -> slice
 *
 * Dedupe MUST run BEFORE the top-PEER_LIST_MAX slice. If it ran after, duplicate
 * lines would already have EVICTED real peers from the list before being
 * removed from it — WFC would spend 2 of its 8 slots on HSBC/HBCYF and
 * CICHF/CICHY, push 2 genuine banks out, and only then collapse to 5 entries.
 * Filtering first and slicing last is what makes the slots go to real companies.
 *
 * @returns {{ matchLevel: 'industry'|null, peers: Array }}
 *   peers is the top-PEER_LIST_MAX eligible, issuer-deduped structural peer rows
 *   (market-cap desc), self excluded. Empty array => structurally peerless
 *   (case-1 candidate).
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

  // Row-local eligibility: self-exclusion, cap band, liquidity floor.
  const eligible = applyCapBand(index.byIndustry.get(row.industry), row.symbol, C);

  if (eligible.length === 0) {
    // Structurally peerless: no same-industry name survives the filters. This is
    // a CASE 1 candidate — the extension shows "no comparable peers". We do NOT
    // fall back to sector; a wrong peer is worse than no peer.
    return { matchLevel: null, peers: [] };
  }

  // Set-level: collapse multiple share lines of one issuer to the traded line.
  const { deduped, dropped } = dedupeByIssuer(eligible);

  // Defensive: dedupe can only ever SHRINK the set, and only when it found a
  // duplicate — so it can never empty a non-empty set. Guarded anyway, because
  // a silent [] here would be misread downstream as case-1 ("no comparable
  // peers") when the truth would be "a bug ate the peers".
  if (deduped.length === 0) {
    console.warn(`  ⚠ ${row.symbol}: dedupeByIssuer emptied a non-empty candidate set (${eligible.length} in). This is a bug — treating as peerless.`);
    return { matchLevel: null, peers: [], dupesDropped: dropped };
  }

  // Trim to the top PEER_LIST_MAX by market cap descending. Market cap is the
  // right SELECTION criterion (bigger same-industry names are the more material
  // sympathy movers), even though the stored/display ORDER is soonest-earnings
  // first — that ordering is applied later, in processPeerSymbol(), once each
  // peer's earnings date is known.
  deduped.sort((a, b) => b.marketCap - a.marketCap);
  const top = deduped.slice(0, PEER_LIST_MAX);

  // matchLevel is always "industry" now (or null when peerless). Retained in the
  // stored block for schema compatibility and analytics; the extension treats it
  // as bag-internal.
  return { matchLevel: 'industry', peers: top, dupesDropped: dropped };
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
 * Read the traded symbol's OWN next earnings date from its earnings:{SYMBOL}
 * cache, for the same-date collision check (§4.5). Unlike
 * getPeerNextEarningsDate(), this does NOT reject a past date: we want the
 * symbol's own date whatever it is, purely to compare it against each peer's.
 * A null return (no cache, malformed, or unparseable) simply disables the
 * collision check for this symbol — peers are then written unflagged, which is
 * the safe default (we never invent a collision we cannot prove).
 *
 * @param {string} symbol
 * @returns {Promise<string|null>} "YYYY-MM-DD" or null
 */
async function getOwnNextEarningsDate(symbol) {
  try {
    const cached = await redis.get(`earnings:${symbol}`);
    if (!cached || typeof cached !== 'object') return null;
    const nextDate = cached.nextDate;
    if (!DateUtils.isValidDateFormat(nextDate)) return null;
    return nextDate;
  } catch (err) {
    console.error(`  ⚠ own earnings read failed for ${symbol} (non-fatal): ${err.message}`);
    return null;
  }
}

/**
 * True when two "YYYY-MM-DD" dates fall within `tolDays` of each other. Used to
 * flag a peer that reports on (or adjacent to) the traded symbol's own earnings
 * date — see SAME_DATE_TOLERANCE_DAYS. Returns false if either date is missing
 * or unparseable (cannot prove a collision => do not flag one).
 *
 * @param {?string} a  "YYYY-MM-DD"
 * @param {?string} b  "YYYY-MM-DD"
 * @param {number}  tolDays  inclusive tolerance in days
 * @returns {boolean}
 */
function datesWithin(a, b, tolDays) {
  if (!DateUtils.isValidDateFormat(a) || !DateUtils.isValidDateFormat(b)) return false;
  // Parse as UTC midnight to avoid any local-timezone / DST drift in the diff.
  const ta = Date.parse(`${a}T00:00:00Z`);
  const tb = Date.parse(`${b}T00:00:00Z`);
  if (!Number.isFinite(ta) || !Number.isFinite(tb)) return false;
  const diffDays = Math.abs(ta - tb) / 86_400_000;
  return diffDays <= tolDays;
}

/**
 * Compute + write peers:{SYMBOL} for one symbol. Reuses the pre-built universe
 * index (structural peers) and reads earnings:{SYMBOL} for each peer's date.
 *
 * @param {{symbol,name,marketCap,sector,industry,price,avgVolume}} row
 *        the traded symbol's universe row
 * @param {object} index  output of buildUniverseIndex()
 * @returns {Promise<'written'|'case1'|'skipped'|'error'>}
 */
async function processPeerSymbol(row, index) {
  const symbol = row.symbol;
  try {
    const { matchLevel, peers: structuralPeers, dupesDropped = 0 } =
      computeStructuralPeers(row, index);

    // Finding D diagnostic. Accumulated even on the case-1 path below: a symbol
    // can have its entire candidate set collapse to duplicates of ONE issuer and
    // still end up peerless, and that is precisely a case worth seeing in the
    // summary rather than losing.
    peerDupesDropped += dupesDropped;

    // CASE 1: structurally peerless — no same-industry name survived the cap
    // band, the liquidity floor, and self-exclusion. Write an explicit
    // qualified-but-empty block so the extension shows the "no comparable peers"
    // note. This is the meaningful empty. (There is no sector fallback: a wrong
    // peer is worse than no peer.)
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

    // The traded symbol's OWN next earnings date, for the §4.5 same-date check.
    // Read once here (not per-peer) — one extra Redis GET per symbol. null when
    // unknown, which simply disables the flag for this symbol (safe default).
    const ownNextEarningsDate = await getOwnNextEarningsDate(symbol);

    // Step 6-7: join each structural peer's upcoming earnings date (Redis read),
    // dropping peers with no upcoming date. Sequential to stay well under any
    // Upstash rate ceiling; the structural set is <= 8, so this is cheap.
    const peersWithEarnings = [];
    for (const peer of structuralPeers) {
      const nextEarningsDate = await getPeerNextEarningsDate(peer.symbol);
      if (!nextEarningsDate) continue;

      // §4.5 — does this peer report on/adjacent to the traded symbol's own
      // date? If so it is a co-reporter, not a sympathy catalyst. Flag, don't
      // drop; the extension chooses the wording. False when the own date is
      // unknown (cannot prove a collision).
      const sameDateAsSelf = datesWithin(nextEarningsDate, ownNextEarningsDate, SAME_DATE_TOLERANCE_DAYS);

      peersWithEarnings.push({
        symbol: peer.symbol,
        // FINDING E — the REAL company name, from the screener row.
        //
        // This previously read `name: peer.symbol` under the comment "screener
        // rows carry no company name; use ticker". That comment was FALSE and
        // was never checked against the payload: the screener returns `name`
        // ("NVIDIA Corporation", "HSBC Holdings plc ADR") on every row, at zero
        // additional cost. The batch was pulling it and throwing it away, then
        // backfilling the ticker into the slot and rationalizing the loss.
        //
        // The fallback to the ticker REMAINS, but is now a genuine last resort
        // for a row EODHD did not name — not the default path. A user-facing
        // card must never render an empty or null company name.
        name: peer.name || peer.symbol,
        marketCap: peer.marketCap,
        nextEarningsDate,              // "YYYY-MM-DD"; extension re-validates + orders
        sameDateAsSelf,               // §4.5: true => co-reporter, NOT a sympathy catalyst
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

    // §4.5 diagnostic: how many of this symbol's peers are co-reporters. Surfaced
    // per-symbol so a symbol whose peers are ALL same-date (a weak card despite a
    // non-empty peer list) is visible in the log rather than looking healthy.
    const sameDateCount = peersWithEarnings.filter(p => p.sameDateAsSelf).length;

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
      ` | nearest ${peersWithEarnings[0].nextEarningsDate}` +   // already soonest-first
      (sameDateCount > 0 ? ` | ${sameDateCount} co-reporting` : '')
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
  peerDupesDropped  = 0;  // module-level (see decl); reset per run, NOT re-declared
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
  console.log(`  Dup lines dropped:  ${peerDupesDropped}  (same-issuer share lines collapsed — Finding D)`);
  console.log(`  Phase 4 elapsed:    ${peerTotalMin} minutes`);
  console.log(`  Run finished:       ${new Date().toISOString()}`);
  console.log(divider);
}

main().catch(err => {
  console.error('FATAL: Unhandled error in main():', err);
  process.exit(1);
});