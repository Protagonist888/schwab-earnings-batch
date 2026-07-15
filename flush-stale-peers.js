/**
 * flush-stale-peers.js — remove ORPHANED peers:{SYMBOL} keys from Redis.
 *
 * WHY THIS EXISTS
 * ---------------
 * peers:* keys carry a 7-day TTL. A batch re-run only OVERWRITES the keys it
 * WRITES. Any symbol that the current logic decides NOT to write a key for
 * (outcome 'skipped' — comparable peers exist but none are reporting) keeps its
 * PREVIOUS block until the TTL expires. After a logic change, those survivors are
 * stale: they can still contain preferred lines (JPM-PC), grey-market OTC tickers
 * (HBCYF, CICHF, IDCBF), sector-era noise, or ticker-as-name placeholders. The
 * extension will happily render them. They are ghosts of the old algorithm.
 *
 * WHAT COUNTS AS AN ORPHAN
 * ------------------------
 * A peers:* key is an orphan when it was NOT written by the most recent batch
 * run. That is detected by `computedAt`: every block the batch writes stamps
 * `computedAt` with the run's ISO timestamp. A block older than the cutoff was
 * left behind.
 *
 * WHAT THIS SCRIPT MUST NOT DO
 * ----------------------------
 * It must NOT delete every peers:* key. The last run wrote 868 GOOD blocks, and
 * the next scheduled run is a WEEK away. A blind `DEL peers:*` would take the
 * sympathy-earnings feature offline until then. Only the survivors of the
 * previous run are removed.
 *
 * SAFETY
 * ------
 * DRY RUN BY DEFAULT. Nothing is deleted unless --commit is passed. The dry run
 * prints the full list of keys it would delete plus a sample of their contents,
 * so the decision is made on evidence, not on trust in this script.
 *
 * USAGE
 *   node flush-stale-peers.js                      # dry run, auto-detected cutoff
 *   node flush-stale-peers.js --commit             # actually delete
 *   node flush-stale-peers.js --cutoff=2026-07-14T15:00:00Z
 *   node flush-stale-peers.js --commit --cutoff=2026-07-14T15:00:00Z
 *
 * CUTOFF AUTO-DETECTION
 *   With no --cutoff, the script reads every peers:* block, finds the LATEST
 *   `computedAt` present, and treats anything more than CUTOFF_SLACK_MIN minutes
 *   older than that as belonging to a previous run. The slack absorbs the fact
 *   that a single run takes ~10 minutes and therefore stamps a RANGE of
 *   timestamps, not one instant.
 */

require('dotenv').config();
const { Redis } = require('@upstash/redis');

// A full Phase 4 pass takes ~10 minutes, so the blocks from ONE run carry
// computedAt values spread across that window. Anything within this many minutes
// of the newest block is treated as part of the same run and is KEPT. Set
// generously — the cost of keeping a stale key is one bad card for <7 days; the
// cost of deleting a fresh key is a symbol with NO card for a week.
const CUTOFF_SLACK_MIN = 90;

// Upstash SCAN page size. Keep modest to stay well under request limits.
const SCAN_COUNT = 200;

// Pause between DEL batches — Upstash rate-limits aggressive loops.
const DEL_BATCH   = 25;
const DEL_PAUSE_MS = 100;

const sleep = ms => new Promise(r => setTimeout(r, ms));

const argv    = process.argv.slice(2);
const COMMIT  = argv.includes('--commit');
const cutArg  = argv.find(a => a.startsWith('--cutoff='));
const CUTOFF_OVERRIDE = cutArg ? cutArg.split('=').slice(1).join('=') : null;

if (!process.env.UPSTASH_REDIS_REST_URL || !process.env.UPSTASH_REDIS_REST_TOKEN) {
  console.error('FATAL: UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN not set. Check .env.');
  process.exit(1);
}

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

/**
 * Enumerate every peers:* key via SCAN. KEYS is deliberately avoided — it blocks
 * the server on large keyspaces. SCAN is cursor-based and safe.
 */
async function scanPeerKeys() {
  const keys = [];
  let cursor = '0';
  let pages  = 0;

  do {
    const [next, batch] = await redis.scan(cursor, {
      match: 'peers:*',
      count: SCAN_COUNT,
    });
    cursor = String(next);
    if (Array.isArray(batch)) keys.push(...batch);
    pages++;
    if (pages % 10 === 0) process.stdout.write(`\r  scanning... ${keys.length} keys`);
  } while (cursor !== '0');

  process.stdout.write(`\r  scanned ${pages} pages, found ${keys.length} peers:* keys\n`);
  // SCAN can return duplicates across pages — dedupe.
  return [...new Set(keys)];
}

/**
 * Read the computedAt of one key. Returns null when the key is missing,
 * unparseable, or carries no usable timestamp.
 *
 * A block with NO computedAt is treated as an orphan: every block the current
 * batch writes stamps one, so its absence means the block predates that logic.
 */
async function readComputedAt(key) {
  try {
    const v = await redis.get(key);
    if (!v || typeof v !== 'object') return { ts: null, block: v };
    const ts = typeof v.computedAt === 'string' ? Date.parse(v.computedAt) : NaN;
    return { ts: Number.isFinite(ts) ? ts : null, block: v };
  } catch (err) {
    console.error(`  ⚠ read failed for ${key} (treating as UNKNOWN, will NOT delete): ${err.message}`);
    return { ts: undefined, block: null };  // undefined => read error, distinct from null
  }
}

/** One-line summary of a block, for the dry-run evidence dump. */
function describe(key, block, ts) {
  if (!block) return `${key.padEnd(14)} <unreadable>`;
  const when  = ts ? new Date(ts).toISOString() : 'NO computedAt';
  const peers = Array.isArray(block.peers) ? block.peers : [];
  const names = peers.map(p => p && p.symbol).filter(Boolean);
  // Surface the exact pathologies this flush is meant to clear.
  const flags = [];
  if (names.some(s => /-P/.test(s)))                       flags.push('PREFERRED');
  if (names.some(s => /^[A-Z]{5}$/.test(s) && /[FY]$/.test(s))) flags.push('OTC-LINE');
  if (block.matchLevel === 'sector')                       flags.push('SECTOR-ERA');
  if (peers.some(p => p && p.name === p.symbol))           flags.push('TICKER-AS-NAME');
  const flagStr = flags.length ? `  [${flags.join(',')}]` : '';
  const peerStr = names.length ? names.join(',') : '(case-1 empty)';
  return `${key.padEnd(14)} ${when}  ${peerStr}${flagStr}`;
}

async function main() {
  const divider = '='.repeat(72);
  console.log(divider);
  console.log('FLUSH STALE peers:* KEYS');
  console.log(`  Mode: ${COMMIT ? '*** COMMIT (WILL DELETE) ***' : 'DRY RUN (nothing will be deleted)'}`);
  console.log(divider);
  console.log('');

  const keys = await scanPeerKeys();
  if (keys.length === 0) {
    console.log('No peers:* keys found. Nothing to do.');
    return;
  }

  console.log('');
  console.log(`Reading computedAt from ${keys.length} keys...`);
  const rows = [];
  for (let i = 0; i < keys.length; i++) {
    const { ts, block } = await readComputedAt(keys[i]);
    rows.push({ key: keys[i], ts, block });
    if ((i + 1) % 100 === 0) process.stdout.write(`\r  read ${i + 1}/${keys.length}`);
  }
  process.stdout.write(`\r  read ${keys.length}/${keys.length}\n`);

  // --- determine the cutoff -------------------------------------------------
  let cutoffMs;
  if (CUTOFF_OVERRIDE) {
    cutoffMs = Date.parse(CUTOFF_OVERRIDE);
    if (!Number.isFinite(cutoffMs)) {
      console.error(`FATAL: --cutoff="${CUTOFF_OVERRIDE}" is not a parseable ISO timestamp.`);
      process.exit(1);
    }
    console.log(`\nCutoff (explicit): ${new Date(cutoffMs).toISOString()}`);
  } else {
    const stamps = rows.map(r => r.ts).filter(t => typeof t === 'number' && t !== null);
    if (stamps.length === 0) {
      console.error('FATAL: no block carries a parseable computedAt. Cannot auto-detect a cutoff.');
      console.error('       Re-run with an explicit --cutoff=<ISO timestamp>.');
      process.exit(1);
    }
    const newest = Math.max(...stamps);
    cutoffMs = newest - CUTOFF_SLACK_MIN * 60_000;
    console.log(`\nNewest block:      ${new Date(newest).toISOString()}`);
    console.log(`Cutoff (auto):     ${new Date(cutoffMs).toISOString()}  (newest - ${CUTOFF_SLACK_MIN}min slack)`);
  }

  // --- classify -------------------------------------------------------------
  const keep    = [];
  const orphans = [];
  const errored = [];

  for (const r of rows) {
    if (r.ts === undefined) { errored.push(r); continue; }   // read error -> never delete
    if (r.ts === null)      { orphans.push(r); continue; }   // no computedAt -> pre-dates current logic
    if (r.ts < cutoffMs)    { orphans.push(r); continue; }   // older than the last run
    keep.push(r);
  }

  console.log('');
  console.log(divider);
  console.log(`  Fresh (keep):     ${keep.length}`);
  console.log(`  ORPHANED (stale): ${orphans.length}`);
  console.log(`  Read errors (kept, not deleted): ${errored.length}`);
  console.log(divider);

  if (orphans.length === 0) {
    console.log('\nNo orphans. Redis is clean — every peers:* block came from the latest run.');
    return;
  }

  // Sanity brake. If we are about to delete a huge share of the keyspace, the
  // cutoff is almost certainly wrong (e.g. the batch never ran, or the clock is
  // off) and this would take the feature offline. Refuse rather than proceed.
  const pct = orphans.length / rows.length;
  if (pct > 0.5) {
    console.error('');
    console.error(`REFUSING: ${(pct * 100).toFixed(0)}% of all peers:* keys classify as orphans.`);
    console.error('That almost certainly means the cutoff is wrong, not that the keyspace is stale.');
    console.error('Deleting these would take the sympathy-earnings feature offline until the next run.');
    console.error('Inspect the timestamps above and re-run with an explicit --cutoff if this is intended.');
    process.exit(1);
  }

  console.log('\nORPHANS TO DELETE:\n');
  for (const r of orphans) console.log('  ' + describe(r.key, r.block, r.ts));

  if (!COMMIT) {
    console.log('');
    console.log(divider);
    console.log('DRY RUN — nothing was deleted.');
    console.log(`Re-run with --commit to delete these ${orphans.length} keys.`);
    console.log(divider);
    return;
  }

  // --- delete ---------------------------------------------------------------
  console.log('');
  console.log(`Deleting ${orphans.length} orphaned keys...`);
  let deleted = 0;
  let failed  = 0;

  for (let i = 0; i < orphans.length; i += DEL_BATCH) {
    const batch = orphans.slice(i, i + DEL_BATCH).map(r => r.key);
    try {
      const n = await redis.del(...batch);
      deleted += (typeof n === 'number' ? n : batch.length);
    } catch (err) {
      failed += batch.length;
      console.error(`  ⚠ DEL failed for ${batch.length} keys (non-fatal): ${err.message}`);
    }
    process.stdout.write(`\r  deleted ${deleted}/${orphans.length}`);
    await sleep(DEL_PAUSE_MS);
  }

  console.log('');
  console.log('');
  console.log(divider);
  console.log('FLUSH COMPLETE');
  console.log(`  Deleted: ${deleted}`);
  console.log(`  Failed:  ${failed}`);
  console.log(`  Kept:    ${keep.length}  (fresh blocks from the latest run)`);
  console.log(divider);
}

main().catch(err => {
  console.error('FATAL: Unhandled error:', err);
  process.exit(1);
});
