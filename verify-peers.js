// verify-peers.js — Check if sympathy peer data is in Redis (Phase 4 output)
// Mirrors verify-redis.js exactly: same dotenv/Redis init, same loop style.
// Run after the weekly Action completes: `node verify-peers.js`
require('dotenv').config();
const { Redis } = require('@upstash/redis');

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

async function checkPeers() {
  console.log('=== Checking Sympathy Peer Data (peers:{SYMBOL}) ===\n');

  // Dense industry (should get full blocks) + a few majors. Adjust as needed.
  const testSymbols = ['NVDA', 'AMD', 'MRVL', 'INTC', 'AAPL', 'MSFT'];

  let written = 0, case1 = 0, missing = 0;

  for (const symbol of testSymbols) {
    try {
      const data = await redis.get(`peers:${symbol}`);

      if (!data) {
        // No key = case 2 / cold / "has peers but none reporting soon" — all silent by design.
        console.log(`✗ ${symbol}: No peers key (case 2 / no imminent peer — silent, correct)\n`);
        missing++;
        continue;
      }

      const peerCount = Array.isArray(data.peers) ? data.peers.length : 'MALFORMED';

      if (Array.isArray(data.peers) && data.peers.length === 0) {
        // Case 1: in-universe but structurally peerless — the "no comparable peers" note.
        console.log(`○ ${symbol}: CASE 1 (qualified, peerless)`);
        console.log(`  qualified: ${data.qualified}`);
        console.log(`  matchLevel: ${data.matchLevel}`);
        console.log(`  computedAt: ${data.computedAt}\n`);
        case1++;
      } else {
        console.log(`✓ ${symbol}:`);
        console.log(`  qualified:  ${data.qualified}`);
        console.log(`  matchLevel: ${data.matchLevel}`);
        console.log(`  peers (${peerCount}):`);
        for (const p of data.peers) {
          console.log(`    - ${String(p.symbol).padEnd(6)} next: ${p.nextEarningsDate}  cap: ${p.marketCap}`);
        }
        console.log(`  computedAt: ${data.computedAt}\n`);
        written++;
      }

      // Confirm the 7-day TTL is set (not persistent, not the 45-day earnings TTL).
      const ttl = await redis.ttl(`peers:${symbol}`);
      console.log(`  TTL: ${ttl}s  (~${(ttl / 86400).toFixed(1)}d; expect ~7d / 604800s)\n`);

    } catch (error) {
      console.error(`✗ ${symbol}: Error - ${error.message}\n`);
    }
  }

  console.log('=== Summary ===');
  console.log(`  Full peer blocks: ${written}`);
  console.log(`  Case-1 (peerless): ${case1}`);
  console.log(`  No key (silent):   ${missing}`);
  console.log('=== Peer Check Complete ===');
}

checkPeers().catch(console.error);